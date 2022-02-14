##########################################################

#

#  ETL via VPN connection

#

#  - establishes VPN connection

#  - loads data from remote MSSQL database and either

#      - saves to file (csv or json)

#      - forwards same file to s3 bucket

#      - saves data in local (MariaDB) database

#

##########################################################

 

import pandas as pd

import logging

from logging import Formatter

from logging.handlers import RotatingFileHandler

import subprocess

import boto3

from botocore.exceptions import ClientError

import os

import numpy as np

import sqlalchemy

from sqlalchemy import create_engine, MetaData, select, Table, Column, String

from sqlalchemy_utils import database_exists, create_database, drop_database

from sqlalchemy.exc import OperationalError

import argparse

import math

from tqdm import tqdm

import toml

 

 

def setup_logging():

    logFormatter = logging.Formatter('%(asctime)s %(message)s')

    rootLogger = logging.getLogger()

    rootLogger.setLevel(logging.DEBUG)

 

    fileHandler = RotatingFileHandler('load.log', maxBytes=10**6, backupCount=5)

    fileHandler.setFormatter(logFormatter)

    fileHandler.setLevel(logging.DEBUG)

    rootLogger.addHandler(fileHandler)

 

    consoleHandler = logging.StreamHandler()

    consoleHandler.setLevel(logging.INFO)

    rootLogger.addHandler(consoleHandler)

 

 

#argparse setup

c = False

table = None

parser = argparse.ArgumentParser()

parser.add_argument('-c', action='store_true', help='establish VPN connection')

parser.add_argument('-t', action='store_true', help='enter test mode')

parser.add_argument('--table', action='store', dest="table", help='load only one table specified as argument')

parser.add_argument('--startfrom', action='store', dest="starttable", help='define table to start from, e.g. after script interruption')

args = parser.parse_args()

vpn_needed = args.c

test_mode = args.t

 

# configuration

# (to be outsourced to configuration file

 

with open('config.toml', "r") as configfile:

    config = toml.load(configfile)

logging.debug(f"Using configuration: {config}")

 

source_db = "source_system" # this has to be defined during object instantiation

target_db = "MariaDB"

source_systemtablelist = config[source_db]["tablelist"]

source_system_oversized_tables = config[source_db]["oversized_tables"]

 

 

class ServerConnection:

 

    # bucket = config[target]['s3_bucket']

    # path = config[target]['filepath']

    engine = create_engine(f"{config[target_db]['dbstring']}://{config[target_db]['username']}:{config[target_db]['password']}@{config[target_db]['server']}/{config[target_db]['name']}")

    source_engine = create_engine(f"{config[source_db]['dbstring']}://{config[source_db]['username']}:{config[source_db]['password']}@{config[source_db]['server']}/{config[source_db]['name']}{config[source_db]['driver']}")

 

    def __init__(self, database, tables, vpn_needed):

        self.tables = tables

        self.database = database

        self.system = LinuxShell()

        if vpn_needed:

            self.system.vpn_connect()

        self.tables_with_column_restrictions = config[source_db]["tables_with_column_restrictions"]

 

 

    def create_database(self):

        logging.info("(Re-)creating database..")

        if not database_exists(ServerConnection.engine.url):

            create_database(ServerConnection.engine.url)

 

 

    def load_process(self, target="database", format="csv"):

        assert target in ["local", "s3", "database"], "Wrong target"

        assert format in ["csv", "json"], "Wrong format"

        self.db_cleanup()

        self.create_database()

        logging.info("Starting load process")

        for table in self.tables:

            chunks, chunk_count = self.retrieve_data(table)

            if target == "database":

                self.table_to_mariadb(chunks, chunk_count, table)

 

    def table_to_csv(self, table):

        chunks = self.retrieve_data(table) 

        logging.debug("Writing to file..")

        with open(f'{ServerConnection.path}{table}.csv', 'a+') as output:

            for n, df in enumerate(chunks):

                write_header = n == 0

                df.to_csv(output, header=write_header, index=False, encoding='utf-8', na_rep='NULL')

        logging.debug(f"Table {table} saved successfully")

 

    def __tables_to_json(self):

        for table in self.tables:

            logging.debug(f"Reading table {table}")

            chunks = pd.read_sql(f'SELECT * FROM {table}', self.cnxn, chunksize=1000)

            logging.debug("Writing to file..")

            json_file = f'{ServerConnection.path}{table}.json'

            with open(json_file, 'a+') as output:

                for n, df in enumerate(chunks):

                    print(f"Processing chunk number {n}", end='\r')

                    df.to_json(output, orient="records", lines=True)

            logging.debug(f"Table {table} saved successfully")   

 

    def transfer_to_s3(self):

        s3r = boto3.resource('s3')

        s3_client = boto3.client('s3')

        this_bucket = s3r.Bucket(ServerConnection.bucket)

        for file_name in os.listdir(ServerConnection.path):

            full_file_name = ServerConnection.path + file_name

            try:

                response = s3_client.upload_file(full_file_name, ServerConnection.bucket, f'source_system/{file_name}')

                os.remove(full_file_name)

                logging.debug(f"File {full_file_name} successfully uploaded")

            except ClientError as e:

                logging.debug(f"Following error occurred: {e}")

        return True

 

 

    def retrieve_data(self, table):

        logging.info(f"Reading table {table}")

        columns = '*'

        chunksize = 1000

        if table in self.tables_with_column_restrictions:

            columns = ",".join(self.tables_with_column_restrictions[table])

            logging.debug("Loading reduced number of columns")

        if table in source_system_oversized_tables:

            chunksize = 200

            logging.debug("Applying reduced chunksize")

        chunk_count_pd = pd.read_sql(f'SELECT COUNT(*) FROM {table}', con=ServerConnection.source_engine)

        chunk_count = math.ceil(chunk_count_pd.loc[0].values[0] / chunksize)

        chunks = pd.read_sql(f'SELECT {columns} FROM {table}', con=ServerConnection.source_engine, chunksize=chunksize)

        return chunks, chunk_count

 

 

    def create_empty_table(self, table, chunk):

        logging.info(f"Creating table {table}")

        chunk.head(0).to_sql(name=table, con=ServerConnection.engine, if_exists='replace', index=False)

 

 

    def delete_first_lines(self, table, lines):

        logging.debug(f"Deleting first 10 lines from {table}")

        filename = f"{ServerConnection.path}{table}.csv"

        df = pd.read_csv(filename, low_memory=True)

        df = df.iloc[10:]

        if df.empty():

            return True

        df.to_csv(filename)

        return False

 

    def tuples_from_rest_csv(self, table):

        chunksize = 10

        complete = False

        logging.debug("Processing new chunk")

        while not complete:

            with open(f'{ServerConnection.path}{table}.csv', 'r') as csv_in:

                chunk = [next(csv_in) for i in range(chunksize)]

                for line in chunk:

                    self.insert_into_table(line, table)

            complete = self.delete_first_lines(table, chunksize)

        """

            complete = (row_nbr > chunksize) or (not row)

                if complete:

                    break

                data = tuple(row)

                logging.debug(f"Inserting row {row_nbr} into table {table}")

                self.insert_into_table(data, table)

        self.delete_first_lines(table, chunksize)

        """

        logging.debug("Database Insertion complete")

 

    def table_to_mariadb(self, chunks, chunk_count, table):

        counter = 1

        complete = False

        chunk = next(chunks)

        self.create_empty_table(table, chunk)

        if chunk.empty:

            return

        binary_columns = self.find_binary_columns(chunk)

        pbar = tqdm(total=chunk_count)

        while not complete:

            pbar.update(1)

            try:

                if binary_columns:

                    chunk = self.hex_encode_binary_columns(chunk, binary_columns, counter)

                chunk = chunk.replace('nan', '')

                logging.debug(f"Writing chunk {counter} of {chunk_count} to table {table}.\r")

                self.chunk_to_mariadb(chunk, table)

                chunk = next(chunks)

                counter += 1

            except StopIteration:

                logging.info("Last chunk processed")

                complete = True

                pbar.close()

 

    def chunk_to_mariadb(self, chunk, table):

        chunk.to_sql(name=table, con=ServerConnection.engine, index=False, if_exists='append')

 

    def find_binary_columns(self, chunk):

        binary_columns = [column for column in chunk.columns if str(chunk[column].iloc[0])[0:2] in ["b\"", "b'"]]

        logging.debug(f"Detected binary columns {binary_columns}")

        return binary_columns

 

    def hex_encode_binary_columns(self, chunk, binary_columns, counter):

        for column in binary_columns:

            #chunk[column] = chunk[column].str.decode("latin-1")

            chunk[column] = chunk[column].apply(lambda x: x.hex())

        return chunk

 

    def insert_into_table(self, row, table_name):

        # not used

        # row-by-row insertion using sqlalchemy instead of pandas.

        # Expects parameter row as tuple.

        logging.debug(f"Inserting row into table {table_name}")

        with ServerConnection.engine.begin() as connection:

            meta_data = MetaData()

            meta_data.reflect(bind=connection)

            table = meta_data.tables[table_name]

            ins = table.insert().values(row)

            result = connection.execute(ins)

 

    def get_table_metadata(self, table):

        # not used

        logging.debug("Retrieving source table metadata")

        source_meta = MetaData(bind=ServerConnection.source_engine)

        source_meta.reflect()

        return source_meta.tables[table]

 

    def db_cleanup(self):

        logging.debug("Deleting database")

        if database_exists(ServerConnection.engine.url):

            local_meta = MetaData(bind=ServerConnection.engine)

            local_meta.reflect()

            local_meta.drop_all()

            drop_database(ServerConnection.engine.url)

        self.system.purge_database()

 

 

class LinuxShell:

 

    vpn_configuration_file = config["VPN_connection"]["openvpn_configuration_file"]

    vpn_credentials = config["VPN_connection"]["vpn_credentials"]

 

    def purge_database(self):

        logging.debug("Deleting Database logfiles")

        bashCmd = "sudo rm -rf /var/lib/mysql/ib*"

        self.shell_execute(bashCmd)

 

    def check_vpn_connection(self):

        # to be implemented

        bashCmd = '/dev/tun0'

        return

 

    def vpn_connect(self):

        logging.debug("Establishing VPN connection")

        bashCmd = f"sudo openvpn --daemon etl --config {LinuxShell.vpn_configuration_file} --auth-user-pass {LinuxShell.vpn_credentials}"

        self.shell_execute(bashCmd)

 

    def shell_execute(self, bash_cmd): 

        logging.debug("Executing")

        process = subprocess.run(bash_cmd, shell=True, capture_output=True)

        logging.debug(process)

 

 

if args.table:

    tables = [args.table]

elif args.starttable:

    tables = source_systemtablelist[source_systemtablelist.index(args.starttable):]

else:

    tables = source_systemtablelist

 

setup_logging()

source_system = ServerConnection('tarisprod', tables, vpn_needed)

source_system.load_process()

 

print("Data transfer complete")