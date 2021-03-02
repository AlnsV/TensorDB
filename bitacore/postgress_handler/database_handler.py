import sys
import psycopg2
import os
import yaml
import time
import pandas as pd

from typing import Dict, List, Any, Union, Set
from loguru import logger

from BC2.config_root_dir import ROOT_DIR


class DatabaseHandler(object):
    _db_connection = None
    max_tries = 5

    def __init__(self):
        file = ROOT_DIR + '/config/config_db.yml'
        with open(file, "r") as yml_config:
            self.connection_auth = yaml.safe_load(yml_config)[os.getenv("ENV_MODE")]

        logger.info(f"Running on with config:{self.connection_auth}")

        # Connection parameters of the database
        self.connect_db()

    def connect_db(self):
        # Connecting to the database
        if DatabaseHandler._db_connection is not None:
            DatabaseHandler._db_connection.close()
            time.sleep(5)
        DatabaseHandler._db_connection = psycopg2.connect(**self.connection_auth)
        logger.info("Connected to the DB")

    def execute_query(self, query, name='') -> Union[pd.Series, pd.DataFrame]:
        num_try = 0
        data = None
        start_time = time.time()
        while num_try < DatabaseHandler.max_tries:
            num_try += 1
            try:
                data = pd.read_sql_query(query, con=DatabaseHandler._db_connection)
            except pd.io.sql.DatabaseError as e:
                logger.error(e)
                self.connect_db()
                continue
            break

        if data is None:
            raise ConnectionError(f"There was a problem in the execution of the query for get {name}")

        logger.info(f"The query for {name} took {time.time() - start_time}s")

        return data

    def query_from_to_table(self, start_date: pd.Timestamp, end_date: pd.Timestamp, table_name: str):
        pass

    def query_time_series(self, start_date: pd.Timestamp, end_date: pd.Timestamp, table_name: str):
        pass

    def query_interest_rates(self, start_date: pd.Timestamp, end_date: pd.Timestamp, table_name: str):
        pass

    def query_country(self):
        pass

    def query_sector(self):
        pass

    def query_file_settings(self):
        pass



