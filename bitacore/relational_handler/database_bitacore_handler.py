import sys
import psycopg2
import os
import yaml
import time
import pandas as pd
from typing import Dict, List, Any, Union, Set
from loguru import logger
from BC2.config_root_dir import ROOT_DIR


class ProviderDatabase(object):
    _db_connection = None
    max_tries = 5

    def __init__(self, **kwargs):
        file = ROOT_DIR + '/config/config_db.yml'
        with open(file, "r") as yml_config:
            self.config = yaml.safe_load(yml_config)[os.getenv("ENV_MODE")]
        # Connection parameters of the database
        self.connection_auth = self.config.copy()
        self.connect_db()

    def connect_db(self):
        try:
            # Connecting to the database
            if ProviderDatabase._db_connection is not None:
                ProviderDatabase._db_connection.close()
                time.sleep(3)
            ProviderDatabase._db_connection = psycopg2.connect(**self.connection_auth)
            logger.info("Connected to DB")
        except psycopg2.Error as e:
            sys.exit(e)

    def execute_query(self, query, name='') -> pd.DataFrame:
        num_try = 0
        data = None
        start_time = time.time()
        while num_try < ProviderDatabase.max_tries:
            num_try += 1
            try:
                data = pd.read_sql_query(query, con=ProviderDatabase._db_connection)
            except pd.io.sql.DatabaseError as e:
                logger.error(e)
                self.connect_db()
                continue
            break

        if data is None:
            raise ConnectionError(f"There was a problem in the downloading of the query to get the {name}")

        logger.info(f"The query for {name} took {time.time() - start_time}s")

        return data

    def get_file_settings(self, name: str):
        query = f"""
            SELECT 
                a.file_settings as file_settings
            FROM m_data_field AS a
            WHERE name = '{name}'
        """
        data = self.execute_query(query, 'get_file_settings')
        return data.iloc[0, 0]

    def get_generic_time_series_data(self,
                                     start_date: Union[str, pd.Timestamp],
                                     table_name: str,
                                     value_name: str,
                                     time_name: str,
                                     end_date: Union[str, pd.Timestamp] = None,
                                     **kwargs) -> pd.DataFrame:
        start_date = start_date if isinstance(start_date, str) else start_date.strftime('%Y-%m-%d')
        end_date_filter = ''
        if end_date is not None:
            end_date = end_date if isinstance(end_date, str) else end_date.strftime('%Y-%m-%d')
            end_date_filter = f"AND {time_name} > '{end_date}'"
        query = f"""
            SELECT 
                a.{time_name} AS dates,
                a.{value_name} AS value,
                a.security_id AS security_id
            FROM {table_name} AS a
            WHERE {time_name} > '{start_date}'
                {end_date_filter}
        """
        data = self.execute_query(query, 'generic_time_series')
        return data

    def get_generic_from_to_data(self,
                                 table_name: str,
                                 value_name: str,
                                 start_date: Union[str, pd.Timestamp],
                                 from_to_name: str = 'from_date',
                                 to_date_name: str = 'to_date',
                                 end_date: Union[str, pd.Timestamp] = None,
                                 **kwargs) -> pd.DataFrame:
        start_date = start_date if isinstance(start_date, str) else start_date.strftime('%Y-%m-%d')
        end_date_filter = ''
        if end_date is not None:
            end_date = end_date if isinstance(end_date, str) else end_date.strftime('%Y-%m-%d')
            end_date_filter = f"AND {to_date_name} > '{end_date}'"
        query = f"""
            SELECT 
                a.{from_to_name} AS from_date,
                a.{to_date_name} as to_date,
                a.{value_name} AS value,
                a.security_id AS security_id
            FROM {table_name} AS a
            WHERE {from_to_name} > '{start_date}'
                {end_date_filter}
        """
        data = self.execute_query(query, 'generic_time_series')
        return data

    def get_esg_data(self,
                     data_field: int,
                     table: str,
                     start_date: Union[str, pd.Timestamp],
                     value_field_name: str = "value",
                     data_level: str = "company",
                     end_date: Union[str, pd.Timestamp] = None,
                     **kwargs) -> pd.DataFrame:
        """
            Gets the ESG ratings data of all the components of
            a list of securities for a list of dates.
        """

        # Converting lists with data into strings for the query
        start_date = start_date if isinstance(start_date, str) else start_date.strftime('%Y-%m-%d')

        end_date_filter = ''
        if end_date is not None:
            end_date = end_date if isinstance(end_date, str) else end_date.strftime('%Y-%m-%d')
            end_date_filter = f"AND from_date > '{end_date}'"

        data_level_query = "esg.m_company_id = m_security.company_id"
        if data_level == 'instrument':
            data_level_query = "esg.m_security_id = m_security.id"

        query = f"""
            SELECT
                esg.from_date AS from_date,
                esg.to_date AS to_date,
                m_security.id AS security_id,
                esg.{value_field_name} AS value
            FROM {table} AS esg
            JOIN m_security
                ON {data_level_query}
            WHERE esg.m_data_field_id = {data_field}
                AND from_date > '{start_date}'
                {end_date_filter}
        """

        data = self.execute_query(query, 'get_esg_data')
        return data

    def get_country_data(self, **kwargs):
        pass

    def get_sector_data(self, **kwargs):
        pass

    def get_ipo_date(self, **kwargs):
        pass

    def get_security_identifiers(self):
        pass

