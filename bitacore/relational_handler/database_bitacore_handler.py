import sys
import psycopg2
import os
import yaml
import time
import pandas as pd
import json

from typing import Dict, List, Any, Union, Set
from loguru import logger
from config_path.config_root_dir import ROOT_DIR


class ProviderDatabase:
    _db_connection = None
    max_tries = 1

    def __init__(self, **kwargs):
        file = ROOT_DIR + '/config/config_db.yml'
        with open(file, "r") as yml_config:
            self.config = yaml.safe_load(yml_config)[os.getenv("ENV_MODE", 'test')]
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
        except psycopg2.Error as e:
            sys.exit(e)

    def get_data_query(self, query, name='') -> pd.DataFrame:
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

        # logger.info(f"The query for {name} took {time.time() - start_time}s")

        return data

    def execute_query(self, query: str):
        num_try = 0
        error = False
        while num_try < ProviderDatabase.max_tries:
            num_try += 1
            try:
                cursor = ProviderDatabase._db_connection.cursor()
                cursor.execute(query)
                ProviderDatabase._db_connection.commit()
                cursor.close()
                error = False
            except:
                logger.error('error')
                self.connect_db()
                error = True
                continue
            break

        if error:
            raise psycopg2.DatabaseError('Error executing the query')

    def upsert_file_settings(self, data_field_name: str, file_settings: Dict, author_id: str = '', **kwargs):
        file_settings_id = data_field_name
        if author_id != '':
            file_settings_id += author_id
        create_date = str(pd.Timestamp.now())
        file_settings = json.dumps(file_settings)
        query = f"""
            INSERT INTO test_files_settings (
                name, 
                author_id, 
                create_date, 
                file_settings_id, 
                file_settings
            )
            VALUES (
                '{data_field_name}', 
                '{author_id}', 
                '{create_date}', 
                '{file_settings_id}', 
                '{file_settings}'
            )
            ON CONFLICT (name, author_id)
            DO 
                UPDATE SET 
                    name = '{data_field_name}',
                    author_id = '{author_id}',
                    create_date = '{create_date}',
                    file_settings_id = '{file_settings_id}', 
                    file_settings = '{file_settings}'
        """
        self.execute_query(query)

    def get_file_settings(self, file_settings_id: str):
        query = f"""
            SELECT 
                a.file_settings as file_settings
            FROM test_files_settings AS a
            WHERE a.file_settings_id = '{file_settings_id}'
        """
        data = self.get_data_query(query, 'get_file_settings')
        return data.iloc[0, 0]

    def get_generic_time_series_data(self,
                                     start_date: Union[str, pd.Timestamp],
                                     table_name: str,
                                     value_name: str,
                                     time_name: str,
                                     end_date: Union[str, pd.Timestamp] = None,
                                     **kwargs) -> pd.DataFrame:
        start_date = pd.Timestamp(start_date).strftime('%Y-%m-%d')
        end_date_filter = ''
        if end_date is not None:
            end_date = pd.Timestamp(end_date).strftime('%Y-%m-%d')
            end_date_filter = f"AND {time_name} <= '{end_date}'"
        query = f"""
            SELECT 
                a.{time_name} AS date,
                a.{value_name} AS value,
                a.security_id AS security_id
            FROM {table_name} AS a
            WHERE {time_name} > '{start_date}'
                {end_date_filter}
        """
        data = self.get_data_query(query, 'generic_time_series')
        return data

    def get_generic_from_to_data(self,
                                 table_name: str,
                                 value_name: str,
                                 start_date: Union[str, pd.Timestamp],
                                 from_to_name: str = 'from_date',
                                 to_date_name: str = 'to_date',
                                 end_date: Union[str, pd.Timestamp] = None,
                                 **kwargs) -> pd.DataFrame:
        start_date = pd.Timestamp(start_date).strftime('%Y-%m-%d')
        end_date_filter = ''
        if end_date is not None:
            end_date = pd.Timestamp(end_date).strftime('%Y-%m-%d')
            end_date_filter = f"AND {to_date_name} <= '{end_date}'"
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
        data = self.get_data_query(query, 'generic_time_series')
        return data

    def get_esg_data(self,
                     data_field: int,
                     table_name: str,
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
        start_date = pd.Timestamp(start_date).strftime('%Y-%m-%d')

        end_date_filter = ''
        if end_date is not None:
            end_date = pd.Timestamp(end_date).strftime('%Y-%m-%d')
            end_date_filter = f"AND to_date <= '{end_date}'"

        data_level_query = "esg.m_company_id = m_security.company_id"
        if data_level == 'instrument':
            data_level_query = "esg.m_security_id = m_security.id"

        query = f"""
            SELECT
                esg.from_date AS from_date,
                esg.to_date AS to_date,
                m_security.id AS security_id,
                esg.{value_field_name} AS value
            FROM {table_name} AS esg
            JOIN m_security
                ON {data_level_query}
            WHERE esg.m_data_field_id = {data_field}
                AND from_date > '{start_date}'
                {end_date_filter}
        """

        data = self.get_data_query(query, 'get_esg_data')
        return data

    def get_prices_data(self,
                        start_date: Union[str, pd.Timestamp],
                        end_date: Union[str, pd.Timestamp] = None,
                        **kwargs) -> pd.DataFrame:
        start_date = pd.Timestamp(start_date).strftime('%Y-%m-%d')
        end_date_filter = ''
        if end_date is not None:
            end_date = pd.Timestamp(end_date).strftime('%Y-%m-%d')
            end_date_filter = f"AND price_date <= '{end_date}'"
        query = f"""
            SELECT 
                sp.price_date AS date,
                sp.close AS value,
                iq.security_id AS security_id
            FROM sp_ts_price AS sp
            JOIN i_quote as iq
	            ON iq.id = sp.quote_id
            WHERE price_date > '{start_date}'
                {end_date_filter}
        """
        data = self.get_data_query(query, 'get_prices_data')
        return data

    def get_country_data(self, **kwargs):
        pass

    def get_sector_data(self, **kwargs):
        pass

    def get_ipo_date(self, **kwargs):
        pass

    def get_security_identifiers(self):
        pass
