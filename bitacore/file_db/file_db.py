import xarray
import pandas as pd

from typing import Dict, List, Any, Union, Callable
from loguru import logger

from store_core.data_handler.files_store import FilesStore
from bitacore.postgress_handler.database_handler import DatabaseHandler


class FileDB(FilesStore):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.postgres_handler = DatabaseHandler()

    def store_generic_table_from_to(self,
                                    file_setting_id: str,
                                    path: Union[str, List],
                                    *args,
                                    **kwargs):
        pass

    def store_generic_table_time_series(self,
                                        file_setting_id: str,
                                        path: Union[str, List],
                                        *args,
                                        **kwargs):
        pass

    def append_generic_table_from_to(self,
                                     file_setting_id: str,
                                     path: Union[str, List],
                                     *args,
                                     **kwargs):
        pass

    def append_generic_table_time_series(self,
                                         file_setting_id: str,
                                         path: Union[str, List],
                                         *args,
                                         **kwargs):
        pass

    def update_generic_table_from_to(self,
                                     file_setting_id: str,
                                     path: Union[str, List],
                                     *args,
                                     **kwargs):
        pass

    def update_generic_table_time_series(self,
                                         file_setting_id: str,
                                         path: Union[str, List],
                                         *args,
                                         **kwargs):
        pass








