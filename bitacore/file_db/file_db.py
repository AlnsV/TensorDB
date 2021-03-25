import xarray
import pandas as pd

from typing import Dict, List, Any, Union, Callable
from loguru import logger

from store_core.data_handler.files_store import FilesStore
from bitacore.relational_handler.database_bitacore_handler import ProviderDatabase
from bitacore.relational_handler.transform_normalized_data import transform_2d_time_series_normalized_to_dataset


default_min_date = '1990-01-01'


class FileDB(FilesStore):
    """
    TODO: Add a decorator or a method to simplify the download data from the provider
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.provider_database = ProviderDatabase()

    def get_provider_data(self,
                          get_data_provider_method: str,
                          *args,
                          **kwargs):
        provider_data = getattr(self.provider_database, get_data_provider_method)(
            *args,
            **kwargs
        )
        if provider_data.empty:
            return None
        return transform_2d_time_series_normalized_to_dataset(provider_data)

    def store_generic_table_time_series(self,
                                        path: Union[str, List],
                                        get_data_method: str,
                                        start_date: pd.Timestamp = None,
                                        *args,
                                        **kwargs):

        start_date = default_min_date if start_date is None else start_date

        provider_data = getattr(self, get_data_method)(
            path=path,
            start_date=start_date,
            *args,
            **kwargs
        )
        if provider_data is None:
            return

        self.get_handler(
            path=path,
            **kwargs
        ).store_data(
            new_data=provider_data,
            *args,
            **kwargs
        )

    def append_generic_table_time_series(self,
                                         path: Union[str, List],
                                         get_data_method: str,
                                         *args,
                                         **kwargs):

        start_date = self.get_dataset(path, *args, **kwargs).coords['dates'][0].values

        provider_data = getattr(self, get_data_method)(
            path=path,
            start_date=start_date,
            *args,
            **kwargs
        )
        if provider_data is None:
            return

        self.get_handler(
            path=path,
            **kwargs
        ).append_data(
            new_data=provider_data,
            *args,
            **kwargs
        )

    def update_generic_table_time_series(self,
                                         path: Union[str, List],
                                         get_data_method: str,
                                         start_date: pd.Timestamp,
                                         end_date: pd.Timestamp,
                                         *args,
                                         **kwargs):

        provider_data = getattr(self, get_data_method)(
            start_date=start_date,
            end_date=end_date,
            path=path,
            *args,
            **kwargs
        )
        if provider_data is None:
            return

        self.get_handler(
            path=path,
            **kwargs
        ).update_data(
            new_data=provider_data,
            *args,
            **kwargs
        )








