import xarray
import pandas as pd
import os

from typing import Dict, List, Any, Union, Callable
from loguru import logger
from numpy import datetime64

from store_core.data_handler.files_store import FilesStore
from bitacore.relational_handler.database_bitacore_handler import ProviderDatabase
from store_core.base_handler.base_store import BaseStore
from bitacore.relational_handler.transform_normalized_data import transform_normalized_data


class FileDB(FilesStore):

    def __init__(self,
                 files_settings: Dict[str, Dict[str, Any]] = None,
                 **kwargs):
        super().__init__(
            files_settings=files_settings,
            **kwargs
        )
        self.provider_database = ProviderDatabase()

    def get_file_settings(self, path) -> Dict:
        return self.provider_database.get_file_settings(os.path.basename(os.path.normpath(path)))

    def add_file_settings(self,
                          data_field_name: str,
                          file_settings: Dict,
                          author_id: str = '',
                          **kwargs):
        self.provider_database.upsert_file_settings(
            data_field_name=data_field_name,
            file_settings=file_settings,
            author_id=author_id,
            **kwargs
        )

    def append_prices_split_return(self, handler, **kwargs):

        prices_split_return = self.get_prices_split_return()
        split_adjustment_factor = self.read(path='Split Adjustment Factor')
        start_date = prices_split_return.coords['date'][0].values

        securities_to_update = split_adjustment_factor.sel(index=split_adjustment_factor.coords['date'] > start_date)
        securities_to_update = securities_to_update.coords['security_id'][
            (securities_to_update != 1).any(axis=0).values
        ]
        if len(securities_to_update) > 0:
            handler.update(
                new_data=prices_split_return.sel(
                    date=prices_split_return.coords['date'] <= start_date,
                    security_id=securities_to_update
                ),
                **kwargs
            )

        handler.append(
            new_data=prices_split_return.sel(
                date=prices_split_return.coords['date'] > start_date,
            ),
            **kwargs
        )

    def get_prices_split_return(self):
        prices_unadjusted = self.read(path='Prices Unadjusted')
        split_adjustment_factor = self.read(path='Split Adjustment Factor')
        return prices_unadjusted / split_adjustment_factor.sel(
            date=prices_unadjusted.coords['date'][::-1]
        ).cumprod(
            dim='date'
        ).sel(
            date=prices_unadjusted.coords['date']
        )

    def concat_start_date(self,
                          handler: BaseStore,
                          action_type: str,
                          default_start_date: str = '1990-01-01',
                          **kwargs) -> Dict[str, datetime64]:
        if action_type == 'store':
            return {'start_date': datetime64(default_start_date)}
        return {'start_date': datetime64(handler.read().coords['date'][-1].values)}

    def get_prices_data(self,
                        start_date: Union[str, pd.Timestamp],
                        end_date: Union[str, pd.Timestamp] = None,
                        **kwargs) -> xarray.DataArray:
        data = self.provider_database.get_prices_data(
            start_date=start_date,
            end_date=end_date,
            **kwargs
        )
        return transform_normalized_data(data)

    def get_esg_data(self,
                     data_field: int,
                     table_name: str,
                     start_date: Union[str, pd.Timestamp],
                     value_field_name: str = "value",
                     data_level: str = "company",
                     end_date: Union[str, pd.Timestamp] = None,
                     **kwargs) -> xarray.DataArray:
        data = self.provider_database.get_esg_data(
            start_date=start_date,
            end_date=end_date,
            data_field=data_field,
            table_name=table_name,
            value_field_name=value_field_name,
            data_level=data_level,
            **kwargs
        )
        return transform_normalized_data(data)

    def get_generic_from_to_data(self,
                                 table_name: str,
                                 value_name: str,
                                 start_date: Union[str, pd.Timestamp],
                                 from_to_name: str = 'from_date',
                                 to_date_name: str = 'to_date',
                                 end_date: Union[str, pd.Timestamp] = None,
                                 **kwargs) -> xarray.DataArray:
        data = self.provider_database.get_generic_from_to_data(
            start_date=start_date,
            end_date=end_date,
            value_name=value_name,
            table_name=table_name,
            from_to_name=from_to_name,
            to_date_name=to_date_name,
            **kwargs
        )
        return transform_normalized_data(data)

    def get_generic_time_series_data(self,
                                     start_date: Union[str, pd.Timestamp],
                                     table_name: str,
                                     value_name: str,
                                     time_name: str,
                                     end_date: Union[str, pd.Timestamp] = None,
                                     **kwargs) -> xarray.DataArray:
        data = self.provider_database.get_generic_time_series_data(
            start_date=start_date,
            end_date=end_date,
            value_name=value_name,
            table_name=table_name,
            time_name=time_name,
            **kwargs
        )
        return transform_normalized_data(data)
