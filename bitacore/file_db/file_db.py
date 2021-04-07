import xarray
import pandas as pd
import os

from typing import Dict, List, Any, Union, Callable
from numpy import nan
from loguru import logger

from store_core.data_handler.files_store import FilesStore
from bitacore.relational_handler.database_bitacore_handler import ProviderDatabase
from bitacore.relational_handler.transform_normalized_data import transform_normalized_data

default_min_date = '1990-01-01'


class FileDB(FilesStore):
    """
        TODO:
            Add methods to validate the data, for example should be useful to check the proportion of missing data
            before save the data
    """

    def __init__(self,
                 files_settings: Dict[str, Dict[str, Any]] = None,
                 **kwargs):
        super().__init__(
            files_settings=files_settings,
            **kwargs
        )
        self.provider_database = ProviderDatabase()

    # def get_file_setting(self, path) -> Dict:
    #     return self.provider_database.get_file_settings(os.path.basename(os.path.normpath(path)))
    #
    # def add_file_setting(self, file_settings_id, file_settings, user_id: str = ''):
    #     pass

    def get_time_series_provider_data(self,
                                      get_data_provider_method: str,
                                      **kwargs):
        provider_data = getattr(self.provider_database, get_data_provider_method)(**kwargs)
        return transform_normalized_data(provider_data)

    def get_prices_split_return(self, **kwargs):
        prices_unadjusted = self.read(path='Prices Unadjusted', **kwargs)
        split_adjustment_factor = self.read(path='Split Adjustment Factor', **kwargs)
        return prices_unadjusted / split_adjustment_factor.sel(
            dates=prices_unadjusted.coords['dates'][::-1]
        ).cumprod().sel(
            dates=prices_unadjusted.coords['dates']
        )

    def reindex_array(self,
                      data: Union[xarray.Dataset, xarray.DataArray],
                      data_array_reindex_path: str,
                      coords_to_reindex: List[str],
                      **kwargs):
        if data is None:
            return None
        data_array = self.read(path=data_array_reindex_path, **kwargs)
        return data.reindex({coord: data_array.coords[coord] for coord in coords_to_reindex})

    def replace_values_array(self,
                             data: Union[xarray.Dataset, xarray.DataArray],
                             data_array_replace_values_path: str,
                             value_for_replace: Any,
                             **kwargs):
        if data is None:
            return None
        data_array = self.read(path=data_array_replace_values_path, **kwargs)
        return data.where(data_array.sel(data.coords), value_for_replace)

    def fill_nan_values_array(self,
                              data: Union[xarray.Dataset, xarray.DataArray],
                              value_to_replace_nan: Any,
                              **kwargs):
        if data is None:
            return None

        return data.fillna(value_to_replace_nan)

    def forward_fill_array(self,
                           path: str,
                           data: Union[xarray.Dataset, xarray.DataArray],
                           dim_forward_fill: str,
                           **kwargs):
        if data is None:
            return None
        data_concat = data
        if self.exist(path):
            data_array = self.read(path, **kwargs)
            data_array = data_array.sel({
                dim_forward_fill: data_array.coords[dim_forward_fill] < data.coords[dim_forward_fill][0]
            })
            if data_array.sizes[dim_forward_fill] > 0:
                data_concat = xarray.concat([data_array.isel({dim_forward_fill: [-1]}), data], dim=dim_forward_fill)

        return data_concat.ffill(dim=dim_forward_fill).sel(data.coords)

    def replace_until_last_valid(self,
                                 data: Union[xarray.Dataset, xarray.DataArray],
                                 data_array_replace_values_path: str,
                                 value_for_replace: Any,
                                 dim_last_valid: str,
                                 **kwargs):
        if data is None:
            return None

        data_array_valid = self.read(data_array_replace_values_path, **kwargs)
        data_array_valid = data_array_valid.fillna(data.coords[dim_last_valid][-1])
        data_array_valid = xarray.DataArray(
            data=data.coords[dim_last_valid].values[:, None] <= data_array_valid.values,
            coords={
                dim_last_valid: data.coords[dim_last_valid],
                **{dim: coord for dim, coord in data_array_valid.coords.items() if dim != dim_last_valid}
            },
            dims=data.dims
        )
        return data.where(data_array_valid.sel(data.coords), value_for_replace)

    def apply_data_methods(self, data_methods_to_apply, **kwargs):
        data = None
        for method in data_methods_to_apply:
            data = getattr(self, method)(
                data=data,
                **kwargs
            )
        return data

    def store_generic_table_time_series(self,
                                        path: Union[str, List],
                                        data_methods_to_apply: List[str],
                                        start_date: pd.Timestamp = None,
                                        **kwargs):

        start_date = default_min_date if start_date is None else start_date
        data = self.apply_data_methods(
            data_methods_to_apply=data_methods_to_apply,
            start_date=start_date,
            **kwargs
        )
        if data is None:
            return

        self._get_handler(
            path=path,
            **kwargs
        ).store(
            new_data=data,
            **kwargs
        )

    def append_generic_table_time_series(self,
                                         path: Union[str, List],
                                         data_methods_to_apply: List[str],
                                         **kwargs):

        start_date = self.read(path, **kwargs).coords['dates'][0].values
        data = self.apply_data_methods(
            data_methods_to_apply=data_methods_to_apply,
            start_date=start_date,
            **kwargs
        )
        if data is None:
            return

        self._get_handler(
            path=path,
            **kwargs
        ).append(
            new_data=data,
            **kwargs
        )

    def update_generic_table_time_series(self,
                                         path: Union[str, List],
                                         data_methods_to_apply: List[str],
                                         start_date: pd.Timestamp,
                                         end_date: pd.Timestamp,
                                         **kwargs):

        data = self.apply_data_methods(
            data_methods_to_apply=data_methods_to_apply,
            start_date=start_date,
            end_date=end_date,
            **kwargs
        )
        if data is None:
            return

        self._get_handler(
            path=path,
            **kwargs
        ).update(
            new_data=data,
            **kwargs
        )

    def append_prices_split_return(self,
                                   path: Union[str, List],
                                   **kwargs):

        prices_split_return = self.get_prices_split_return(**kwargs)
        split_adjustment_factor = self.read(path='Split Adjustment Factor', **kwargs)
        start_date = prices_split_return.coords['dates'][0].values

        handler = self._get_handler(
            path=path,
            **kwargs
        )

        securities_to_update = split_adjustment_factor.sel(index=split_adjustment_factor.coords['dates'] > start_date)
        securities_to_update = securities_to_update.coords['security_id'][
            (securities_to_update != 1).any(axis=0).values
        ]
        if len(securities_to_update) > 0:
            handler.update(
                new_data=prices_split_return.sel(
                    dates=prices_split_return.coords['dates'] <= start_date,
                    security_id=securities_to_update
                ),
                **kwargs
            )

        handler.append(
            new_data=prices_split_return.sel(
                dates=prices_split_return.coords['dates'] > start_date,
            ),
            **kwargs
        )
