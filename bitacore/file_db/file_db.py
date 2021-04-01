import xarray
import pandas as pd

from typing import Dict, List, Any, Union, Callable
from loguru import logger

from store_core.data_handler.files_store import FilesStore
from bitacore.relational_handler.database_bitacore_handler import ProviderDatabase
from bitacore.relational_handler.transform_normalized_data import transform_normalized_data

default_min_date = '1990-01-01'


class FileDB(FilesStore):
    """
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.provider_database = ProviderDatabase()

    def get_time_series_provider_data(self,
                                      get_data_provider_method: str,
                                      **kwargs):
        provider_data = getattr(self.provider_database, get_data_provider_method)(**kwargs)
        return transform_normalized_data(provider_data)

    def get_prices_split_return(self, **kwargs):
        prices_unadjusted = self.get_dataset(path='Prices Unadjusted', **kwargs)
        split_adjustment_factor = self.get_dataset(path='Split Adjustment Factor', **kwargs)
        return prices_unadjusted / split_adjustment_factor.sel(
            dates=prices_unadjusted.coords['dates'][::-1]
        ).cumprod().sel(
            dates=prices_unadjusted.coords['dates']
        )

    def reindex_dataset(self,
                        data: Union[xarray.Dataset, xarray.DataArray],
                        dataset_reindex_path: str,
                        coords_to_reindex: List[str],
                        **kwargs):
        if data is None:
            return None
        dataset = self.get_dataset(path=dataset_reindex_path, **kwargs)
        return data.reindex({coord: dataset.coords[coord] for coord in coords_to_reindex})

    def replace_values_dataset(self,
                               data: Union[xarray.Dataset, xarray.DataArray],
                               dataset_replace_values_path: str,
                               value_for_replace: Any,
                               **kwargs):
        if data is None:
            return None
        dataset = self.get_dataset(path=dataset_replace_values_path, **kwargs)
        return data.where(dataset.sel(data.coords), value_for_replace)

    def fill_nan_values(self,
                        data: Union[xarray.Dataset, xarray.DataArray],
                        value_for_fill_nan: Any,
                        **kwargs):
        return data.fillna(value_for_fill_nan)

    def forward_fill_dataset(self,
                             path: str,
                             data: Union[xarray.Dataset, xarray.DataArray],
                             dim_forward_fill: str = "dates",
                             **kwargs):
        if data is None:
            return None
        data_concat = data
        if self.exist_dataset(path):
            dataset = self.get_dataset(path, **kwargs)
            dataset = dataset.sel({
                dim_forward_fill: dataset.coords[dim_forward_fill] < data.coords[dim_forward_fill][0]
            })
            if dataset.sizes[dim_forward_fill] > 0:
                data_concat = xarray.concat([dataset.isel({dim_forward_fill: [-1]}), data], dim=dim_forward_fill)

        return data_concat.ffill(dim=dim_forward_fill).sel(data.coords)

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

        self.get_handler(
            path=path,
            **kwargs
        ).store_data(
            new_data=data,
            **kwargs
        )

    def append_generic_table_time_series(self,
                                         path: Union[str, List],
                                         data_methods_to_apply: List[str],
                                         **kwargs):

        start_date = self.get_dataset(path, *args, **kwargs).coords['dates'][0].values
        data = self.apply_data_methods(
            data_methods_to_apply=data_methods_to_apply,
            start_date=start_date,
            **kwargs
        )
        if data is None:
            return

        self.get_handler(
            path=path,
            **kwargs
        ).append_data(
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

        self.get_handler(
            path=path,
            **kwargs
        ).update_data(
            new_data=data,
            **kwargs
        )

    def append_prices_split_return(self,
                                   path: Union[str, List],
                                   **kwargs):

        prices_split_return = self.get_prices_split_return(**kwargs)
        split_adjustment_factor = self.get_dataset(path='Split Adjustment Factor', **kwargs)
        start_date = prices_split_return.coords['dates'][0].values

        handler = self.get_handler(
            path=path,
            **kwargs
        )

        securities_to_update = split_adjustment_factor.sel(index=split_adjustment_factor.coords['dates'] > start_date)
        securities_to_update = securities_to_update.coords['security_id'][
            (securities_to_update != 1).any(axis=0).to_array().values[0]
        ]
        if len(securities_to_update) > 0:
            handler.update_data(
                new_data=prices_split_return.sel(
                    dates=prices_split_return.coords['dates'] <= start_date,
                    security_id=securities_to_update
                ),
                **kwargs
            )

        handler.append_data(
            new_data=prices_split_return.sel(
                dates=prices_split_return.coords['dates'] > start_date,
            ),
            **kwargs
        )
