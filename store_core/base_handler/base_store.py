import xarray
from abc import abstractmethod
from typing import Dict, List, Any, Union, Callable, Generic
from store_core.netcdf_handler.dims_handler import DimsHandler
from store_core.base_handler.base_metadata_handler import BaseMetadataHandler


class BaseStore:
    def __init__(self,
                 base_path: str,
                 dims: List[str],
                 dims_type: Dict[str, str],
                 dims_space: Dict[str, Union[float, int]],
                 default_free_value: Any = None,
                 concat_dim: str = "index",
                 first_write: bool = False,
                 *args,
                 **kwargs):
        self.base_path = base_path
        self.metadata: BaseMetadataHandler = None
        self.dims_handler = DimsHandler(
            coords={},
            dims=dims,
            dims_space=dims_space,
            dims_type=dims_type,
            default_free_value=default_free_value,
            concat_dim=concat_dim,
            *args,
            **kwargs
        )

    @abstractmethod
    def append_data(self, new_data: xarray.DataArray, *args, **kwargs):
        pass

    @abstractmethod
    def update_data(self, new_data: xarray.DataArray, *args, **kwargs):
        pass

    @abstractmethod
    def store_data(self, new_data: xarray.DataArray, *args, **kwargs):
        pass

    @abstractmethod
    def get_dataset(self) -> xarray.Dataset:
        pass

    @abstractmethod
    def save(self):
        pass

    @abstractmethod
    def close(self):
        pass


