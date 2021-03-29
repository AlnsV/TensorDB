import xarray
import os

from abc import abstractmethod
from typing import Dict, List, Any, Union, Callable, Generic


class BaseStore:
    def __init__(self,
                 path: str,
                 base_path: str = None,
                 *args,
                 **kwargs):
        self.path = path
        self.base_path = base_path
        self.__dict__.update(kwargs)

    @abstractmethod
    def append_data(self, new_data: Union[xarray.DataArray, xarray.Dataset], *args, **kwargs):
        pass

    @abstractmethod
    def update_data(self, new_data: Union[xarray.DataArray, xarray.Dataset], *args, **kwargs):
        pass

    @abstractmethod
    def store_data(self, new_data: Union[xarray.DataArray, xarray.Dataset], *args, **kwargs):
        pass

    @abstractmethod
    def upsert_data(self, new_data: Union[xarray.DataArray, xarray.Dataset], *args, **kwargs):
        pass

    @abstractmethod
    def update_from_backup(self, *args, **kwargs):
        pass

    @abstractmethod
    def backup(self, *args, **kwargs):
        pass

    @abstractmethod
    def get_dataset(self, *args, **kwargs) -> xarray.Dataset:
        pass

    @abstractmethod
    def close(self, *args, **kwargs):
        pass

    @abstractmethod
    def equal_to_backup(self, *args, **kwargs):
        pass

    @property
    def local_path(self):
        return os.path.join("" if self.base_path is None else self.base_path, self.path)

