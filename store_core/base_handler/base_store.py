import xarray
from abc import abstractmethod


class BaseStore:
    def __init__(self, base_path: str, *args, **kwargs):
        self.base_path = base_path
        pass

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


