import xarray
import os

from typing import Dict, List, Any, Union, Callable, Generic
from loguru import logger

from store_core.base_handler.base_store import BaseStore


class FilesStore:
    def __init__(self,
                 base_path: str,
                 files_settings: Dict[str, Dict[str, Any]],
                 data_handler: Callable[[BaseStore], BaseStore],
                 use_env: bool = True,
                 *args,
                 **kwargs):

        self.base_path = base_path
        self.files_settings = files_settings
        self.open_base_store: Dict[str, BaseStore] = {}
        self.data_handler = data_handler
        if use_env:
            self.base_path = os.path.join(self.base_path, os.getenv("ENV_MODE"))
            self.files_settings = files_settings[os.getenv("ENV_MODE")]

        self.__dict__.update(**kwargs)

    def get_handler(self, name, path, **kwargs):
        if name not in self.open_base_store:
            kwargs['path'] = self.complete_path(path, name)
            self.open_base_store[name] = self.data_handler(**self.files_settings[name], **kwargs)
        return self.open_base_store[name]

    def get_dataset(self, name: str, path: Union[str, List] = None, *args, **kwargs) -> xarray.Dataset:
        if 'get_dataset' in self.files_settings[name]:
            return getattr(self, self.files_settings[name]['get_dataset'])(*args, **kwargs)

        return self.get_handler(name, path=path, **kwargs).get_dataset()

    def append_data(self,
                    new_data: xarray.DataArray,
                    name: str,
                    path: Union[str, List] = None,
                    *args,
                    **kwargs):

        if 'append_data' in self.files_settings[name]:
            return getattr(self, self.files_settings[name]['append_data'])(new_data, *args, **kwargs)

        self.get_handler(name, path=path, **kwargs).append_data(new_data, *args, **kwargs)

    def update_data(self,
                    new_data: xarray.DataArray,
                    name: str,
                    path: Union[str, List] = None,
                    *args,
                    **kwargs):
        if 'update_data' in self.files_settings[name]:
            return getattr(self, self.files_settings[name]['update_data'])(new_data, *args, **kwargs)

        self.get_handler(name, path=path, **kwargs).update_data(new_data, *args, **kwargs)

    def store_data(self,
                   new_data: xarray.DataArray,
                   name: str,
                   path: Union[str, List] = None,
                   *args,
                   **kwargs):

        if 'store_data' in self.files_settings[name]:
            return getattr(self, self.files_settings[name]['store_data'])(new_data, *args, **kwargs)

        self.get_handler(name, path=path, first_write=True, **kwargs).store_data(new_data, *args, **kwargs)

    def complete_path(self, path: Union[str, List], name: str):
        if path is None:
            return os.path.join(self.base_path, self.files_settings[name].get('extra_path', ''), name)

        if isinstance(path, list):
            return os.path.join(self.base_path, self.files_settings[name].get('extra_path', ''), *path)

        return os.path.join(self.base_path, self.files_settings[name].get('extra_path', ''), path)

    def close_store(self, name):
        if name in self.open_base_store:
            self.open_base_store[name].close()
            del self.open_base_store[name]

    def close(self):
        for name in list(self.open_base_store.keys()):
            self.close_store(name)














