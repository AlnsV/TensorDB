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

    def get_handler(self, name, **kwargs):
        if name not in self.open_base_store:
            if 'data_handler' in self.files_settings[name]:
                self.open_base_store[name] = self.files_settings[name]['data_handler'](
                    **self.files_settings[name], **kwargs
                )
            else:
                self.open_base_store[name] = self.data_handler(**self.files_settings[name], **kwargs)
        return self.open_base_store[name]

    def get_dataset(self, name: str, *args, **kwargs) -> xarray.Dataset:
        if 'get_dataset' in self.files_settings[name]:
            return getattr(self, self.files_settings[name]['get_dataset'])(*args, **kwargs)

        dataset = self.get_handler(name, **kwargs).get_dataset()
        return dataset

    def append_data(self, new_data: xarray.DataArray, name: str, *args, **kwargs):
        if 'append_data' in self.files_settings[name]:
            getattr(self, self.files_settings[name]['append_data'])(new_data, *args, **kwargs)
            return

        self.get_handler(name, **kwargs).append_data(new_data, *args, **kwargs)

    def update_data(self, new_data: xarray.DataArray, name: str, *args, **kwargs):
        if 'update_data' in self.files_settings[name]:
            getattr(self, self.files_settings[name]['update_data'])(new_data, *args, **kwargs)
            return

        self.get_handler(name, **kwargs).update_data(new_data, *args, **kwargs)

    def store_data(self, new_data, name, *args, **kwargs):
        if 'store_data' in self.files_settings[name]:
            getattr(self, self.files_settings[name]['store_data'])(new_data, *args, **kwargs)
            return

        kwargs['first_write'] = True
        self.get_handler(name, **kwargs).store_data(new_data, *args, **kwargs)

    def close_store(self, name):
        if name in self.open_base_store:
            self.open_base_store[name].close()
            del self.open_base_store[name]

    def close(self):
        for name in list(self.open_base_store.keys()):
            self.close_store(name)














