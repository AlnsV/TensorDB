import xarray
import os
import pandas as pd

from typing import Dict, List, Any, Union, Callable
from loguru import logger

from store_core.base_handler.base_store import BaseStore
from store_core.s3_handler.s3_handler import S3Handler
from config_path.config_root_dir import ROOT_DIR
from store_core.netcdf_handler.metadata_handler import MetadataHandler


class FilesStore:
    """
        FilesStore
        ----------
        It's a kind of SGBD based on files (not necessary the same type of file). It provide a set of basic methods
        that include append, update, store and retrieve data, all these methods are combined with a backup using S3.

        It was designed with the idea of being an inheritable class, so if there is a file that need a special
        treatment, it will be possible to create a new method that handle that specific file

        Notes
        -----
        1) This class does not have any kind of concurrency but of course the internal handler could have

        2) The actual recommend option to handle the files is using the zarr handler class which allow to write and read
        concurrently
    """

    def __init__(self,
                 files_settings: Dict[str, Dict[str, Any]],
                 base_path: str = None,
                 use_env: bool = False,
                 s3_settings: Union[Dict[str, str], S3Handler] = None,
                 max_files_in_disk: int = 30,
                 *args,
                 **kwargs):
        """

        """
        self.base_path = os.path.join(ROOT_DIR, 'file_db') if base_path is None else base_path
        self.files_settings = files_settings
        self.open_base_store: Dict[str, Dict[str, Any]] = {}
        self.max_files_in_disk = max_files_in_disk

        if use_env:
            self.base_path = os.path.join(self.base_path, os.getenv("ENV_MODE"))
            self.files_settings = files_settings[os.getenv("ENV_MODE")]

        self.s3_handler = s3_settings
        if s3_settings is not None:
            if isinstance(s3_settings, dict):
                self.s3_handler = S3Handler(**s3_settings)

        self.__dict__.update(**kwargs)

    def get_handler(self,
                    file_setting_id: str,
                    path: Union[str, List],
                    **kwargs) -> BaseStore:

        local_path = self.complete_path(file_setting_id=file_setting_id, path=path)
        if local_path not in self.open_base_store:
            self.open_base_store[local_path] = {
                'data_handler': self.files_settings[file_setting_id]['data_handler'](
                    base_path=self.base_path,
                    path=self.complete_path(file_setting_id=file_setting_id, path=path, omit_base_path=True),
                    s3_handler=self.s3_handler,
                    **self.files_settings[file_setting_id],
                    **kwargs
                ),
                'first_read_date': pd.Timestamp.now(),
                'num_use': 0
            }
        self.open_base_store[local_path]['num_use'] += 1
        return self.open_base_store[local_path]['data_handler']

    def get_dataset(self,
                    file_setting_id: str,
                    path: Union[str, List],
                    *args,
                    **kwargs) -> xarray.Dataset:

        if 'get_dataset' in self.files_settings[file_setting_id]:
            return getattr(self, self.files_settings[file_setting_id]['get_dataset'])(*args, **kwargs)
        return self.get_handler(file_setting_id=file_setting_id, path=path, **kwargs).get_dataset()

    def append_data(self,
                    file_setting_id: str,
                    path: Union[str, List],
                    new_data: xarray.DataArray = None,
                    *args,
                    **kwargs):

        file_setting = self.files_settings[file_setting_id]
        if 'append_data' in file_setting:
            return getattr(self, file_setting['append_data'])(
                file_setting=file_setting,
                path=path,
                new_data=new_data,
                *args,
                **kwargs
            )

        return self.get_handler(
            file_setting_id,
            path=path,
            **kwargs
        ).append_data(
            new_data,
            *args,
            **kwargs
        )

    def update_data(self,
                    file_setting_id: str,
                    path: Union[str, List],
                    new_data: xarray.DataArray = None,
                    *args,
                    **kwargs):

        file_setting = self.files_settings[file_setting_id]
        if 'update_data' in file_setting:
            return getattr(self, file_setting['update_data'])(
                file_setting=file_setting,
                path=path,
                new_data=new_data,
                *args,
                **kwargs
            )

        return self.get_handler(
            file_setting_id,
            path=path,
            **kwargs
        ).update_data(
            new_data,
            *args,
            **kwargs
        )

    def store_data(self,
                   file_setting_id: str,
                   path: Union[str, List],
                   new_data: xarray.DataArray = None,
                   *args,
                   **kwargs):

        file_setting = self.files_settings[file_setting_id]
        if 'store_data' in file_setting:
            return getattr(self, file_setting['store_data'])(
                file_setting=file_setting,
                path=path,
                new_data=new_data,
                *args,
                **kwargs
            )

        return self.get_handler(
            file_setting_id=file_setting_id,
            path=path,
            **kwargs
        ).store_data(
            new_data=new_data,
            *args,
            **kwargs
        )

    def backup(self,
               file_setting_id: str,
               path: Union[str, List],
               *args,
               **kwargs):
        file_setting = self.files_settings[file_setting_id]
        if 'backup' in file_setting:
            return getattr(self, file_setting['backup'])(
                file_setting=file_setting,
                path=path,
                *args,
                **kwargs
            )

        return self.get_handler(
            file_setting_id=file_setting_id,
            path=path,
            **kwargs
        ).backup(
            *args,
            **kwargs
        )

    def update_from_backup(self,
                           file_setting_id: str,
                           path: Union[str, List],
                           *args,
                           **kwargs):
        file_setting = self.files_settings[file_setting_id]
        if 'update_from_backup' in file_setting:
            return getattr(self, file_setting['update_from_backup'])(
                file_setting=file_setting,
                path=path,
                *args,
                **kwargs
            )

        return self.get_handler(
            file_setting_id=file_setting_id,
            path=path,
            **kwargs
        ).update_from_backup(
            *args,
            **kwargs
        )

    def close(self,
              file_setting_id: str,
              path: Union[str, List],
              *args,
              **kwargs):

        file_setting = self.files_settings[file_setting_id]
        if 'close' in file_setting:
            return getattr(self, file_setting['close'])(
                file_setting=file_setting,
                path=path,
                *args,
                **kwargs
            )

        return self.get_handler(
            file_setting_id=file_setting_id,
            path=path,
            **kwargs
        ).close(
            *args,
            **kwargs
        )

    def complete_path(self,
                      file_setting_id: str,
                      path: Union[List[str], str],
                      omit_base_path: bool = False):

        path = path if isinstance(path, list) else [path]
        if not omit_base_path:
            return os.path.join(self.base_path, self.files_settings[file_setting_id].get('extra_path', ''), *path)

        return os.path.join(self.files_settings[file_setting_id].get('extra_path', ''), *path)
