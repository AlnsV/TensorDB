import xarray
import os

from typing import Dict, List, Any, Union, Callable

from store_core.base_handler.base_store import BaseStore
from store_core.s3_handler.s3_handler import S3Handler
from config_path.config_root_dir import ROOT_DIR


class FilesStore:
    def __init__(self,
                 files_settings: Dict[str, Dict[str, Any]],
                 data_handler: Callable[[BaseStore], BaseStore],
                 base_path: str = None,
                 use_env: bool = True,
                 s3_settings: Union[Dict[str, str], S3Handler] = None,
                 *args,
                 **kwargs):

        self.base_path = base_path
        if self.base_path is None:
            self.base_path = os.path.join(ROOT_DIR, 'file_db')
        self.files_settings = files_settings
        self.open_base_store: Dict[str, Dict[str, Any]] = {}
        self.last_modified_date = {}
        self.data_handler = data_handler
        if use_env:
            self.base_path = os.path.join(self.base_path, os.getenv("ENV_MODE"))
            self.files_settings = files_settings[os.getenv("ENV_MODE")]

        self.s3_handler = s3_settings
        if s3_settings is not None:
            if isinstance(s3_settings, dict):
                self.s3_handler = S3Handler(**s3_settings)

        self.__dict__.update(**kwargs)

    def get_handler(self, file_setting_id: str, path: Union[str, List] = None, **kwargs):
        self.download_partitions(file_setting_id, path)
        local_path = self.complete_path(file_setting_id=file_setting_id, path=path)
        if local_path not in self.open_base_store:
            kwargs['path'] = local_path
            self.open_base_store[local_path] = {
                'file_setting_id': file_setting_id,
                'path': path,
                'handler': self.data_handler(**self.files_settings[file_setting_id], **kwargs)
            }

        return self.open_base_store[local_path]['handler']

    def get_dataset(self, file_setting_id: str, path: Union[str, List] = None, *args, **kwargs) -> xarray.Dataset:
        if 'get_dataset' in self.files_settings[file_setting_id]:
            return getattr(self, self.files_settings[file_setting_id]['get_dataset'])(*args, **kwargs)

        return self.get_handler(file_setting_id=file_setting_id, path=path, **kwargs).get_dataset()

    def append_data(self,
                    new_data: xarray.DataArray,
                    file_setting_id: str,
                    path: Union[str, List] = None,
                    *args,
                    **kwargs):

        if 'append_data' in self.files_settings[file_setting_id]:
            return getattr(self, self.files_settings[file_setting_id]['append_data'])(new_data, *args, **kwargs)

        self.get_handler(file_setting_id, path=path, **kwargs).append_data(new_data, *args, **kwargs)

    def update_data(self,
                    new_data: xarray.DataArray,
                    file_setting_id: str,
                    path: Union[str, List] = None,
                    *args,
                    **kwargs):

        if 'update_data' in self.files_settings[file_setting_id]:
            return getattr(self, self.files_settings[file_setting_id]['update_data'])(new_data, *args, **kwargs)

        self.get_handler(file_setting_id, path=path, **kwargs).update_data(new_data, *args, **kwargs)

    def store_data(self,
                   new_data: xarray.DataArray,
                   file_setting_id: str,
                   path: Union[str, List] = None,
                   *args,
                   **kwargs):

        if 'store_data' in self.files_settings[file_setting_id]:
            return getattr(self, self.files_settings[file_setting_id]['store_data'])(new_data, *args, **kwargs)

        self.get_handler(file_setting_id, path=path, first_write=True, **kwargs).store_data(new_data, *args, **kwargs)

    def download_partitions(self, file_setting_id: str, path: List[str] = None):
        if self.s3_handler is None:
            return None

        s3_path = self.complete_path(file_setting_id, path=path, omit_base_path=True)
        local_path = self.complete_path(file_setting_id, path=path)
        s3_metadata_path = os.path.join(s3_path, 'metadata.nc')
        local_metadata_path = os.path.join(local_path, 'metadata.nc')

        if self.is_updated(s3_metadata_path, local_metadata_path):
            self.s3_handler.download_file(s3_path=s3_metadata_path,
                                          local_path=local_metadata_path,
                                          **self.files_settings[file_setting_id])



    def is_updated(self, s3_path_file: str, local_path: str):
        if local_path not in self.open_base_store:
            return False

        local_last_modified_date = self.open_base_store[local_path]['last_modified_date']
        s3_last_modified_date = self.s3_handler.get_last_modified_date(
            self.open_base_store[local_path]['bucket_name'],
            s3_path_file
        )
        return local_last_modified_date == s3_last_modified_date

    def complete_path(self, file_setting_id: str, path: Union[List[str], str] = None, omit_base_path: bool = False):
        path = [file_setting_id] if path is None else path
        path = path if isinstance(path, list) else [path]
        if not omit_base_path:
            return os.path.join(self.base_path, self.files_settings[file_setting_id].get('extra_path', ''), *path)

        return os.path.join(self.files_settings[file_setting_id].get('extra_path', ''), *path)

    def close_store(self, file_setting_id: str, path: Union[List[str], str] = None, *args, **kwargs):
        if 'close_store' in self.files_settings[file_setting_id]:
            return getattr(self, self.files_settings[file_setting_id]['close_store'])(file_setting_id, *args, **kwargs)

        complete_path = self.complete_path(file_setting_id, path)
        if complete_path in self.open_base_store:
            self.open_base_store[complete_path]['handler'].close()
            del self.open_base_store[complete_path]

    def delete_store(self, file_setting_id: str, path: Union[List[str], str] = None, *args, **kwargs):
        self.close_store(file_setting_id, path)
        complete_path = self.complete_path(file_setting_id, path)
        # TODO Implement this
        pass

    def close(self):
        for key in list(self.open_base_store.keys()):
            self.close_store(**self.open_base_store[key])














