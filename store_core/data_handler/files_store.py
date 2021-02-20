import xarray
import os
import pandas as pd

from typing import Dict, List, Any, Union, Callable
from loguru import logger

from store_core.base_handler.base_store import BaseStore
from store_core.s3_handler.s3_handler import S3Handler
from config_path.config_root_dir import ROOT_DIR
from store_core.netcdf_handler.metadata_handler import MetadataHandler, BaseMetadataHandler


class FilesStore:
    def __init__(self,
                 files_settings: Dict[str, Dict[str, Any]],
                 data_handler: Callable[[BaseStore], BaseStore],
                 base_path: str = None,
                 use_env: bool = True,
                 s3_settings: Union[Dict[str, str], S3Handler] = None,
                 metadata_handler: BaseMetadataHandler = None,
                 max_files_in_disk: int = 30,
                 *args,
                 **kwargs):

        self.base_path = base_path
        if self.base_path is None:
            self.base_path = os.path.join(ROOT_DIR, 'file_db')

        self.files_settings = files_settings
        self.open_base_store: Dict[str, Dict[str, Any]] = {}
        self.data_handler = data_handler
        self.max_files_in_disk = max_files_in_disk

        if use_env:
            self.base_path = os.path.join(self.base_path, os.getenv("ENV_MODE"))
            self.files_settings = files_settings[os.getenv("ENV_MODE")]

        self.s3_handler = s3_settings
        if s3_settings is not None:
            if isinstance(s3_settings, dict):
                self.s3_handler = S3Handler(**s3_settings)

        self.metadata_handler = metadata_handler
        if self.metadata_handler is None:
            self.metadata_handler = MetadataHandler

        self.__dict__.update(**kwargs)

    def get_handler(self, file_setting_id: str, path: Union[str, List] = None, avoid_download: bool = False, **kwargs):
        if not avoid_download:
            self.download_partitions(file_setting_id, path)

        local_base_path = self.complete_path(file_setting_id=file_setting_id, path=path)
        if local_base_path not in self.open_base_store or not self.open_base_store[local_base_path]['open']:

            if len(self.open_base_store) + 1 > self.max_files_in_disk:
                self._delete_excess_files()

            kwargs['base_path'] = local_base_path
            self.open_base_store[local_base_path] = {
                'file_setting_id': file_setting_id,
                'path': path,
                'handler': self.data_handler(**self.files_settings[file_setting_id], **kwargs),
                'open': True,
                'first_read_date': pd.Timestamp.now(),
                'num_use': 0
            }
        self.open_base_store[local_base_path]['num_use'] += 1
        return self.open_base_store[local_base_path]['handler']

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

        if 'last_modified_date' not in kwargs:
            kwargs['last_modified_date'] = pd.Timestamp.now()

        self.get_handler(file_setting_id, path=path, **kwargs).append_data(new_data, *args, **kwargs)

    def update_data(self,
                    new_data: xarray.DataArray,
                    file_setting_id: str,
                    path: Union[str, List] = None,
                    *args,
                    **kwargs):

        if 'update_data' in self.files_settings[file_setting_id]:
            return getattr(self, self.files_settings[file_setting_id]['update_data'])(new_data, *args, **kwargs)

        if 'last_modified_date' not in kwargs:
            kwargs['last_modified_date'] = pd.Timestamp.now()

        self.get_handler(file_setting_id, path=path, **kwargs).update_data(new_data, *args, **kwargs)

    def store_data(self,
                   new_data: xarray.DataArray,
                   file_setting_id: str,
                   path: Union[str, List] = None,
                   *args,
                   **kwargs):

        if 'store_data' in self.files_settings[file_setting_id]:
            return getattr(self, self.files_settings[file_setting_id]['store_data'])(new_data, *args, **kwargs)

        logger.info("storing data")
        if 'last_modified_date' not in kwargs:
            kwargs['last_modified_date'] = pd.Timestamp.now()

        self.get_handler(
            avoid_download=True, file_setting_id=file_setting_id, path=path, first_write=True, **kwargs
        ).store_data(
            new_data, *args, **kwargs
        )

    def download_partitions(self, file_setting_id: str, path: List[str] = None, *args, **kwargs):
        if self.s3_handler is None:
            return None
        logger.info("in downloading")

        s3_base_path = self.complete_path(file_setting_id, path=path, omit_base_path=True)
        local_base_path = self.complete_path(file_setting_id, path=path)

        metadata_file_name = self.files_settings[file_setting_id]['metadata_file_name']
        s3_metadata_path = os.path.join(s3_base_path, metadata_file_name)
        local_metadata_path = os.path.join(local_base_path, metadata_file_name)

        self.close_store(file_setting_id, path, *args, **kwargs)

        last_modified_date = self.s3_handler.get_last_modified_date(
            s3_path=s3_metadata_path,
            **self.files_settings[file_setting_id]
        )

        act_metadata = None
        if os.path.exists(local_metadata_path):
            act_metadata = self.metadata_handler(path=local_metadata_path,
                                                 base_path=local_base_path,
                                                 first_write=False,
                                                 metadata_file_name=metadata_file_name,
                                                 avoid_load_index=True)
            logger.info(act_metadata.last_s3_modified_date)
            logger.info(last_modified_date)
            if act_metadata.last_s3_modified_date is not None and \
                    act_metadata.last_s3_modified_date == last_modified_date:
                logger.info("inside the if")
                return

        self.s3_handler.download_file(s3_path=s3_metadata_path,
                                      local_path=local_metadata_path,
                                      **self.files_settings[file_setting_id])

        new_metadata = self.metadata_handler(path=local_metadata_path,
                                             base_path=local_base_path,
                                             first_write=False,
                                             metadata_file_name=metadata_file_name,
                                             avoid_load_index=True)

        for partition_name in new_metadata.partition_names:
            s3_partition_path = os.path.join(s3_base_path, partition_name)
            local_partition_path = os.path.join(local_base_path, partition_name)
            if not FilesStore.is_updated(new_metadata, act_metadata, partition_name):
                self.s3_handler.download_file(s3_path=s3_partition_path,
                                              local_path=local_partition_path,
                                              **self.files_settings[file_setting_id])

    @staticmethod
    def is_updated(new_metadata: BaseMetadataHandler,
                   act_metadata: BaseMetadataHandler,
                   partition_name: str):
        if act_metadata is None:
            return False

        if partition_name not in act_metadata.partitions_metadata:
            return False

        if 'last_modified_date' not in act_metadata.partitions_metadata[partition_name]:
            return False

        local_partition_path = new_metadata.get_partition_path(partition_name)
        if not os.path.exists(local_partition_path):
            return False

        act_date = act_metadata.partitions_metadata[partition_name]['last_modified_date']
        new_date = new_metadata.partitions_metadata[partition_name]['last_modified_date']

        return act_date == new_date

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
            modified_partitions = self.open_base_store[complete_path]['handler'].modified_partitions.copy()
            self.open_base_store[complete_path]['handler'].close()
            self.open_base_store[complete_path]['open'] = False
            self._upload_files(
                file_setting_id=file_setting_id,
                path=path,
                modified_partitions=modified_partitions
            )

    def _upload_files(self,
                      file_setting_id: str,
                      path: Union[List[str], str],
                      modified_partitions: set):

        if self.s3_handler is None or len(modified_partitions) == 0:
            return

        s3_base_path = self.complete_path(file_setting_id, path=path, omit_base_path=True)
        local_base_path = self.complete_path(file_setting_id, path=path)
        metadata_file_name = self.files_settings[file_setting_id]['metadata_file_name']

        modified_partitions.add(metadata_file_name)

        # update the modified files to s3
        for partition_name in modified_partitions:
            self.s3_handler.upload_file(
                s3_path=os.path.join(s3_base_path, partition_name),
                local_path=os.path.join(local_base_path, partition_name),
                **self.files_settings[file_setting_id]
            )

        last_modified_date = self.s3_handler.get_last_modified_date(
            s3_path=os.path.join(s3_base_path, metadata_file_name),
            **self.files_settings[file_setting_id]
        )

        # modify the metadata file saving the last modified date of s3, this will be useful to avoid
        # that multiple services that share the same memory download the same metadata file again and again
        metadata = self.metadata_handler(
            path=os.path.join(local_base_path, metadata_file_name),
            base_path=local_base_path,
            first_write=False,
            metadata_file_name=metadata_file_name,
            avoid_load_index=True
        )
        metadata.save_attributes(last_s3_modified_date=last_modified_date)

    def delete_store(self, file_setting_id: str, path: Union[List[str], str] = None, *args, **kwargs):
        self.close_store(file_setting_id, path)
        local_base_path = self.complete_path(file_setting_id, path=path)
        metadata_file_name = self.files_settings[file_setting_id]['metadata_file_name']
        local_metadata_path = os.path.join(local_base_path, metadata_file_name)
        if not os.path.exists(local_metadata_path):
            return

        del self.open_base_store[local_base_path]

        total_paths = self.metadata_handler(
            path=local_metadata_path,
            base_path=local_base_path,
            first_write=False,
            metadata_file_name=metadata_file_name,
            avoid_load_index=True
        ).get_partition_paths()
        total_paths += [local_metadata_path]

        for path in total_paths:
            os.remove(path)

    def _delete_excess_files(self, exclude: List[str] = None):
        exclude = [] if exclude is None else exclude
        act_date = pd.Timestamp.now()
        stores_to_delete = {
            k: ((act_date - v['first_read_date']).days + 1) / v['num_use']
            for k, v in self.open_base_store.items() if k not in exclude
        }

        if len(stores_to_delete) == 0:
            return
        less_used_store = min(stores_to_delete.items(), key=lambda x: x[1])[0]
        self.delete_store(**self.open_base_store[less_used_store])

    def close(self):
        for key in list(self.open_base_store.keys()):
            self.close_store(**self.open_base_store[key])
