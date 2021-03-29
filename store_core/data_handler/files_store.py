import xarray
import os
import pandas as pd

from typing import Dict, List, Any, Union, Callable
from loguru import logger

from store_core.base_handler.base_store import BaseStore
from store_core.s3_handler.s3_handler import S3Handler
from store_core.zarr_handler.zarr_store import ZarrStore
from config_path.config_root_dir import ROOT_DIR


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
        self._files_settings = files_settings
        self.open_base_store: Dict[str, Dict[str, Any]] = {}
        self.max_files_in_disk = max_files_in_disk

        if use_env:
            self.base_path = os.path.join(self.base_path, os.getenv("ENV_MODE"))
            self._files_settings = files_settings[os.getenv("ENV_MODE")]

        self.s3_handler = s3_settings
        if s3_settings is not None:
            if isinstance(s3_settings, dict):
                self.s3_handler = S3Handler(**s3_settings)

        self.__dict__.update(**kwargs)

    @property
    def file_settings(self):
        return self._files_settings

    def get_file_setting(self, path):
        file_setting = os.path.basename(os.path.normpath(path))
        return self._files_settings.get(file_setting, self._files_settings.get('generic', {}))

    def get_handler(self,
                    path: Union[str, List],
                    **kwargs) -> BaseStore:

        file_setting = self.get_file_setting(path)
        local_path = self.complete_path(file_setting=file_setting, path=path)
        if local_path not in self.open_base_store:
            self.open_base_store[local_path] = {
                'data_handler': file_setting.get('data_handler', ZarrStore)(
                    base_path=self.base_path,
                    path=self.complete_path(file_setting=file_setting, path=path, omit_base_path=True),
                    s3_handler=self.s3_handler,
                    **file_setting,
                    **kwargs
                ),
                'first_read_date': pd.Timestamp.now(),
                'num_use': 0
            }
        self.open_base_store[local_path]['num_use'] += 1
        return self.open_base_store[local_path]['data_handler']

    def get_dataset_from_formula(self, path, *args, **kwargs):
        file_setting = self.get_file_setting(path)
        formula = file_setting['formula']
        data_fields_intervals = [i for i, c in enumerate(formula) if c == '`']
        data_fields = {}
        for i in range(0, len(data_fields_intervals), 2):
            name_data_field = formula[data_fields_intervals[i] + 1: data_fields_intervals[i + 1]]
            data_fields[name_data_field] = self.get_dataset(
                name_data_field,
                *args,
                **kwargs
            )

        for name, dataset in data_fields.items():
            formula = formula.replace(f"`{name}`", f"data_fields['{name}']")

        dataset_formula = eval(formula)
        return dataset_formula

    def get_dataset(self,
                    path: Union[str, List],
                    *args,
                    **kwargs) -> xarray.Dataset:
        """
        TODO: Add a boolean parameter that allow the user control if the dataset must be always check if it is equal
            to the backup
        """

        file_setting = self.get_file_setting(path)
        if 'get_dataset' in file_setting:
            return getattr(self, file_setting['get_dataset'])(*args, **kwargs)

        if file_setting.get('on_fly', False):
            return self.get_dataset_from_formula(path, *args, **kwargs)

        return self.get_handler(
            path=path,
            **kwargs
        ).get_dataset()

    def append_data(self,
                    path: Union[str, List],
                    *args,
                    **kwargs):

        file_setting = self.get_file_setting(path)
        if 'append_data' in file_setting:
            return getattr(self, file_setting['append_data'])(
                file_setting=file_setting,
                path=path,
                *args,
                **kwargs
            )

        return self.get_handler(
            path=path,
            **kwargs
        ).append_data(
            *args,
            **kwargs
        )

    def update_data(self,
                    path: Union[str, List],
                    *args,
                    **kwargs):

        file_setting = self.get_file_setting(path)
        if 'update_data' in file_setting:
            return getattr(self, file_setting['update_data'])(
                path=path,
                *args,
                **kwargs
            )

        return self.get_handler(
            path=path,
            **kwargs
        ).update_data(
            *args,
            **kwargs
        )

    def store_data(self,
                   path: Union[str, List],
                   *args,
                   **kwargs):

        file_setting = self.get_file_setting(path)
        if 'store_data' in file_setting:
            return getattr(self, file_setting['store_data'])(
                path=path,
                *args,
                **kwargs
            )

        return self.get_handler(
            path=path,
            **kwargs
        ).store_data(
            *args,
            **kwargs
        )

    def backup(self,
               path: Union[str, List],
               *args,
               **kwargs):

        file_setting = self.get_file_setting(path)
        if 'backup' in file_setting:
            return getattr(self, file_setting['backup'])(
                file_setting=file_setting,
                path=path,
                *args,
                **kwargs
            )

        return self.get_handler(
            path=path,
            **kwargs
        ).backup(
            *args,
            **kwargs
        )

    def update_from_backup(self,
                           path: Union[str, List],
                           *args,
                           **kwargs):

        file_setting = self.get_file_setting(path)
        if 'update_from_backup' in file_setting:
            return getattr(self, file_setting['update_from_backup'])(
                file_setting=file_setting,
                path=path,
                *args,
                **kwargs
            )

        return self.get_handler(
            path=path,
            **kwargs
        ).update_from_backup(
            *args,
            **kwargs
        )

    def close(self,
              path: Union[str, List],
              *args,
              **kwargs):

        file_setting = self.get_file_setting(path)
        if 'close' in file_setting:
            return getattr(self, file_setting['close'])(
                path=path,
                *args,
                **kwargs
            )

        return self.get_handler(
            path=path,
            **kwargs
        ).close(
            *args,
            **kwargs
        )

    def complete_path(self,
                      file_setting: Dict,
                      path: Union[List[str], str],
                      omit_base_path: bool = False):

        path = path if isinstance(path, list) else [path]
        if not omit_base_path:
            return os.path.join(self.base_path, file_setting.get('extra_path', ''), *path)

        return os.path.join(file_setting.get('extra_path', ''), *path)

    def add_file_setting(self, name, file_setting):
        self._files_settings[name] = file_setting

