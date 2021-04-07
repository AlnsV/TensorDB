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
                 max_files_on_disk: int = 30,
                 **kwargs):
        """

        """
        self.env_mode = os.getenv("ENV_MODE") if use_env else ""
        self.base_path = os.path.join(ROOT_DIR, 'file_db') if base_path is None else base_path
        self.base_path = os.path.join(self.base_path, self.env_mode)
        self._files_settings = files_settings
        self.open_base_store: Dict[str, Dict[str, Any]] = {}
        self.max_files_on_disk = max_files_on_disk

        if use_env:
            self.base_path = os.path.join(self.base_path, os.getenv("ENV_MODE"))
            self._files_settings = files_settings[os.getenv("ENV_MODE")]

        self.s3_handler = s3_settings
        if s3_settings is not None:
            if isinstance(s3_settings, dict):
                self.s3_handler = S3Handler(**s3_settings)

        self.__dict__.update(**kwargs)

    def get_file_settings(self, path) -> Dict:
        file_settings_id = os.path.basename(os.path.normpath(path))
        return self._files_settings[file_settings_id]

    def _get_handler(self,
                     path: Union[str, List],
                     **kwargs) -> BaseStore:

        file_setting = self.get_file_settings(path).get('handler', {}).copy()
        local_path = self.complete_path(file_setting=file_setting, path=path)
        if local_path not in self.open_base_store:
            self.open_base_store[local_path] = {
                'data_handler': file_setting.get('data_handler', ZarrStore)(
                    base_path=self.base_path,
                    path=self.complete_path(file_setting=file_setting, path=path, omit_base_path=True),
                    s3_handler=self.s3_handler,
                    **file_setting
                ),
                'first_read_date': pd.Timestamp.now(),
                'num_use': 0
            }
        self.open_base_store[local_path]['num_use'] += 1
        return self.open_base_store[local_path]['data_handler']

    def read_from_formula(self, formula, **kwargs):
        data_fields = {}
        data_fields_intervals = [i for i, c in enumerate(formula) if c == '`']
        for i in range(0, len(data_fields_intervals), 2):
            name_data_field = formula[data_fields_intervals[i] + 1: data_fields_intervals[i + 1]]
            data_fields[name_data_field] = self.read(
                name_data_field
            )
        for name, dataset in data_fields.items():
            formula = formula.replace(f"`{name}`", f"data_fields['{name}']")
        return eval(formula)

    def read(self,
             path: Union[str, List],
             **kwargs) -> xarray.DataArray:

        file_settings = self.get_file_settings(path).get('read', {}).copy()
        file_settings.update(kwargs)
        if 'method' in file_settings:
            return getattr(self, file_settings['method'])(**file_settings)

        return self._get_handler(
            path=path,
            **kwargs
        ).read(
            **file_settings
        )

    def append(self,
               path: Union[str, List],
               **kwargs):
        file_settings = self.get_file_settings(path).get('append', {}).copy()
        file_settings.update(kwargs)
        if 'method' in file_settings:
            return getattr(self, file_settings['method'])(**file_settings)

        return self._get_handler(
            path=path,
            **kwargs
        ).append(
            **file_settings
        )

    def update(self,
               path: Union[str, List],
               **kwargs):

        file_settings = self.get_file_settings(path).get('update', {}).copy()
        file_settings.update(kwargs)
        if 'method' in file_settings:
            return getattr(self, file_settings['method'])(**file_settings)

        return self._get_handler(
            path=path,
            **kwargs
        ).update(
            **file_settings
        )

    def store(self,
              path: Union[str, List],
              **kwargs):

        file_settings = self.get_file_settings(path).get('store', {}).copy()
        file_settings.update(kwargs)
        if 'method' in file_settings:
            return getattr(self, file_settings['method'])(**file_settings)

        return self._get_handler(
            path=path,
            **kwargs
        ).store(
            **file_settings
        )

    def backup(self,
               path: Union[str, List],
               **kwargs):

        file_settings = self.get_file_settings(path).get('backup', {}).copy()
        file_settings.update(kwargs)
        if 'method' in file_settings:
            return getattr(self, file_settings['method'])(**file_settings)

        return self._get_handler(
            path=path,
            **kwargs
        ).backup(
            **file_settings
        )

    def update_from_backup(self,
                           path: Union[str, List],
                           **kwargs):

        file_settings = self.get_file_settings(path).get('update_from_backup', {}).copy()
        file_settings.update(kwargs)
        if 'method' in file_settings:
            return getattr(self, file_settings['method'])(**file_settings)

        return self._get_handler(
            path=path,
            **kwargs
        ).update_from_backup(
            **file_settings
        )

    def close(self,
              path: Union[str, List],
              **kwargs):

        file_settings = self.get_file_settings(path).get('close', {}).copy()
        file_settings.update(kwargs)
        if 'method' in file_settings:
            return getattr(self, file_settings['method'])(**file_settings)

        return self._get_handler(
            path=path,
            **kwargs
        ).close(
            **file_settings
        )

    def complete_path(self,
                      file_setting: Dict,
                      path: Union[List[str], str],
                      omit_base_path: bool = False):

        path = path if isinstance(path, list) else [path]
        if not omit_base_path:
            return os.path.join(self.base_path, file_setting.get('extra_path', ''), *path)

        return os.path.join(file_setting.get('extra_path', ''), *path)

    def exist(self,
              path: str,
              **kwargs):
        return self._get_handler(
            path,
            **kwargs
        ).exist(
            **kwargs
        )

    def add_file_setting(self, file_settings_id, file_settings):
        self._files_settings[file_settings_id] = file_settings
