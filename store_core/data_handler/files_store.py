import xarray
import os

from typing import Dict, List, Any, Union, Callable
from loguru import logger
from numpy import nan
from pandas import Timestamp

from store_core.base_handler.base_store import BaseStore
from store_core.s3_handler.s3_handler import S3Handler
from store_core.zarr_handler.zarr_store import ZarrStore
from config.config_root_dir import ROOT_DIR


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

        TODO
        ----
        1) Add methods to validate the data, for example should be useful to check the proportion of missing data
            before save the data
        2) Add more methods to modify the data like bfill or other xarray methods that can be improved when
            appending data.
    """

    def __init__(self,
                 files_settings: Dict[str, Dict[str, Any]],
                 base_path: str = None,
                 use_env: bool = False,
                 s3_settings: Union[Dict[str, str], S3Handler] = None,
                 max_files_on_disk: int = 30,
                 **kwargs):

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

    def add_file_setting(self, file_settings_id, file_settings):
        self._files_settings[file_settings_id] = file_settings

    def get_file_settings(self, path) -> Dict:
        file_settings_id = os.path.basename(os.path.normpath(path))
        return self._files_settings[file_settings_id]

    def _get_handler(self, path: Union[str, List], file_settings: Dict = None) -> BaseStore:
        handler_settings = self.get_file_settings(path) if file_settings is None else file_settings
        handler_settings = handler_settings.get('handler', {})
        local_path = self._complete_path(file_setting=handler_settings, path=path)
        if local_path not in self.open_base_store:
            self.open_base_store[local_path] = {
                'data_handler': handler_settings.get('data_handler', ZarrStore)(
                    base_path=self.base_path,
                    path=self._complete_path(file_setting=handler_settings, path=path, omit_base_path=True),
                    s3_handler=self.s3_handler,
                    **handler_settings
                ),
                'first_read_date': Timestamp.now(),
                'num_use': 0
            }
        self.open_base_store[local_path]['num_use'] += 1
        return self.open_base_store[local_path]['data_handler']

    def _personalize_method(func):
        def wrap(self, path: str, **kwargs):
            file_settings = self.get_file_settings(path)
            kwargs.update({
                'action_type': func.__name__,
                'handler': self._get_handler(path=path, file_settings=file_settings),
                'file_settings': file_settings
            })
            method_settings = file_settings.get(kwargs['action_type'], {})
            if 'personalized_method' in method_settings:
                return getattr(self, method_settings['personalized_method'])(**kwargs)
            if 'data_methods' in method_settings:
                kwargs['new_data'] = self._apply_data_methods(data_methods=method_settings['data_methods'], **kwargs)
            return func(self, **{**kwargs, **method_settings})

        return wrap

    @_personalize_method
    def read(self, handler: BaseStore, **kwargs) -> xarray.DataArray:
        return handler.read(**kwargs)

    @_personalize_method
    def append(self, handler: BaseStore, **kwargs):
        return handler.append(**kwargs)

    @_personalize_method
    def update(self, handler: BaseStore, **kwargs):
        return handler.update(**kwargs)

    @_personalize_method
    def store(self, handler: BaseStore, **kwargs):
        return handler.store(**kwargs)

    @_personalize_method
    def backup(self, handler: BaseStore, **kwargs):
        return handler.backup(**kwargs)

    @_personalize_method
    def update_from_backup(self, handler: BaseStore, **kwargs):
        return handler.update_from_backup(**kwargs)

    @_personalize_method
    def close(self, handler: BaseStore, **kwargs):
        return handler.close(**kwargs)

    def exist(self,
              path: str,
              **kwargs):
        return self._get_handler(
            path,
            **kwargs
        ).exist(
            **kwargs
        )

    def _complete_path(self,
                       file_setting: Dict,
                       path: Union[List[str], str],
                       omit_base_path: bool = False):

        path = path if isinstance(path, list) else [path]
        if not omit_base_path:
            return os.path.join(self.base_path, file_setting.get('extra_path', ''), *path)
        return os.path.join(file_setting.get('extra_path', ''), *path)

    def _apply_data_methods(self, data_methods: List[str], file_settings: Dict, **kwargs):
        results = {**{'new_data': None}, **kwargs}
        for method in data_methods:
            result = getattr(self, method)(
                **{**file_settings.get(method, {}), **results},
                file_settings=file_settings
            )
            result = result if isinstance(result, dict) else {'new_data': result}
            results.update(result)
        return results['new_data']

    def read_from_formula(self, file_settings, **kwargs):
        formula = file_settings['read_from_formula']['formula']
        data_fields = {}
        data_fields_intervals = [i for i, c in enumerate(formula) if c == '`']
        for i in range(0, len(data_fields_intervals), 2):
            name_data_field = formula[data_fields_intervals[i] + 1: data_fields_intervals[i + 1]]
            data_fields[name_data_field] = self.read(name_data_field)
        for name, dataset in data_fields.items():
            formula = formula.replace(f"`{name}`", f"data_fields['{name}']")
        return eval(formula)

    def reindex(self,
                new_data: xarray.DataArray,
                reindex_path: str,
                coords_to_reindex: List[str],
                method_ffill: str = None,
                **kwargs) -> Union[xarray.DataArray, None]:
        if new_data is None:
            return None
        data_reindex = self.read(path=reindex_path, **kwargs)
        return new_data.reindex(
            {coord: data_reindex.coords[coord] for coord in coords_to_reindex},
            method=method_ffill
        )

    def last_valid_dim(self,
                       new_data: xarray.DataArray,
                       dim: str,
                       **kwargs) -> Union[xarray.DataArray, None]:
        if new_data is None:
            return None
        return new_data.notnull().cumsum(dim=dim).idxmax(dim=dim)

    def replace_values(self,
                       new_data: xarray.DataArray,
                       replace_path: str,
                       value: Any = nan,
                       **kwargs) -> Union[xarray.DataArray, None]:
        if new_data is None:
            return new_data
        replace_data_array = self.read(path=replace_path, **kwargs)
        return new_data.where(replace_data_array.sel(new_data.coords), value)

    def fillna(self,
               new_data: xarray.DataArray,
               value: Any = nan,
               **kwargs) -> Union[xarray.DataArray, None]:

        if new_data is None:
            return new_data
        return new_data.fillna(value)

    def ffill(self,
              handler: BaseStore,
              new_data: xarray.DataArray,
              dim: str,
              action_type: str,
              limit: int = None,
              **kwargs) -> Union[xarray.DataArray, None]:

        if new_data is None:
            return new_data
        data_concat = new_data
        if action_type != 'store':
            data = handler.read()
            data = data.sel({dim: data.coords[dim] < new_data.coords[dim][0]})
            if data.sizes[dim] > 0:
                data_concat = xarray.concat([data.isel({dim: [-1]}), new_data], dim=dim)

        return data_concat.ffill(dim=dim, limit=limit).sel(new_data.coords)

    def replace_last_valid_dim(self,
                               new_data: xarray.DataArray,
                               replace_path: str,
                               dim: str,
                               value: Any = nan,
                               **kwargs) -> Union[xarray.DataArray, None]:
        if new_data is None:
            return new_data

        last_valid = self.read(path=replace_path, **kwargs)
        last_valid = last_valid.fillna(new_data.coords[dim][-1])
        last_valid = xarray.DataArray(
            data=new_data.coords[dim].values[:, None] <= last_valid.values,
            coords={
                dim: new_data.coords[dim],
                **{dim_last: coord for dim_last, coord in last_valid.coords.items() if dim != dim_last}
            },
            dims=new_data.dims
        )
        return new_data.where(last_valid.sel(new_data.coords), value)
