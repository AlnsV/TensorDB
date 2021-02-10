import xarray
import numpy as np
import pandas as pd
import netCDF4
import ast
import os

from typing import Dict, List, Any, Union
from loguru import logger

from .core_handler import CoreNetcdfHandler, BaseCoreHandler
from .attributes_utils import get_attribute, get_all_attributes, save_attributes


def get_partition_index(data=None, index=None):
    if data is None and index is None:
        return xarray.DataArray(
            dims=['index', 'columns'],
            coords={
                'columns': ['partition_pos', 'internal_partition_pos'],
            },
        )

    return xarray.DataArray(
        data,
        dims=['index', 'columns'],
        coords={
            'columns': ['partition_pos', 'internal_partition_pos'],
            'index': index
        },
    )


def get_partition_name(data=None):
    if data is None:
        return xarray.DataArray(
            dims=['index', 'columns'],
            coords={
                'columns': ['partition_names'],
            },
        )

    return xarray.DataArray(
        data,
        dims=['index', 'columns'],
        coords={
            'index': np.array([data], dtype='<U500'),
            'columns': ['partition_names'],
        },
    )


class MetadataHandler:

    def __init__(self,
                 path: str,
                 first_write: bool,
                 *args,
                 **kwargs):

        self.path = path
        self.first_write = first_write
        self.metadata_path = os.path.join(self.path, 'metadata.nc')
        self.partition_names: List[str] = []

        if self.first_write:
            # create an empty file
            xarray.DataArray().to_netcdf(self.metadata_path)
            self.index = None
            self.save_attributes(**kwargs)
        else:
            self.index = CoreNetcdfHandler.get_external_computed_array(self.metadata_path, 'index')
            self.partition_names = self.get_attribute(name='partition_names')

    def _create_default_core_metadata_handler(self, group, first_write, dims_space=None):
        dims_space = dims_space
        if dims_space is None:
            dims_space = {'columns': 2}
        return CoreNetcdfHandler(
            self.metadata_path,
            group=group,
            dims=['index', 'columns'],
            dims_space=dims_space,
            dims_type={'index': 'dynamic', 'columns': 'fixed'},
            first_write=first_write
        )

    def concat_new_partition(self,
                             indexes: np.array,
                             partition: BaseCoreHandler):

        if self.index is not None:
            if np.any(self.index.coords['index'].isin(indexes)):
                raise ValueError(f"You are appending a repeated value in concat dimension.")

        if partition.name in self.partition_names:
            raise ValueError(f"You are concatenating an used partition")

        self.partition_names.append(partition.name)

        self.append_row_index(indexes, 0)

    def get_attribute(self, name, group: str = None, default: Any = None):
        return get_attribute(path=self.metadata_path, name=name, group=group, default=default)

    def get_all_attributes(self,  group: str = None):
        return get_all_attributes(path=self.metadata_path, group=group)

    def save_attributes(self, group: str = None, **kwargs):
        save_attributes(path=self.metadata_path, group=group, **kwargs)

    def append_row_index(self,
                         indexes: np.array,
                         internal_partition_pos: int = None):

        if len(self.partition_names) == 0:
            raise Exception(f"You can't append data to an empty file")

        internal_partition_pos = internal_partition_pos
        if internal_partition_pos is None:
            internal_partition_pos = self.get_last_internal_pos() + 1

        partition_pos = len(self.partition_names) - 1
        new_partition_pos = [[partition_pos, internal_partition_pos + i] for i, index in enumerate(indexes)]

        if self.index is None:
            self.index = get_partition_index(
                data=new_partition_pos,
                index=indexes
            )
        else:
            indexes = indexes[~np.isin(indexes, self.index.coords['index'].values)]
            self.index = xarray.concat(
                [self.index, get_partition_index(data=new_partition_pos, index=indexes)],
                dim='index'
            )

    def append_variable(self,
                        data: xarray.DataArray,
                        group: str,
                        update: bool = False,
                        unlimited_dims: List[str] = None):
        pass

    def get_last_partition_name(self) -> str:
        return self.partition_names[-1]

    def get_last_partition_path(self) -> str:
        return os.path.join(self.path, self.get_last_partition_name())

    def get_last_internal_pos(self) -> int:
        return self.index[-1].loc['internal_partition_pos'].values

    def get_partition_path(self, partition_pos: int) -> str:
        return os.path.join(self.path, self.partition_names[partition_pos])

    def get_partition_paths(self) -> List[str]:
        return [os.path.join(self.path, name) for name in self.partition_names]

    def _update_files(self, data, group, **kwargs):
        core_file = self._create_default_core_metadata_handler(group, self.first_write, **kwargs)
        core_file.append_data(data)

    @property
    def empty(self):
        return self.partition_names is None or len(self.partition_names) == 0

    def close(self):
        self.save()
        self.index = None
        self.partition_names = []

    def save(self):
        self.save_attributes(partition_names=self.partition_names)
        self._update_files(self.index, 'index')
        self.first_write = False


