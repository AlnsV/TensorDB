import xarray
import numpy as np
import pandas as pd
import netCDF4
import ast
import os

from typing import Dict, List, Any, Union
from loguru import logger

from .core_handler import CoreNetcdfHandler


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

        if self.first_write:
            # create an empty file
            xarray.DataArray().to_netcdf(self.metadata_path)
            self.partition_names = None
            self.index = None
            self.save_metadata(**kwargs)
        else:
            self.index = CoreNetcdfHandler.get_external_computed_array(self.metadata_path, 'index')
            self.partition_names = CoreNetcdfHandler.get_external_computed_array(self.metadata_path, 'partition_names')

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
                             partition: CoreNetcdfHandler):

        if self.index is not None:
            if np.any(self.index.coords['index'].isin(indexes)):
                raise ValueError(f"You are appending a repeated value in concat dimension.")

        if self.partition_names is None:
            self.partition_names = get_partition_name(partition.path)
        elif partition.path in self.partition_names.coords['index']:
            raise ValueError(f"You are concatenating an used partition")
        else:
            self.partition_names = xarray.concat(
                [self.partition_names, get_partition_name(partition.path)],
                dim='index',
            )

        self.append_row_index(indexes, 0)

        # create an empty group
        xarray.DataArray().to_netcdf(self.metadata_path, group=partition.path, mode='a')

        # self.save_partition_metadata(partition)

    # def save_partition_metadata(self, partition: CoreNetcdfHandler):
    #     attributes = {}
    #     transformed_to_str_data = []
    #
    #     for key, val in partition.get_descriptors().items():
    #         if isinstance(val, (list, dict, bool)) or val != val or val is None:
    #             transformed_to_str_data.append(key)
    #             attributes[key] = str(val)
    #         else:
    #             attributes[key] = val
    #
    #     attributes['transformed_to_str_data'] = str(transformed_to_str_data)
    #     dataset = netCDF4.Dataset(self.metadata_path, mode='a')
    #     group_partition = dataset.groups.get(partition.path, dataset)
    #     group_partition.setncatts(attributes)
    #     dataset.close()

    def save_metadata(self, group: str = None, **kwargs):
        attributes = {}
        transformed_to_str_data = []

        for key, val in kwargs.items():
            if isinstance(val, (list, dict, bool)) or val != val or val is None:
                transformed_to_str_data.append(key)
                attributes[key] = str(val)
            else:
                attributes[key] = val

        attributes['transformed_to_str_data'] = str(transformed_to_str_data)
        dataset = netCDF4.Dataset(self.metadata_path, mode='a')
        group_partition = dataset.groups.get(group, dataset)
        group_partition.setncatts(attributes)
        dataset.close()

    def append_row_index(self,
                         indexes: np.array,
                         internal_partition_pos: int = None):

        if self.partition_names is None:
            raise Exception(f"You can't append data to an empty file")

        internal_partition_pos = internal_partition_pos
        if internal_partition_pos is None:
            internal_partition_pos = self.get_last_internal_pos()

        partition_pos = self.partition_names.sizes['index'] - 1
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
        return self.partition_names.values[self.index[-1].loc['partition_pos'].values][0]

    def get_last_internal_pos(self) -> int:
        return self.index[-1].loc['internal_partition_pos'].values

    def get_metadata(self, group: str = None) -> Dict[str, Any]:
        dataset = netCDF4.Dataset(self.metadata_path, mode='r')
        group_partition = dataset.groups.get(group, dataset)
        attributes = {}
        transform = set(ast.literal_eval(group_partition.transformed_to_str_data))
        for key, val in group_partition.__dict__.items():
            if key == 'transformed_to_str_data':
                continue

            if key in transform:
                if val == 'True' or val == 'False':
                    val = bool(val)
                elif val == 'nan':
                    val = np.nan
                else:
                    val = ast.literal_eval(val)

            attributes[key] = val

        dataset.close()

        return attributes

    def _update_files(self, data, group, **kwargs):
        core_file = self._create_default_core_metadata_handler(group, self.first_write, **kwargs)
        core_file.append_data(data)

    @property
    def empty(self):
        return self.partition_names is None or len(self.partition_names) == 0

    def close(self):
        self.save()
        self.index = None
        self.partition_names = None

    def save(self):
        self._update_files(self.index, 'index')
        self._update_files(self.partition_names, 'partition_names', dims_space={'columns': 1})
        self.first_write = False


