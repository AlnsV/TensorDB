import xarray
import numpy as np
import pandas as pd
import os

from typing import Dict, List, Any, Union
from loguru import logger

from .core_handler import CoreNetcdfHandler, BaseCoreHandler
from .attributes_utils import get_attribute, get_all_attributes, save_attributes
from store_core.base_handler import BaseMetadataHandler


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


class MetadataHandler(BaseMetadataHandler):

    def __init__(self,
                 avoid_load_index: bool = False,
                 *args,
                 **kwargs):

        super().__init__(*args, **kwargs)

        if self.first_write:
            # create an empty file
            xarray.DataArray().to_netcdf(self.metadata_path)
            self.save_attributes(**kwargs)
        else:
            if not avoid_load_index:
                self.index = CoreNetcdfHandler.get_external_computed_array(self.metadata_path, 'index')
            self.partitions_metadata = self.get_attribute(name='partitions_metadata')
            self.last_s3_modified_date = self.get_attribute(name='last_s3_modified_date')

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
                             partition: BaseCoreHandler,
                             *args,
                             **kwargs):

        if self.index is not None:
            if np.any(self.index.coords['index'].isin(indexes)):
                raise ValueError(f"You are appending a repeated value in concat dimension.")

        if partition.name in self.partitions_metadata:
            raise ValueError(f"You are concatenating an used partition")

        self.partitions_metadata[partition.name] = {}
        self._partition_names = None

        self.append_index(indexes, 0, *args, **kwargs)

    def append_index(self,
                     indexes: np.array,
                     internal_partition_pos: int = None,
                     *args,
                     **kwargs):

        if len(self.partition_names) == 0:
            raise Exception(f"You can't append data to an empty file")

        self.partitions_metadata[self.get_last_partition_name()].update(**kwargs)

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
        raise NotImplemented(f"The append variable method is not yet implemented")

    def _update_files(self, data, group, **kwargs):
        core_file = self._create_default_core_metadata_handler(group, self.first_write, **kwargs)
        core_file.append_data(data)

    @property
    def empty(self):
        return self.partition_names is None or len(self.partition_names) == 0

    def close(self):
        self.save()
        self.index = None
        self.partitions_metadata = {}

    def save(self):
        self.save_attributes(
            partitions_metadata=self.partitions_metadata,
            last_s3_modified_date=self.last_s3_modified_date
        )
        self._update_files(self.index, 'index')
        self.first_write = False

    def get_last_internal_pos(self) -> int:
        return self.index[-1].loc['internal_partition_pos'].values

    def get_last_partition_name(self) -> str:
        return self.partition_names[-1]

    def get_last_partition_path(self) -> str:
        return os.path.join(self.base_path, self.get_last_partition_name())

    def get_partition_path(self, partition_id: Union[int, str]) -> str:
        partition_pos = partition_id
        if isinstance(partition_pos, str):
            partition_pos = self.partition_names.index(partition_id)
        return os.path.join(self.base_path, self.partition_names[partition_pos])

    def get_partition_paths(self) -> List[str]:
        return [os.path.join(self.base_path, name) for name in self.partition_names]

    def get_attribute(self, name, group: str = None, default: Any = None):
        return get_attribute(path=self.metadata_path, name=name, group=group, default=default)

    def get_all_attributes(self, group: str = None):
        return get_all_attributes(path=self.metadata_path, group=group)

    def save_attributes(self, group: str = None, **kwargs):
        save_attributes(path=self.metadata_path, group=group, **kwargs)
