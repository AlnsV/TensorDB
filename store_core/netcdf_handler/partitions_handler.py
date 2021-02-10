import xarray
import numpy as np
import os

from typing import Dict, List, Any, Union, Callable, Generic
from loguru import logger

from store_core.base_handler.base_store import BaseStore
from store_core.netcdf_handler.core_handler import CoreNetcdfHandler, BaseCoreHandler
from store_core.netcdf_handler.dims_handler import DimsHandler
from store_core.netcdf_handler.metadata_handler import MetadataHandler
from store_core.utils import modify_coord_dtype


class PartitionsStore(BaseStore):
    def __init__(self,
                 path: str,
                 dims_conversion: Dict[str, str] = None,
                 default_value: Any = np.nan,
                 max_cached_data: int = 0,
                 first_write: bool = False,
                 core_handler: Union[Callable[[BaseCoreHandler], BaseCoreHandler], str] = None,
                 *args,
                 **kwargs):

        super().__init__(*args, **kwargs)

        self.metadata = MetadataHandler(
            path=path,
            first_write=first_write,
            *args,
            **kwargs
        )

        self.dims_handler = DimsHandler(
            coords={},
            *args,
            **kwargs
        )

        self.default_value = default_value
        self.cached_data = []
        self.modified_partitions = set()
        self._last_partition = None
        self._last_partition_name = None
        self.dataset = None
        self.dims_conversion = dims_conversion
        self.max_cached_data = max_cached_data
        self.core_handler = core_handler
        if self.core_handler is None:
            self.core_handler = CoreNetcdfHandler
        elif isinstance(self.core_handler, str):
            pass

        self.count_writes = 0

    @property
    def index(self):
        if self.dims_conversion is not None:
            concat_dim = self.dims_handler.concat_dim
            return self.metadata.index.assign_coords({
                concat_dim: modify_coord_dtype(
                    self.metadata.index.coords[concat_dim].values, self.dims_conversion[concat_dim]
                )
            })
        return self.metadata.index

    @property
    def partition_names(self) -> List[str]:
        return self.metadata.partition_names

    @property
    def last_partition(self) -> BaseCoreHandler:
        if self._last_partition_name is None or self.metadata.get_last_partition_name() != self._last_partition_name:
            self._last_partition_name = self.metadata.get_last_partition_name()
            last_partition_path = self.metadata.get_last_partition_path()
            self._last_partition = self.get_core_partition(path=last_partition_path)

        return self._last_partition

    def get_core_partition(self, path: str, first_write: bool = False, *args, **kwargs) -> BaseCoreHandler:
        return self.core_handler(
            path=path,
            dims=self.dims_handler.dims,
            dims_space=self.dims_handler.dims_space,
            dims_type=self.dims_handler.dims_type,
            concat_dim=self.dims_handler.concat_dim,
            default_value=self.default_value,
            default_free_value=self.dims_handler.default_free_value,
            first_write=first_write,
            *args,
            **kwargs
        )

    def append_data(self, new_data: xarray.DataArray, force_write: bool = False, *args, **kwargs):

        self.close_dataset()

        if new_data is not None:
            self.cached_data.append(new_data)

        # TODO: Improve this condition
        if (
                force_write or
                len(self.cached_data) >= self.max_cached_data or
                (
                    not self.metadata.empty and
                    self.last_partition.dims_handler.free_sizes[self.dims_handler.concat_dim] == len(self.cached_data)
                )
        ):
            new_data = xarray.concat(self.cached_data, dim=self.dims_handler.concat_dim)
            self.count_writes += 1
            if self.metadata.empty or self.last_partition.dims_handler.is_complete():
                self.write_new_partition(new_data)
            else:
                self.last_partition.append_data(new_data)
                self.metadata.append_row_index(new_data.coords['index'].values)
                self.modified_partitions.add(self.last_partition.path)

            self.cached_data = []

    def write_new_partition(self, new_data: xarray.DataArray, *args, **kwargs):
        self.close_dataset()
        new_partition = self.get_core_partition(
            path=os.path.join(self.metadata.path, str(new_data.coords[self.dims_handler.concat_dim].values[0]) + '.nc'),
            first_write=True,
            *args,
            **kwargs
        )
        new_partition.write_file(new_data)
        self.metadata.concat_new_partition(
            new_partition.dims_handler.used_coords[self.dims_handler.concat_dim],
            new_partition
        )
        self.modified_partitions.add(new_partition.path)

    def update_data(self, new_data: xarray.DataArray, *args, **kwargs):
        self.close_dataset()

        partitions_index_groups = new_data.groupby(self.index.sel(columns='partition_pos'))

        for partition_pos, act_data in partitions_index_groups:
            partition_path = self.metadata.get_partition_path(partition_pos)
            partition = self.get_core_partition(partition_path, *args, **kwargs)
            partition.update_data(act_data)
            self.modified_partitions.add(partition.path)

    def store_data(self, new_data: xarray.DataArray, *args, **kwargs):
        self.close_dataset()

        chunk_size = self.dims_handler.dims_space[self.dims_handler.concat_dim]
        for i in range(0, new_data.sizes[self.dims_handler.concat_dim], chunk_size):
            act_data = new_data.isel(**{
                self.dims_handler.concat_dim:
                    list(range(i, min(i + chunk_size, new_data.sizes[self.dims_handler.concat_dim])))
            })
            self.write_new_partition(act_data)

    def get_dataset(self) -> xarray.Dataset:
        if self.dataset is not None:
            return self.dataset

        self.dataset = xarray.open_mfdataset(
            self.metadata.get_partition_paths(),
            concat_dim=['index'],
            combine='nested'
        )

        if self.dims_handler.default_free_value is not None:
            self.dataset = self.dataset.sel({
                dim: [not self.dims_handler.is_free(val, dim) for val in coord.values]
                for dim, coord in self.dataset.coords.items()
            })

        if self.dims_conversion is not None:
            self.dataset = self.dataset.assign_coords({
                name: modify_coord_dtype(coord.values, self.dims_conversion[name])
                for name, coord in self.dataset.coords.items()
            })

        return self.dataset

    def save(self):
        if len(self.cached_data):
            self.append_data(None, True)
        self.close_dataset()
        self.metadata.save()

    def close_dataset(self):
        if self.dataset is not None:
            self.dataset.close()
            self.dataset = None

    def close(self):
        self.save()
        self.metadata.close()
