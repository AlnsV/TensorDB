import xarray
import numpy as np
import os

from typing import Dict, List, Any, Union, Callable, Generic
from loguru import logger

from store_core.base_handler.base_store import BaseStore
from store_core.netcdf_handler.core_handler import CoreNetcdfHandler, BaseCoreHandler
from store_core.netcdf_handler.metadata_handler import MetadataHandler
from store_core.utils import modify_coord_dtype


class PartitionsStore(BaseStore):
    """
    PartitionsStore
    ---------------
    It's a simple handler for partitioned xarray.

    Provide a set of method to efficiently append, store, update and retrieve data from some xarray
    supported file format, combined with that it has a metadata handler which provide a set of useful
    functions to handle and check for the integrity of the data

    Internally it handle every partition using another handler, so this can be seen as a handler of handlers


    Attributes
    ----------
    metadata: Handle the metadata file
    default_value: Indicate the value in case of missing data for all the partitions (probably will be in other part)
    max_cached_data: Indicate the max number of appends that can be preserved in memory before being writed
        to the file
    core_handler: Internal handle for every partition, this control how the writes/reads/append happen
    dataset: It's an xarray Dataset which handle all the partitions as a unique array, so this is used to read
    dims_conversion: This will be deleted in the future
    modified_partitions: Save the names of all the modified partitions, this is helpful to know what partitions
        needs a backup

    Methods
    -------

    """
    def __init__(self,
                 dims_conversion: Dict[str, str] = None,
                 default_value: Any = np.nan,
                 max_cached_data: int = 0,
                 core_handler: Union[Callable[[BaseCoreHandler], BaseCoreHandler], str] = None,
                 *args,
                 **kwargs):

        super().__init__(*args, **kwargs)
        self.metadata = MetadataHandler(*args, **kwargs)
        self.default_value = default_value
        self._cached_data = []
        self.modified_partitions = set()
        self._last_partition = None
        self._last_partition_name = None
        self.dataset: xarray.Dataset = None
        self.dims_conversion = dims_conversion
        self.max_cached_data = max_cached_data
        self.core_handler = core_handler
        if self.core_handler is None:
            self.core_handler = CoreNetcdfHandler
        elif isinstance(self.core_handler, str):
            raise NotImplemented("Using an string to select the core handler is not supported")

        self.count_writes = 0

    @property
    def index(self) -> xarray.DataArray:
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
            default_free_values=self.dims_handler.default_free_values,
            first_write=first_write,
            *args,
            **kwargs
        )

    def append_data(self, new_data: xarray.DataArray, force_write: bool = False, *args, **kwargs):
        self.close_dataset()

        if new_data is not None:
            self._cached_data.append(new_data)

        # TODO: Improve this condition
        if (
                force_write or
                len(self._cached_data) >= self.max_cached_data or
                (
                    not self.metadata.empty and
                    self.last_partition.dims_handler.free_sizes[self.dims_handler.concat_dim] == len(self._cached_data)
                )
        ):
            new_data = xarray.concat(self._cached_data, dim=self.dims_handler.concat_dim)
            self.count_writes += 1
            if self.metadata.empty or self.last_partition.dims_handler.is_complete():
                self.write_new_partition(new_data)
            else:
                self.last_partition.append_data(new_data)
                self.metadata.append_index(new_data.coords['index'].values, *args, **kwargs)
                self.modified_partitions.add(self.last_partition.name)

            self._cached_data = []

    def write_new_partition(self, new_data: xarray.DataArray, *args, **kwargs):
        self.close_dataset()
        new_partition = self.get_core_partition(
            path=os.path.join(self.base_path, str(new_data.coords[self.dims_handler.concat_dim].values[0]) + '.nc'),
            first_write=True,
            *args,
            **kwargs
        )
        new_partition.write_file(new_data)
        self.metadata.concat_new_partition(
            new_partition.dims_handler.used_coords[self.dims_handler.concat_dim],
            new_partition,
            *args,
            **kwargs
        )
        self.modified_partitions.add(new_partition.name)

    def update_data(self, new_data: xarray.DataArray, *args, **kwargs):
        self.close_dataset()

        partitions_index_groups = new_data.groupby(self.index.sel(columns='partition_pos'))

        for partition_pos, act_data in partitions_index_groups:
            partition_path = self.metadata.get_partition_path(partition_pos)
            partition = self.get_core_partition(partition_path, *args, **kwargs)
            partition.update_data(act_data)
            self.modified_partitions.add(partition.name)
            self.metadata.partitions_metadata[partition.name].update(**kwargs)

    def store_data(self, new_data: xarray.DataArray, *args, **kwargs):
        self.close_dataset()

        chunk_size = self.dims_handler.dims_space[self.dims_handler.concat_dim]
        for i in range(0, new_data.sizes[self.dims_handler.concat_dim], chunk_size):
            act_data = new_data.isel(**{
                self.dims_handler.concat_dim:
                    list(range(i, min(i + chunk_size, new_data.sizes[self.dims_handler.concat_dim])))
            })
            self.write_new_partition(act_data, *args, **kwargs)

    def get_dataset(self) -> xarray.Dataset:
        if self.dataset is not None:
            return self.dataset

        self.dataset = xarray.open_mfdataset(
            self.metadata.get_partition_paths(),
            concat_dim=['index'],
            combine='nested'
        )

        # filter the free data
        self.dataset = self.dataset.isel(
            {
                dim: self.dims_handler.get_positions_coord(
                    coord.values,
                    np.array([v for v in coord.values if not self.dims_handler.is_free(v, dim)])
                )
                for dim, coord in self.dataset.coords.items()
            }
        )

        return self.dataset

    def save(self):
        if len(self._cached_data):
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
