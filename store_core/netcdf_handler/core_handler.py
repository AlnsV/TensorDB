import xarray
import numpy as np
import netCDF4
import os

from typing import Dict, List, Any, Union
from abc import abstractmethod
from loguru import logger

from .dims_handler import DimsHandler, get_positions_from_unsorted


class BaseCoreHandler:

    def __init__(self,
                 path: str,
                 dims: List[str],
                 dims_type: Dict[str, str],
                 dims_space: Dict[str, Union[float, int]],
                 default_free_value: Any = None,
                 concat_dim: str = "index",
                 group: str = None,
                 first_write: bool = False,
                 default_value: Any = np.nan,
                 **kwargs):

        self.path = path
        self.group = group
        self.name = os.path.basename(self.path)
        self.default_value = default_value
        self.dims_handler = DimsHandler(
            coords=self.get_computed_coords(dims) if not first_write else {},
            dims=dims,
            dims_space=dims_space,
            dims_type=dims_type,
            default_free_value=default_free_value,
            concat_dim=concat_dim
        )

        self.__dict__.update(kwargs)

    @abstractmethod
    def update_data(self, data: xarray.DataArray):
        pass

    @abstractmethod
    def append_data(self, data: xarray.DataArray):
        pass

    @abstractmethod
    def write_file(self, data: xarray.DataArray):
        pass

    @abstractmethod
    def write_as_new_group(self, data: xarray.DataArray):
        pass

    def set_attributes(self, attributes: Dict[str, Any]):
        dataset = netCDF4.Dataset(self.path, mode='a', format="NETCDF4")
        dataset_group = dataset.groups.get(self.group, dataset)
        dataset_group.setncatts(attributes)
        dataset.close()

    def get_attributes(self) -> Dict[str, Any]:
        dataset = netCDF4.Dataset(self.path, mode='r', format="NETCDF4")
        dataset_group = dataset.groups.get(self.group, dataset)
        d = dataset_group.__dict__.copy()
        dataset.close()
        return d

    def get_computed_array(
            self,
    ) -> Union[xarray.DataArray, Dict[str, Union[xarray.DataArray, np.array]]]:

        data = xarray.open_dataarray(self.path, group=self.group)
        data_computed = data.compute()
        data.close()
        return data_computed

    def get_computed_coords(self, coords_names: List[str] = None) -> Dict[str, np.array]:
        data = xarray.open_dataarray(self.path, group=self.group)
        coords_names = coords_names
        if coords_names is None:
            coords_names = self.dims_handler.dims
        coords = {dim: data.coords[dim].values for dim in coords_names}
        data.close()
        return coords

    def get_descriptors(self):
        descriptors = {
            'dims': self.dims_handler.dims,
            'dims_type': self.dims_handler.dims_type,
            'dims_space': self.dims_handler.dims_space,
            'default_free_value': self.dims_handler.default_free_value,
            'concat_dim': self.dims_handler.concat_dim,
        }

        for key, val in self.__dict__.items():
            if key == 'dims_handler' or key == 'mode':
                continue
            descriptors[key] = val

        return descriptors

    @staticmethod
    def get_external_computed_array(path: str, group: str = None) -> xarray.DataArray:
        data = xarray.open_dataarray(path, group=group)
        data_computed = data.compute()
        data.close()
        return data_computed

    @staticmethod
    def coords_to_string(data: xarray.DataArray):
        return data.assign_coords({key: coord.astype(str) for key, coord in data.coords.items()})


class CoreNetcdfHandler(BaseCoreHandler):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def write_file(self, new_data: xarray.DataArray):
        self._write_file(new_data, 'w')

    def write_as_new_group(self, new_data: xarray.DataArray):
        self._write_file(new_data, 'a')

    def _write_file(self, new_data, mode):
        new_data = CoreNetcdfHandler.coords_to_string(new_data)

        # TODO: Add a validation in case that the data is bigger than the fixed dimensions

        # update the coords
        self.dims_handler.update_coords(new_data.coords)

        if not self._is_data_complete(new_data):
            new_data = self._concat_extra_space(new_data)

        new_data.to_netcdf(self.path,
                           mode=mode,
                           group=self.group,
                           unlimited_dims=self.dims_handler.get_dims('dynamic'))

    def _concat_extra_space(self, data):
        self.dims_handler.concat_free_coords()

        data = xarray.concat(
            [
                data,
                xarray.DataArray(
                    self.default_value,
                    dims=self.dims_handler.dims,
                    coords=self.dims_handler.filter_free_coords({self.dims_handler.concat_dim})
                )
            ],
            dim=self.dims_handler.concat_dim
        )

        return data

    def append_data(self, new_data: xarray.DataArray):
        dataset = netCDF4.Dataset(self.path, mode='a', format="NETCDF4")
        if self.group is not None and self.group not in dataset.groups:
            dataset.close()
            self.write_as_new_group(new_data)
            return

        new_data = CoreNetcdfHandler.coords_to_string(new_data)

        new_coords = self.dims_handler.get_new_coords(new_data.coords)

        if self.dims_handler.concat_dim not in new_coords:
            dataset.close()
            return

        new_data = new_data.sel(**{self.dims_handler.concat_dim: new_coords[self.dims_handler.concat_dim]})

        if not self.dims_handler.is_appendable(new_coords):
            dataset.close()
            data = self.get_computed_array()
            data = data.sel(**self.dims_handler.used_coords)
            new_data = xarray.concat([data, new_data], dim=self.dims_handler.concat_dim)
            self.write_file(new_data)
            return

        dataset_group = dataset.groups.get(self.group, dataset)

        self._append_coords(dataset_group, new_coords)
        self._update_file(dataset_group, new_data)

        dataset.close()

    def _append_coords(self, dataset: netCDF4.Dataset, new_coords: Dict[str, xarray.DataArray]):
        if new_coords is None:
            return

        for dim, coord in new_coords.items():
            positions = self.dims_handler.get_positions_to_update(coord, dim)
            for i, pos in enumerate(positions):
                dataset.variables[dim][pos] = coord[i]
            self.dims_handler.update_coords({dim: dataset.variables[dim][:]})

    def _is_data_complete(self, data: xarray.DataArray):
        for dim in self.dims_handler.dims:
            if self.dims_handler.dims_type[dim] == 'fixed' and data.sizes[dim] < self.dims_handler.dims_space[dim]:
                return False
        return True

    def update_data(self, new_data: xarray.DataArray):
        dataset = netCDF4.Dataset(self.path, mode='a', format="NETCDF4")
        dataset_group = dataset.groups.get(self.group, dataset)
        self._update_file(dataset_group, CoreNetcdfHandler.coords_to_string(new_data))
        dataset.close()

    def _update_file(self,
                     dataset: netCDF4.Dataset,
                     new_data: xarray.DataArray):

        coords_pos = self.dims_handler.get_positions_coords(new_data.coords)
        total_positions = tuple(coords_pos[dim] for dim in self.dims_handler.dims)
        dataset.variables['__xarray_dataarray_variable__'][total_positions] = new_data.values


class CoreSimpleHandler(CoreNetcdfHandler):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def write_file(self, data: xarray.DataArray):
        data.to_netcdf(self.path, mode='w', group=self.group, unlimited_dims=self.dims_handler.get_dims('dynamic'))

    def write_as_new_group(self, data: xarray.DataArray):
        data.to_netcdf(self.path, mode='a', group=self.group, unlimited_dims=self.dims_handler.get_dims('dynamic'))

    def append_data(self, new_data: xarray.DataArray):
        data = self.get_computed_array()
        data = xarray.concat([data, new_data], dim=self.dims_handler.concat_dim)
        data.to_netcdf(self.path,
                       mode='w',
                       group=self.group,
                       unlimited_dims=self.dims_handler.get_dims('dynamic'))

    def update_data(self, data: xarray.DataArray):
        dataset = netCDF4.Dataset(self.path, mode='a', format="NETCDF4")
        dataset_group = dataset.groups.get(self.group, dataset)
        self._update_file(dataset_group, data)
        dataset.close()

    def _update_file(self,
                     dataset: netCDF4.Dataset,
                     data: xarray.DataArray):
        coords = self.get_computed_coords()
        total_positions = tuple(
            get_positions_from_unsorted(coord, data.coords[dim].values)
            for dim, coord in coords.items()
        )
        dataset.variables['__xarray_dataarray_variable__'][total_positions] = data
