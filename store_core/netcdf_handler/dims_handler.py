import xarray
import numpy as np
import pandas as pd
import netCDF4

from typing import Dict, List, Any, Union
from pandas.api.types import is_numeric_dtype
from loguru import logger


class DtypeFreeDimsHandler:
    def __init__(self, free_value: Any):
        self.free_value = free_value
        if isinstance(free_value, DtypeFreeDimsHandler):
            self.free_value = free_value.free_value

    def __add__(self, increment: Any):
        if isinstance(self.free_value, pd.Timestamp):
            return self.free_value + pd.DateOffset(increment)
        if isinstance(self.free_value, str):
            return self.free_value + str(increment)
        return self.free_value + increment

    def is_free(self, other: Any):
        return self.free_value <= other

    def __str__(self):
        return f"{self.free_value}"

    def __repr__(self):
        return self.__str__()


class DimsHandler:
    def __init__(self,
                 coords: Dict[str, np.ndarray],
                 dims: List[str],
                 dims_type: Dict[str, str],
                 default_free_values: Dict[str, Any] = None,
                 dims_space: Dict[str, Union[float, int]] = None,
                 concat_dim: str = 'index',
                 *args,
                 **kwargs):

        self._dims = dims
        self._coords = {dim: coords.get(dim, np.array([])) for dim in dims}
        self._dims_space = {} if dims_space is None else dims_space
        self._dims_type = {} if dims_type is None else dims_type
        self._default_free_values: Dict[str, DtypeFreeDimsHandler] = {}
        if default_free_values is not None:
            self._default_free_values = {
                k: DtypeFreeDimsHandler(val)
                for k, val in default_free_values.items()
            }

        self._is_static = np.all([self.is_dim_static(dim) for dim in self._dims])
        self._free_sizes = {}
        self._sizes = {}
        self._concat_dim = concat_dim
        if self._coords:
            self._sizes = {dim: len(coord) for dim, coord in self._coords.items()}
            self._update_free_sizes(self._dims)

    @property
    def coords(self):
        return self._coords

    @property
    def free_sizes(self):
        return self._free_sizes

    @property
    def sizes(self):
        return self._sizes

    @property
    def dims(self):
        return self._dims

    @property
    def dims_space(self):
        return self._dims_space

    @property
    def dims_type(self):
        return self._dims_type

    @property
    def is_static(self):
        return self._is_static

    @property
    def concat_dim(self):
        return self._concat_dim

    @property
    def default_free_values(self):
        return self._default_free_values

    @property
    def used_coords(self):
        return {
            dim: np.array([v for v in self._coords[dim] if not self.is_free(v, dim)])
            for dim in self._dims
        }

    def is_free(self, element: Any, dim: str):
        if dim not in self._default_free_values:
            return False
        return self._default_free_values[dim].is_free(element)

    def create_free_value(self, increment: Any, dim: str):
        return self._default_free_values[dim] + increment

    def _update_free_sizes(self, dims: Union[str, List[str]]):
        dims = dims
        if isinstance(dims, str):
            dims = [dims]

        for dim in dims:
            self._free_sizes[dim] = np.inf
            if self.is_dim_static(dim):
                self._free_sizes[dim] = sum(self.is_free(val, dim) for val in self._coords[dim])

    def _concat_percentage_unique_free_coord(self, dim: str) -> np.array:
        coord = self._coords[dim][[not self.is_free(v, dim) for v in self._coords[dim]]]
        free_space = [
            self.create_free_value(i, dim)
            for i in range(int(len(coord) * self._dims_space[dim]))
        ]
        return np.concatenate([coord, free_space])

    def _concat_percentage_nonunique_free_coord(self, dim: str) -> np.array:
        coord = self._coords[dim][[not self.is_free(v, dim) for v in self._coords[dim]]]
        free_space = [
            self.create_free_value(0, dim)
            for i in range(int(len(coord) * self._dims_space[dim]))
        ]
        return np.concatenate([coord, free_space])

    def _concat_fixed_unique_free_coord(self, dim: str) -> np.array:
        free_space = [
            self.create_free_value(i, dim)
            for i in range(self._dims_space[dim] - len(self._coords[dim]))
        ]
        return np.concatenate([self._coords[dim], free_space])

    def _concat_fixed_nonunique_free_coord(self, dim: str) -> np.array:
        free_space = [
            self.create_free_value(0, dim)
            for _ in range(self._dims_space[dim] - len(self._coords[dim]))
        ]
        return np.concatenate([self._coords[dim], free_space])

    def concat_free_coords(self):
        for dim in self._dims:
            if self._dims_type[dim] == 'simple':
                continue
            elif self._dims_type[dim] == 'fixed':
                self._coords[dim] = self._concat_fixed_nonunique_free_coord(dim)
            elif self._dims_type[dim] == 'fixed_unique':
                self._coords[dim] = self._concat_fixed_unique_free_coord(dim)
            elif self._dims_type[dim] == 'percentage':
                self._coords[dim] = self._concat_percentage_nonunique_free_coord(dim)
            elif self._dims_type[dim] == 'percentage_unique':
                self._coords[dim] = self._concat_percentage_unique_free_coord(dim)
            else:
                raise NotImplemented(f"The option {self._dims_type[dim]} is not implemented")

            self._sizes[dim] = len(self._coords[dim])
            self._update_free_sizes(dim)

    def filter_free_coords(self, only_free: set):
        coords = {}
        for dim in self._dims:
            if dim in only_free:
                coords[dim] = np.array([v for v in self._coords[dim] if self.is_free(v, dim)])
            else:
                coords[dim] = self._coords[dim]

        return coords

    def is_dim_static(self, dim):
        return self.is_dim_fixed(dim) or self.is_dim_percentage(dim)

    def is_dim_fixed(self, dim):
        return 'fixed' in self._dims_type[dim]

    def is_dim_percentage(self, dim):
        return 'percentage' in self._dims_type[dim]

    def is_appendable(self, new_coords) -> bool:
        for dim in self._dims:
            if dim not in new_coords:
                continue

            if self.is_dim_fixed(dim) and len(new_coords[dim]) > self.free_sizes[dim]:
                raise OverflowError(
                    f"You are appending {len(new_coords[dim])} new elements "
                    f"to the fixed dimension {dim} and there are only {self.free_sizes[dim]} "
                    f"free positions"
                )

            if self.is_dim_percentage(dim) and len(new_coords[dim]) > self.free_sizes[dim]:
                return False

        return True

    def is_complete(self):
        for dim in self.dims:
            if self.is_dim_fixed(dim) and self.free_sizes[dim] > 0:
                return False
        return True

    @staticmethod
    def get_positions_from_unsorted(x, y):
        x_sorted = np.argsort(x)
        y_pos = np.searchsorted(x[x_sorted], y)
        indices = x_sorted[y_pos]
        return indices

    def get_positions_coords(self, coords) -> Dict[str, np.array]:
        coords_pos = {}
        for dim in self._dims:
            coord = coords[dim]

            if isinstance(coord, xarray.DataArray):
                coord = coord.values

            coords_pos[dim] = DimsHandler.get_positions_coord(self._coords[dim], coord)

        return coords_pos

    @staticmethod
    def get_positions_coord(x, y):
        if np.all(x[:-1] <= x[1:]):
            return np.searchsorted(x, y)
        return DimsHandler.get_positions_from_unsorted(x, y)

    def get_new_coords(self, coords_to_append: Union[Dict[str, np.array]]):
        new_coords = {}
        for dim in self._dims:
            coord_to_append = coords_to_append[dim]
            if isinstance(coord_to_append, xarray.DataArray):
                coord_to_append = coord_to_append.values

            new_coord = coord_to_append[~np.isin(coord_to_append, self._coords[dim], assume_unique=True)]
            if len(new_coord):
                new_coords[dim] = new_coord

        return new_coords

    def get_positions_to_update(self, new_coord, dim):
        coord = self._coords[dim]
        if self._is_static:
            positions = np.where([self.is_free(v, dim) for v in coord])[0][:len(new_coord)]
        else:
            positions = np.array(list(range(len(new_coord)))) + len(coord)

        return positions

    def get_dims(self, type_dim: str):
        return [dim for dim in self._dims if self._dims_type[dim] == type_dim]

    def update_coords(self, new_coords):
        for dim, coord in new_coords.items():
            self._coords[dim] = coord
            if isinstance(coord, xarray.DataArray):
                self._coords[dim] = coord.values

            self._update_free_sizes(dim)
            self._sizes[dim] = len(self._coords[dim])

