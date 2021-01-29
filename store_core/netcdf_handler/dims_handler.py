import xarray
import numpy as np
import netCDF4

from typing import Dict, List, Any, Union
from pandas.api.types import is_numeric_dtype
from loguru import logger


def get_positions_from_unsorted(x, y):
    x_sorted = np.argsort(x)
    y_pos = np.searchsorted(x[x_sorted], y)
    indices = x_sorted[y_pos]
    return indices


class DimsHandler:
    def __init__(self,
                 coords: Dict[str, np.ndarray],
                 dims: List[str],
                 dims_type: Dict[str, str],
                 dims_space: Dict[str, Union[float, int]] = None,
                 default_free_value: Any = None,
                 concat_dim: str = 'index'):

        self._dims = dims
        self._coords = {dim: coords.get(dim, np.array([])) for dim in dims}
        self._dims_space = {} if dims_space is None else dims_space
        self._dims_type = {} if dims_type is None else dims_type
        self._default_free_value = default_free_value
        if self._default_free_value is not None and not isinstance(self._default_free_value, dict):
            self._default_free_value = {dim: self._default_free_value for dim in self._dims}

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
    def default_free_value(self):
        return self._default_free_value

    @property
    def used_coords(self):
        return {
            dim: np.array([v for v in self._coords[dim] if not self.is_free(v, dim)])
            for dim in self._dims
        }

    def is_free(self, element: Any, dim: str):
        if self._default_free_value is None:
            return False

        if isinstance(element, str):
            return self._default_free_value[dim] in element

        return self._default_free_value[dim] == element

    def create_free_value(self, element: Any, dim: str):
        if self._default_free_value is None:
            return element

        if isinstance(element, str):
            return self._default_free_value[dim] + element

        return self.default_free_value

    def _update_free_sizes(self, dims: Union[str, List[str]]):
        dims = dims
        if isinstance(dims, str):
            dims = [dims]

        for dim in dims:
            self._free_sizes[dim] = np.inf
            if self.is_dim_static(dim):
                self._free_sizes[dim] = sum(self.is_free(val, dim) for val in self._coords[dim])

    def _concat_fixed_free_coord(self, dim: str) -> np.array:
        coord = self._coords[dim]
        free_space = [self.create_free_value(str(i), dim) for i in range(self._dims_space[dim] - len(coord))]
        return np.concatenate([coord, free_space])

    def _concat_percentage_free_coord(self, dim: str) -> np.array:
        coord = self._coords[dim][[not self.is_free(v, dim) for v in self._coords[dim]]]
        free_space = [self.create_free_value(str(i), dim) for i in range(int(len(coord) * self._dims_space[dim]))]
        return np.concatenate([coord, free_space])

    def concat_free_coords(self):
        for dim in self._dims:
            if self._dims_type[dim] == 'fixed':
                self._coords[dim] = self._concat_fixed_free_coord(dim)
            elif self._dims_type[dim] == 'percentage':
                self._coords[dim] = self._concat_percentage_free_coord(dim)

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
        return self._dims_type[dim] in ['fixed', 'percentage']

    def is_appendable(self, new_coords) -> bool:
        for dim in self._dims:
            if dim not in new_coords:
                continue

            if self._dims_type[dim] == 'fixed' and len(new_coords[dim]) > self.free_sizes[dim]:
                raise OverflowError(f"You are appending {len(new_coords[dim])} new elements "
                                    f"to the fixed dimension {dim} and there are only {self.free_sizes[dim]} "
                                    f"free positions")

            if self._dims_type[dim] == 'percentage' and len(new_coords[dim]) > self.free_sizes[dim]:
                return False

        return True

    def is_complete(self):
        for dim in self.dims:
            if self._dims_type[dim] == 'fixed' and self.free_sizes[dim] > 0:
                return False
        return True

    def get_positions_coords(self, coords) -> Dict[str, np.array]:
        coords_pos = {}
        for dim in self._dims:
            coord = coords[dim]

            if isinstance(coord, xarray.DataArray):
                coord = coord.values

            if np.all(self._coords[dim][:-1] <= self._coords[dim][1:]):
                coords_pos[dim] = np.searchsorted(self._coords[dim], coord)
            else:
                coords_pos[dim] = get_positions_from_unsorted(self._coords[dim], coord)

        return coords_pos

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


