import xarray
import numpy as np

from loguru import logger

from store_core.netcdf_handler.dims_handler import DimsHandler
from store_core.utils import create_dummy_array


def get_default_dims_handler(coords=None):
    if coords is None:
        coords = {'index': np.array(['0', '1', '2', '3']), 'columns': np.array(['5', '6', '7', '8'])}
    return DimsHandler(
        coords=coords,
        dims=['index', 'columns'],
        dims_type={'index': 'fixed', 'columns': 'percentage'},
        dims_space={'index': 5, 'columns': 0.3},
        concat_dim='index',
        default_free_value='free',
    )


class TestDimsHandler:

    def test_concat_free_coords(self):
        dims_handler = get_default_dims_handler()

        dims_handler.concat_free_coords()

        assert dims_handler.free_sizes['index'] == 1
        assert dims_handler.free_sizes['columns'] == 1
        assert dims_handler.sizes['index'] == 5
        assert dims_handler.sizes['columns'] == 5
        assert len(dims_handler.used_coords['index']) == 4
        assert len(dims_handler.used_coords['columns']) == 4

    def test_filter_free_coords(self):
        dims_handler = get_default_dims_handler(
            {'index': np.array(['0', '1', '2', '3', 'free0']), 'columns': np.array(['5', '6', '7', '8', 'free0'])}
        )
        used_coords = dims_handler.filter_free_coords({'index'})
        assert np.all(used_coords['index'] == dims_handler.coords['index'][[4]])
        assert np.all(used_coords['columns'] == dims_handler.coords['columns'])

    def test_dims(self):
        dims_handler = get_default_dims_handler()

        assert dims_handler.is_dim_static('index')
        assert dims_handler.is_dim_static('columns')
        assert dims_handler.is_static
        assert dims_handler.is_complete()

    def test_coords_positions(self):
        dims_handler = get_default_dims_handler(
            {'index': np.array(['0', '1', '2', '3', 'free0']), 'columns': np.array(['0', '1', '2', '3', 'free0'])}
        )
        arr = create_dummy_array(4, 3, coords={
            'index': np.array(['0', '1', '2', '3']),
            'columns': np.array(['2', '0', '3'])
        })

        pos = dims_handler.get_positions_coords(arr.coords)

        logger.info(pos)
        assert np.all(pos['index'] == np.array([0, 1, 2, 3]))
        # this test show that the dims handler work even with no sorted coords.
        # note: use sorted coords is the best option
        assert np.all(pos['columns'] == np.array([2, 0, 3]))

    def test_is_complete(self):
        dims_handler = get_default_dims_handler()
        assert dims_handler.is_complete()

    def test_get_new_coords(self):
        dims_handler = get_default_dims_handler()
        logger.info(dims_handler.coords)
        coords_to_append = {'index': np.array(['0', '10', 'a', '1']), 'columns': np.array(['b', '5', '1'])}
        new_coords = dims_handler.get_new_coords(coords_to_append)

        assert np.all(new_coords['index'] == np.array(['10', 'a']))
        assert np.all(new_coords['columns'] == np.array(['b', '1']))

    def test_get_positions_to_update(self):
        dims_handler = get_default_dims_handler(
            {
                'index': np.array(['0', '1', 'free1', 'free2', '2', '3', 'free0']),
                'columns': np.array(['0', '1', '2', '3', 'free0'])
            }
        )
        coords_to_append = {'index': np.array(['0', '10', 'a', 'c'])}
        pos_to_update = dims_handler.get_positions_to_update(coords_to_append['index'], 'index')

        assert np.all(pos_to_update == np.array([2, 3, 6]))

    def test_update_coords(self):
        dims_handler = get_default_dims_handler()
        coords_to_update = {'index': np.array(['0', '10', 'a', 'c'])}
        dims_handler.update_coords(coords_to_update)
        assert np.all(dims_handler.coords['index'] == coords_to_update['index'])

    def test_get_dims(self):
        dims_handler = get_default_dims_handler()
        assert dims_handler.get_dims('fixed')[0] == 'index'
        assert dims_handler.get_dims('percentage')[0] == 'columns'


if __name__ == "__main__":
    test = TestDimsHandler()
    test.test_concat_free_coords()
    # test.test_filter_free_coords()
    # test.test_dims()
    # test.test_coords_positions()
    # test.test_new_coords()
    # test.test_get_positions_to_update()
    # test.test_update_coords()
    # test.test_get_dims()

