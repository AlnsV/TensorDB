import xarray
import numpy as np
import os
import netCDF4

from typing import Dict

from store_core.netcdf_handler.core_handler import CoreNetcdfHandler
from store_core.utils import create_dummy_array
from config_path.config_root_dir import TEST_DIR_CORE


def get_default_handler(first_write):
    path = os.path.join(TEST_DIR_CORE, 'test.nc')
    return CoreNetcdfHandler(
        path=path,
        dims=['index', 'columns'],
        dims_type={'index': 'fixed', 'columns': 'percentage'},
        dims_space={'index': 10, 'columns': 0.4},
        first_write=first_write,
        default_free_value="free"
    )


class TestCoreHandler:

    @staticmethod
    def generic_test(new_arr,
                     option,
                     expected_free_sizes: Dict[str, int],
                     expected_sizes: Dict[str, int]):

        if option == 'append' or option == 'update':
            handler = get_default_handler(False)
        elif option == 'write':
            handler = get_default_handler(True)
        else:
            raise ValueError(f"The option: {option} is not valid")

        if option == 'write':
            handler.write_file(new_arr)
        elif option == 'append':
            handler.append_data(new_arr)
        else:
            handler.update_data(new_arr)

        arr = xarray.open_dataarray(os.path.join(TEST_DIR_CORE, 'test.nc'))

        free_sizes = handler.dims_handler.free_sizes

        assert len(arr.coords['index']) == expected_sizes['index']
        assert len(arr.coords['columns']) == expected_sizes['columns']
        assert free_sizes['index'] == expected_free_sizes['index']
        assert free_sizes['columns'] == expected_free_sizes['columns']
        assert arr.sel(new_arr.coords).equals(new_arr)

        arr.close()

    def test_write_file(self):
        array = create_dummy_array(5, 5)
        self.generic_test(
            new_arr=array,
            option='write',
            expected_sizes={'index': 10, 'columns': 7},
            expected_free_sizes={'index': 5, 'columns': 2}
        )

    def test_append_data(self):
        self.test_write_file()

        new_arr = create_dummy_array(4, 7, {
            'index': np.sort(np.array(list(map(str, range(5, 9))), dtype='<U15')),
            'columns': np.sort(np.array(list(map(str, range(7))), dtype='<U15'))
        })
        self.generic_test(
            new_arr=new_arr,
            option='append',
            expected_sizes={'index': 10, 'columns': 7},
            expected_free_sizes={'index': 1, 'columns': 0}
        )

    def test_append_with_rewrite(self):
        self.test_write_file()
        new_arr = create_dummy_array(4, 8, {
            'index': np.sort(np.array(list(map(str, range(5, 9))), dtype='<U15')),
            'columns': np.sort(np.array(list(map(str, range(8))), dtype='<U15'))
        })
        self.generic_test(
            new_arr=new_arr,
            option='append',
            expected_sizes={'index': 10, 'columns': 11},
            expected_free_sizes={'index': 1, 'columns': 3}
        )

    def test_append_with_perfect_rewrite(self):
        self.test_write_file()
        new_arr = create_dummy_array(5, 8, {
            'index': np.sort(np.array(list(map(str, range(5, 10))), dtype='<U15')),
            'columns': np.sort(np.array(list(map(str, range(8))), dtype='<U15'))
        })
        self.generic_test(
            new_arr=new_arr,
            option='append',
            expected_sizes={'index': 10, 'columns': 8},
            expected_free_sizes={'index': 0, 'columns': 0}
        )

    def test_update_data(self):
        self.test_write_file()
        new_arr = create_dummy_array(5, 5, {
            'index': np.sort(np.array(list(map(str, range(5))), dtype='<U15')),
            'columns': np.sort(np.array(list(map(str, range(5))), dtype='<U15'))
        })

        self.generic_test(
            new_arr=new_arr,
            option='update',
            expected_sizes={'index': 10, 'columns': 7},
            expected_free_sizes={'index': 5, 'columns': 2}
        )

    def test_attributes(self):
        self.test_write_file()
        handler = get_default_handler('a')

        handler.set_attributes(
            {'attribute_1': list(map(str, range(5))),
             'attribute_2': 0,
             'attribute_3': '3'}
        )
        attributes = handler.get_attributes()
        dataset = netCDF4.Dataset(handler.path, mode='r')

        assert np.all(np.array(dataset.attribute_1) == np.array(list(map(str, range(5)))))
        assert dataset.attribute_2 == 0
        assert dataset.attribute_3 == '3'

        assert np.all(np.array(dataset.attribute_1) == np.array(attributes['attribute_1']))
        assert dataset.attribute_2 == attributes['attribute_2']
        assert dataset.attribute_3 == attributes['attribute_3']


if __name__ == "__main__":
    test = TestCoreHandler()
    test.test_write_file()
    # test.test_append_data()
    # test.test_append_with_rewrite()
    # test.test_append_with_perfect_rewrite()
    # test.test_update_data()
    # test.test_attributes()



