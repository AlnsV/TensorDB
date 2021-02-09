import xarray
import numpy as np
import netCDF4
import os

from loguru import logger
from typing import Dict, List

from store_core.netcdf_handler.metadata_handler import MetadataHandler
from store_core.netcdf_handler.core_handler import CoreNetcdfHandler
from config_root_dir import TEST_DIR_METADATA


def get_default_metadata(first_write):
    return MetadataHandler(
        path=TEST_DIR_METADATA,
        first_write=first_write,
        attribute_1='test_1',
        attribute_2={'index': 'ey'},
        attribute_3=['1', '2'],
    )


def get_default_handler(path, first_write):
    return CoreNetcdfHandler(
        path=path,
        dims=['index', 'columns'],
        dims_type={'index': 'fixed', 'columns': 'percentage'},
        dims_space={'index': 10, 'columns': 0.4},
        first_write=first_write
    )


class TestMetadataHandler:

    def test_concat_new_partition(self):
        metadata_handler = get_default_metadata(True)

        core_handler = get_default_handler(
            os.path.join(metadata_handler.path,  '0.nc'),
            True
        )
        metadata_handler.concat_new_partition(
            np.array(['0', '1', '2', '3']),
            core_handler
        )
        partition_paths = metadata_handler.get_partition_paths()
        metadata_handler.close()
        index = xarray.open_dataarray(metadata_handler.metadata_path, group='index')
        partition_names = xarray.open_dataarray(metadata_handler.metadata_path, group='partition_names')

        assert np.all(index.loc[:, 'partition_pos'].values == 0)
        assert np.all(index.coords['index'].values == np.array(['0', '1', '2', '3']))
        assert np.all(index.loc[:, 'internal_partition_pos'].values == np.array([0, 1, 2, 3]))
        assert np.all(np.array(partition_paths) == core_handler.path)
        assert np.all(partition_names.loc[:, 'partition_names'].values == core_handler.name)
        assert np.all(partition_names.coords['index'].values == core_handler.name)

    def test_append_data(self):
        self.test_concat_new_partition()
        metadata_handler = get_default_metadata(False)

        metadata_handler.append_row_index(
            np.array(['4', '5', '6', '7']),
            4
        )
        partition_paths = metadata_handler.get_partition_paths()
        metadata_handler.close()
        index = xarray.open_dataarray(metadata_handler.metadata_path, group='index')
        partition_names = xarray.open_dataarray(metadata_handler.metadata_path, group='partition_names')

        assert np.all(index.loc[:, 'partition_pos'].values == 0)
        assert np.all(index.coords['index'].values == np.array(['0', '1', '2', '3', '4', '5', '6', '7']))
        assert np.all(index.loc[:, 'internal_partition_pos'].values ==
                      np.array([0, 1, 2, 3, 4, 5, 6, 7]))

        assert np.all(partition_names.loc[:, 'partition_names'].values == '0.nc')
        assert np.all(partition_names.coords['index'].values == '0.nc')
        assert np.all(np.array(partition_paths) == os.path.join(metadata_handler.path,  '0.nc'))

    def test_get_attributes(self):
        self.test_append_data()
        metadata_handler = get_default_metadata(False)
        metadata = metadata_handler.get_metadata()

        assert metadata['attribute_1'] == 'test_1'
        assert metadata['attribute_2'] == {'index': 'ey'}
        assert metadata['attribute_3'] == ['1', '2']


if __name__ == "__main__":
    test = TestMetadataHandler()
    # test.test_concat_new_partition()
    test.test_append_data()
    # test.test_get_attributes()




