import xarray
import numpy as np
import os
import pandas as pd

from loguru import logger

from store_core.netcdf_handler.metadata_handler import MetadataHandler
from store_core.netcdf_handler.core_handler import CoreNetcdfHandler
from config_path.config_root_dir import TEST_DIR_METADATA


def get_default_metadata(first_write):
    return MetadataHandler(
        base_path=TEST_DIR_METADATA,
        first_write=first_write,
        attribute_1='test_1',
        attribute_2={'index': 'ey'},
        attribute_3=['1', '2'],
        attribute_4={
            '10': {
                '100': pd.Timestamp.now().strftime('%Y-%m-%d'),
                '200': {
                    '300': pd.Timestamp.now().strftime('%Y-%m-%d')
                }
            }
        },
        metadata_file_name='metadata.nc',
    )


def get_default_handler(path, first_write):
    return CoreNetcdfHandler(
        path=path,
        dims=['index', 'columns'],
        dims_type={'index': 'fixed', 'columns': 'percentage'},
        dims_space={'index': 10, 'columns': 0.4},
        first_write=first_write,
        default_free_values={'index': 'free', 'columns': 'free'}
    )


class TestMetadataHandler:

    def test_concat_new_partition(self):
        metadata_handler = get_default_metadata(True)

        core_handler = get_default_handler(
            os.path.join(metadata_handler.base_path,  '0.nc'),
            True
        )
        metadata_handler.concat_new_partition(
            np.array(['0', '1', '2', '3']),
            core_handler
        )
        partition_paths = metadata_handler.get_partition_paths()
        metadata_handler.close()
        index = xarray.open_dataarray(metadata_handler.metadata_path, group='index')
        partition_names = metadata_handler.get_attribute('partitions_metadata')
        partition_names = [name for name in partition_names.keys()
                           if name != metadata_handler.metadata_file_name]

        assert np.all(index.loc[:, 'partition_pos'].values == 0)
        assert np.all(index.coords['index'].values == np.array(['0', '1', '2', '3']))
        assert np.all(index.loc[:, 'internal_partition_pos'].values == np.array([0, 1, 2, 3]))
        assert np.all(np.array(partition_paths) == core_handler.path)
        assert np.all(np.array(partition_names) == core_handler.name)

    def test_append_data(self):
        self.test_concat_new_partition()
        metadata_handler = get_default_metadata(False)

        metadata_handler.append_index(
            np.array(['4', '5', '6', '7']),
        )
        partition_paths = metadata_handler.get_partition_paths()
        metadata_handler.close()
        index = xarray.open_dataarray(metadata_handler.metadata_path, group='index')
        partition_names = metadata_handler.get_attribute('partitions_metadata')
        partition_names = [name for name in partition_names.keys()
                           if name != metadata_handler.metadata_file_name]

        assert np.all(index.loc[:, 'partition_pos'].values == 0)
        assert np.all(index.coords['index'].values == np.array(['0', '1', '2', '3', '4', '5', '6', '7']))
        assert np.all(index.loc[:, 'internal_partition_pos'].values ==
                      np.array([0, 1, 2, 3, 4, 5, 6, 7]))

        assert np.all(np.array(partition_names) == '0.nc')
        assert np.all(np.array(partition_paths) == os.path.join(metadata_handler.base_path,  '0.nc'))

    def test_get_attributes(self):
        self.test_append_data()
        logger.info("fsadfa")
        metadata_handler = get_default_metadata(False)
        metadata = metadata_handler.get_all_attributes()
        logger.info(metadata)

        assert metadata['attribute_1'] == 'test_1'
        assert metadata['attribute_2'] == {'index': 'ey'}
        assert metadata['attribute_3'] == ['1', '2']
        assert metadata['attribute_4'] == {
            '10': {
                '100': pd.Timestamp.now().strftime('%Y-%m-%d'),
                '200': {
                    '300': pd.Timestamp.now().strftime('%Y-%m-%d')
                }
            }
        }


if __name__ == "__main__":
    test = TestMetadataHandler()
    # test.test_concat_new_partition()
    # test.test_append_data()
    test.test_get_attributes()




