from loguru import logger

from store_core.data_handler.files_store import FilesStore
from store_core.netcdf_handler.partitions_handler import PartitionsStore
from store_core.zarr_handler.zarr_store import ZarrStore
from store_core.utils import create_dummy_array, compare_dataset
from config_path.config_root_dir import TEST_DIR_FILE_STORE


def get_default_file_store():
    default_settings = {
        'dims': ['index', 'columns'],
        'dims_type': {'index': 'fixed', 'columns': 'percentage'},
        'dims_space': {'index': 5, 'columns': 0.1},
        'default_free_values': {'index': 'free', 'columns': 'free'},
        'metadata_file_name': 'metadata.nc',
        'bucket_name': 'test.bitacore.data.2.0',
        'data_handler': ZarrStore,
    }
    first_data = default_settings.copy()
    # first_data['path'] = os.path.join(TEST_DIR_FILE_STORE, 'data_one')
    second_data = default_settings.copy()
    # second_data['path'] = os.path.join(TEST_DIR_FILE_STORE, 'data_two')

    files_settings = {
        'data_one': first_data,
        'data_two': second_data
    }

    return FilesStore(
        base_path=TEST_DIR_FILE_STORE,
        files_settings=files_settings,
        use_env=False,
        s3_handler={
            'aws_access_key_id': "AKIAV5EJ3JJSZ5JQTD3K",
            'aws_secret_access_key': "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",
            'region_name': 'us-east-2',
        }
    )


class TestFileStore:
    def test_store_data(self):
        file_store = get_default_file_store()
        arr = create_dummy_array(10, 10)
        file_store.store_data(new_data=arr, file_setting_id='data_one', path='data_one')
        assert compare_dataset(file_store.get_dataset(file_setting_id='data_one', path='data_one'), arr)

        arr = create_dummy_array(10, 10)
        file_store.store_data(new_data=arr, file_setting_id='data_two', path='data_two')
        assert compare_dataset(file_store.get_dataset(file_setting_id='data_two', path='data_two'), arr)

        # file_store.close()

    def test_update_data(self):
        self.test_store_data()
        file_store = get_default_file_store()
        arr = create_dummy_array(10, 10)
        file_store.update_data(new_data=arr, file_setting_id='data_one', path='data_one')
        assert compare_dataset(file_store.get_dataset(file_setting_id='data_one', path='data_one'), arr)
        # file_store.close()

    def test_append_data(self):
        self.test_store_data()
        file_store = get_default_file_store()

        arr = create_dummy_array(20, 10)
        arr = arr.sel(
            index=(
                ~arr.coords['index'].isin(
                    file_store.get_dataset(
                        file_setting_id='data_one',
                        path='data_one'
                    ).coords['index']
                )
            )
        )

        for i in range(arr.sizes['index']):
            file_store.append_data(new_data=arr.isel(index=[i]), file_setting_id='data_one', path='data_one')

        assert compare_dataset(file_store.get_dataset(file_setting_id='data_one', path='data_one').sel(arr.coords), arr)
        # file_store.close()

    def test_backup(self):
        file_store = get_default_file_store()
        arr = create_dummy_array(3, 3)
        file_store.store_data(new_data=arr, file_setting_id='data_one', path='data_one')
        handler = file_store.get_handler(file_setting_id='data_one', path='data_one')

        assert handler.check_modification
        handler.backup()
        handler.update_from_backup()

        assert handler.s3_handler is not None
        assert compare_dataset(file_store.get_dataset(file_setting_id='data_one', path='data_one').sel(arr.coords), arr)
        logger.info(handler.get_dataset())


if __name__ == "__main__":
    test = TestFileStore()
    # test.test_store_data()
    # test.test_update_data()
    # test.test_append_data()
    test.test_backup()

