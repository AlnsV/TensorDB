from store_core.data_handler.files_store import FilesStore
from store_core.netcdf_handler.partitions_handler import PartitionsStore
from store_core.utils import create_dummy_array, compare_dataset
from config_path.config_root_dir import TEST_DIR_FILE_STORE


def get_default_file_store():
    default_settings = {
        'dims': ['index', 'columns'],
        'dims_type': {'index': 'fixed', 'columns': 'percentage'},
        'dims_space': {'index': 5, 'columns': 0.1},
        'default_free_value': "free"
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
        data_handler=PartitionsStore,
        use_env=False
    )


class TestFileStore:
    def test_store_data(self):
        file_store = get_default_file_store()
        arr = create_dummy_array(10, 10)
        file_store.store_data(arr, 'data_one')
        assert compare_dataset(file_store.get_dataset('data_one'), arr)

        arr2 = create_dummy_array(10, 10)
        file_store.store_data(arr2, 'data_two')
        assert compare_dataset(file_store.get_dataset('data_two'), arr2)

        file_store.close()

    def test_update_data(self):
        self.test_store_data()
        file_store = get_default_file_store()
        arr = create_dummy_array(10, 10)
        file_store.update_data(arr, 'data_one')
        assert compare_dataset(file_store.get_dataset('data_one'), arr)
        file_store.close()

    def test_append_data(self):
        self.test_store_data()
        file_store = get_default_file_store()

        arr = create_dummy_array(20, 10)
        arr = arr.sel(index=(~arr.coords['index'].isin(file_store.get_dataset('data_one').coords['index'])))

        for i in range(arr.sizes['index']):
            file_store.append_data(arr.isel(index=i), 'data_one')

        assert compare_dataset(file_store.get_dataset('data_one').sel(arr.coords), arr)
        file_store.close()


if __name__ == "__main__":
    test = TestFileStore()
    # test.test_store_data()
    test.test_update_data()
    # test.test_append_data()


