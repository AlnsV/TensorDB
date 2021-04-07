from loguru import logger

from store_core.data_handler.files_store import FilesStore
from store_core.zarr_handler.zarr_store import ZarrStore
from store_core.utils import create_dummy_array, compare_dataset
from config_path.config_root_dir import TEST_DIR_FILES_STORE


def get_default_files_store():
    default_settings = {
        'handler': {
            'dims': ['index', 'columns'],
            'bucket_name': 'test.bitacore.data.2.0',
            'data_handler': ZarrStore,
        },
    }
    evaluate_formula_settings = {
        'read': {
            'method': 'read_from_formula',
            'formula': "(`data_one` * `data_two`).rolling({'index': 3}).sum()",
        }
    }
    files_settings = {
        'data_one': default_settings.copy(),
        'data_two': default_settings.copy(),
        'data_three': evaluate_formula_settings.copy()
    }

    return FilesStore(
        base_path=TEST_DIR_FILES_STORE,
        files_settings=files_settings,
        use_env=False,
        s3_handler={
            'aws_access_key_id': "AKIAV5EJ3JJSZ5JQTD3K",
            'aws_secret_access_key': "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",
            'region_name': 'us-east-2',
        }
    )


class TestFileStore:
    def test_store(self):
        files_store = get_default_files_store()
        arr = create_dummy_array(10, 10)
        files_store.store(new_data=arr, path='data_one')
        assert compare_dataset(files_store.read(path='data_one'), arr)

        arr = create_dummy_array(10, 10)
        files_store.store(new_data=arr, path='data_two')
        assert compare_dataset(files_store.read(path='data_two'), arr)

    def test_update(self):
        self.test_store()
        files_store = get_default_files_store()
        arr = create_dummy_array(10, 10)
        files_store.update(new_data=arr, path='data_one')
        assert compare_dataset(files_store.read(path='data_one'), arr)

    def test_append(self):
        self.test_store()
        files_store = get_default_files_store()

        arr = create_dummy_array(20, 10)
        arr = arr.sel(
            index=(
                ~arr.coords['index'].isin(
                    files_store.read(
                        file_setting_id='data_one',
                        path='data_one'
                    ).coords['index']
                )
            )
        )

        for i in range(arr.sizes['index']):
            files_store.append(new_data=arr.isel(index=[i]), path='data_one')

        assert compare_dataset(files_store.read(path='data_one').sel(arr.coords), arr)

    def test_backup(self):
        files_store = get_default_files_store()
        arr = create_dummy_array(3, 3)
        files_store.store(new_data=arr, path='data_one')
        handler = files_store._get_handler(path='data_one')

        assert handler.s3_handler is not None
        assert handler.check_modification

        handler.backup()
        assert not handler.update_from_backup()
        assert handler.update_from_backup(force_update_from_backup=True)

        assert compare_dataset(files_store.read(path='data_one').sel(arr.coords), arr)

    def test_read_from_formula(self):
        self.test_store()
        files_store = get_default_files_store()
        data_three = files_store.read(path='data_three')
        data_one = files_store.read(path='data_one')
        data_two = files_store.read(path='data_two')
        assert compare_dataset(data_three, (data_one * data_two).rolling({'index': 3}).sum())


if __name__ == "__main__":
    test = TestFileStore()
    # test.test_store()
    # test.test_update()
    # test.test_append()
    test.test_backup()
    # test.test_read_from_formula()

