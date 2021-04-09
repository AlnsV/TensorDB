import xarray
import numpy as np

from loguru import logger

from store_core.data_handler.files_store import FilesStore
from store_core.zarr_handler.zarr_store import ZarrStore
from store_core.utils import create_dummy_array, compare_dataset
from config.config_root_dir import TEST_DIR_FILES_STORE


def get_default_files_store():
    default_settings = {
        'handler': {
            'dims': ['index', 'columns'],
            'bucket_name': 'test.bitacore.data.2.0',
            'data_handler': ZarrStore,
        },
    }

    files_settings = {
        'data_one': default_settings.copy(),
        'data_two': default_settings.copy(),
        'data_three': {
            'read': {
                'personalized_method': 'read_from_formula',
            },
            'read_from_formula': {
                'formula': "(`data_one` * `data_two`).rolling({'index': 3}).sum()",
            }
        },
        'data_ffill': {
            **default_settings,
            'store': {
                'data_methods': ['read_from_formula', 'ffill'],
            },
            'read_from_formula': {
                'formula': "`data_one`",
            },
            'ffill': {
                'dim': 'index'
            }
        },
        'data_replace_last_valid_dim': {
            **default_settings,
            'store': {
                'data_methods': ['read_from_formula', 'ffill', 'replace_last_valid_dim'],
            },
            'read_from_formula': {
                'formula': "`data_one`",
            },
            'ffill': {
                'dim': 'index'
            },
            'replace_last_valid_dim': {
                'replace_path': 'last_valid_index',
                'value': np.nan,
                'dim': 'index'
            }
        },
        'last_valid_index': {
            'store': {
                'data_methods': ['read_from_formula', 'last_valid_dim'],
            },
            'read_from_formula': {
                'formula': "`data_one`",
            },
            'last_valid_dim': {
                'dim': "index",
            }
        }
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
    """
    TODO: All the tests has dependencies with others, so probably should be good idea use pytest-order to stablish
        an order between the tests, using this we can avoid calling some test from another test
    """

    arr = xarray.DataArray(
        data=np.array([
            [1, 2, 7, 4, 5],
            [np.nan, 3, 5, 5, 6],
            [3, 3, np.nan, 5, 6],
            [np.nan, 3, 10, 5, 6],
            [np.nan, 7, 8, 5, 6],
        ], dtype=float),
        dims=['index', 'columns'],
        coords={'index': [0, 1, 2, 3, 4], 'columns': [0, 1, 2, 3, 4]},
    )

    arr2 = xarray.DataArray(
        data=np.array([
            [1, 2, 7, 4, 5],
            [2, 6, 5, 5, 6],
            [3, 3, 11, 5, 6],
            [4, 3, 10, 5, 6],
            [5, 7, 8, 5, 6],
        ], dtype=float),
        dims=['index', 'columns'],
        coords={'index': [0, 1, 2, 3, 4], 'columns': [0, 1, 2, 3, 4]},
    )

    def test_store(self):
        files_store = get_default_files_store()
        files_store.store(new_data=TestFileStore.arr, path='data_one')
        assert files_store.read(path='data_one').equals(TestFileStore.arr)

        files_store.store(new_data=TestFileStore.arr2, path='data_two')
        assert files_store.read(path='data_two').equals(TestFileStore.arr2)

    def test_update(self):
        self.test_store()
        files_store = get_default_files_store()
        files_store.update(new_data=TestFileStore.arr2, path='data_one')
        assert files_store.read(path='data_one').equals(TestFileStore.arr2)

    def test_append(self):
        self.test_store()
        files_store = get_default_files_store()

        arr = create_dummy_array(10, 5, dtype=int)
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

        assert files_store.read(path='data_one').sel(arr.coords).equals(arr)
        assert files_store.read(path='data_one').sizes['index'] > arr.sizes['index']

    def test_backup(self):
        files_store = get_default_files_store()
        files_store.store(new_data=TestFileStore.arr, path='data_one')

        handler = files_store._get_handler(path='data_one')
        assert handler.s3_handler is not None
        assert handler.check_modification

        handler.backup()
        assert not handler.update_from_backup()
        assert handler.update_from_backup(force_update_from_backup=True)

        assert files_store.read(path='data_one').sel(TestFileStore.arr.coords).equals(TestFileStore.arr)

    def test_read_from_formula(self):
        self.test_store()
        files_store = get_default_files_store()
        data_three = files_store.read(path='data_three')
        data_one = files_store.read(path='data_one')
        data_two = files_store.read(path='data_two')
        assert data_three.equals((data_one * data_two).rolling({'index': 3}).sum())

    def test_ffill(self):
        self.test_store()
        files_store = get_default_files_store()
        files_store.store(path='data_ffill')
        assert files_store.read(path='data_ffill').equals(files_store.read(path='data_one').ffill('index'))

    def test_last_valid_index(self):
        self.test_store()
        files_store = get_default_files_store()
        files_store.store(path='last_valid_index')
        assert np.array_equal(files_store.read(path='last_valid_index').values, [2, 4, 4, 4, 4])

    def test_replace_last_valid_dim(self):
        self.test_last_valid_index()
        files_store = get_default_files_store()
        files_store.store(path='data_replace_last_valid_dim')

        data_ffill = files_store.read(path='data_ffill')
        data_ffill.loc[[3, 4], 0] = np.nan
        assert files_store.read(path='data_replace_last_valid_dim').equals(data_ffill)


if __name__ == "__main__":
    test = TestFileStore()
    # test.test_store()
    # test.test_update()
    # test.test_append()
    # test.test_backup()
    # test.test_read_from_formula()
    # test.test_ffill()
    test.test_replace_last_valid_dim()
    # test.test_last_valid_index()


