import xarray
import numpy as np
import os
import shutil
import zarr

from typing import Dict
from loguru import logger

from store_core.utils import create_dummy_array, compare_dataset
from store_core.zarr_handler.zarr_store import ZarrStore
from config_path.config_root_dir import TEST_DIR_ZARR


def get_default_zarr_store():
    return ZarrStore(
        base_path=TEST_DIR_ZARR,
        path='first_test',
        name='data_test',
        chunks={'index': 3, 'columns': 2},
        dims=['index', 'columns'],
        bucket_name='test.bitacore.data.2.0',
        s3_handler=dict(
            aws_access_key_id="AKIAV5EJ3JJSZ5JQTD3K",
            aws_secret_access_key="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",
            region_name='us-east-2',
        )
    )


class TestZarrStore:
    arr = xarray.DataArray(
        data=np.array([
            [1, 2, 7, 4, 5],
            [2, 3, 5, 5, 6],
            [3, 3, 11, 5, 6],
            [4, 3, 10, 5, 6],
            [5, 7, 8, 5, 6],
        ], dtype=float),
        dims=['index', 'columns'],
        coords={'index': [0, 1, 2, 3, 4], 'columns': [0, 1, 2, 3, 4]},
    )

    arr2 = xarray.DataArray(
        data=np.array([
            [1, 2, 7, 4, 5, 10, 13],
            [2, 3, 5, 5, 6, 11, 15],
            [2, 3, 5, 5, 6, 11, 15],
        ], dtype=float),
        dims=['index', 'columns'],
        coords={'index': [6, 7, 8], 'columns': [0, 1, 2, 3, 4, 5, 6]},
    )

    def test_store_data(self):
        a = get_default_zarr_store()
        a.store_data(TestZarrStore.arr)
        dataset = a.get_dataset()
        assert compare_dataset(dataset, TestZarrStore.arr)

    def test_append_data(self):
        a = get_default_zarr_store()
        a.s3_handler = None
        if os.path.exists(a.local_path):
            shutil.rmtree(a.local_path)

        arr = TestZarrStore.arr.to_dataset(name='data_test')
        for i in range(5):
            a.append_data(arr.isel(index=[i]))

        arr2 = TestZarrStore.arr2.to_dataset(name='data_test')
        for i in range(3):
            a.append_data(arr2.isel(index=[i]))

        total_data = xarray.concat([arr, arr2], dim='index')
        dataset = a.get_dataset()
        assert compare_dataset(dataset, total_data)

    def test_update_data(self):
        self.test_store_data()
        a = get_default_zarr_store()
        a.update_data(TestZarrStore.arr + 5)
        dataset = a.get_dataset()
        assert compare_dataset(dataset, TestZarrStore.arr + 5)

    def test_backup(self):
        """
        TODO: Improve this test
        """
        a = get_default_zarr_store()
        a.store_data(TestZarrStore.arr)
        a.backup()
        shutil.rmtree(a.local_path)
        a.update_from_backup()
        dataset = a.get_dataset()
        assert compare_dataset(dataset, TestZarrStore.arr)


if __name__ == "__main__":
    test = TestZarrStore()
    test.test_store_data()
    # test.test_append_data()
    # test.test_update_data()
    # test.test_backup()
