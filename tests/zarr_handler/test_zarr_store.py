import xarray
import numpy as np
import os
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

    def test_store_data(self):
        arr = create_dummy_array(5, 5)
        a = get_default_zarr_store()
        a.store_data(arr)
        dataset = a.get_dataset()
        assert compare_dataset(dataset, arr)

    def test_append_data(self):
        self.test_store_data()
        arr = create_dummy_array(8, 7).isel(index=[-3, -2, -1])
        arr = arr.to_dataset(name='data_test')
        a = get_default_zarr_store()
        dataset = a.get_dataset()
        total_data = xarray.concat([dataset, arr], dim='index')
        for i in range(3):
            a.append_data(arr.isel(index=[i]))
        dataset = a.get_dataset()
        assert compare_dataset(dataset, total_data)

    def test_update_data(self):
        self.test_store_data()
        arr = create_dummy_array(3, 5).isel(columns=[0, 2, 4])
        a = get_default_zarr_store()
        a.update_data(arr)
        dataset = a.get_dataset().sel(arr.coords)
        assert compare_dataset(dataset, arr)

    def test_backup(self):
        """
        TODO: Improve this test
        """
        arr = create_dummy_array(3, 3)
        a = get_default_zarr_store()
        a.store_data(arr)
        a.backup()
        a.update_from_backup()
        dataset = a.get_dataset().sel(arr.coords)
        assert compare_dataset(dataset, arr)


if __name__ == "__main__":
    test = TestZarrStore()
    # test.test_store_data()
    # test.test_append_data()
    # test.test_update_data()
    test.test_backup()
