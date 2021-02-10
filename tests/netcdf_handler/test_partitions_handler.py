import os

from loguru import logger

from store_core.netcdf_handler.partitions_handler import PartitionsStore
from store_core.utils import create_dummy_array, compare_dataset
from config_path.config_root_dir import TEST_DIR_PARTITIONS


class TestPartitionsHandler:

    def test_write_new_partitions(self):
        partitions_store = PartitionsStore(
            path=TEST_DIR_PARTITIONS + '/creation',
            dims=['index', 'columns'],
            dims_type={'index': 'fixed', 'columns': 'percentage'},
            dims_space={'index': 5, 'columns': 0.1},
            first_write=True,
            default_free_value='free'
        )
        arr = create_dummy_array(10, 10)

        partitions_store.write_new_partition(
            arr.isel(
                index=list(range(5))
            )
        )
        partitions_store.write_new_partition(
            arr.isel(
                index=list(range(5, 10))
            )
        )
        dataset = partitions_store.get_dataset()

        assert compare_dataset(dataset, arr)
        assert len(partitions_store.partition_names) == 2

        partitions_store.close()

    def test_append_data(self):
        partitions_store = PartitionsStore(
            path=os.path.join(TEST_DIR_PARTITIONS, 'append'),
            dims=['index', 'columns'],
            dims_type={'index': 'fixed', 'columns': 'percentage'},
            dims_space={'index': 5, 'columns': 0.1},
            first_write=True,
            max_cached_data=5,
            default_free_value='free'
        )
        arr = create_dummy_array(9, 10)
        for i in range(9):
            partitions_store.append_data(arr.isel(index=i))

        partitions_store.save()

        dataset = partitions_store.get_dataset()
        assert compare_dataset(dataset, arr)
        assert len(partitions_store.partition_names) == 2

        partitions_store.close()

        assert partitions_store.count_writes == 2

    def test_store_data(self):
        partitions_store = PartitionsStore(
            path=os.path.join(TEST_DIR_PARTITIONS, 'append'),
            dims=['index', 'columns'],
            dims_type={'index': 'fixed', 'columns': 'percentage'},
            dims_space={'index': 5, 'columns': 0.1},
            first_write=True,
            max_cached_data=5,
            default_free_value='free'
        )
        arr = create_dummy_array(16, 10)

        partitions_store.store_data(arr)

        dataset = partitions_store.get_dataset()

        assert compare_dataset(dataset, arr)

        partitions_store.close()

    def test_update_data(self):
        self.test_store_data()
        partitions_store = PartitionsStore(
            path=os.path.join(TEST_DIR_PARTITIONS, 'append'),
            dims=['index', 'columns'],
            dims_type={'index': 'fixed', 'columns': 'percentage'},
            dims_space={'index': 5, 'columns': 0.1},
            max_cached_data=5,
            default_free_value='free'
        )
        arr = create_dummy_array(16, 10)

        partitions_store.update_data(arr)
        partitions_store.save()

        dataset = partitions_store.get_dataset()
        assert compare_dataset(dataset, arr)

        partitions_store.close()


if __name__ == "__main__":
    test = TestPartitionsHandler()
    # test.test_write_new_partitions()
    # test.test_append_data()
    # test.test_store_data()
    test.test_update_data()
