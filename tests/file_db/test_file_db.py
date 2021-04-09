import pandas as pd
import numpy as np

from loguru import logger
from bitacore.file_db.file_db import FileDB
from store_core.utils import create_dummy_array, compare_dataset
from config_path.config_root_dir import TEST_DIR_FILE_DB


def get_default_file_db():
    files_settings = {
        'Prices': {
            'handler': {
                'dims': ['date', 'security_id'],
                'bucket_name': 'test.bitacore.data.2.0',
            },
            'store': {
                'data_methods': ['get_prices_data'],
            },
            'append': {
                'data_methods': ['concat_start_date', 'get_prices_data'],
            }
        },
    }
    return FileDB(
        base_path=TEST_DIR_FILE_DB,
        use_env=False,
        files_settings=files_settings,
        s3_handler={
            'aws_access_key_id': "AKIAV5EJ3JJSZ5JQTD3K",
            'aws_secret_access_key': "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",
            'region_name': 'us-east-2',
        }
    )


class TestFileDB:

    def test_prices(self):
        file_db = get_default_file_db()
        file_db.store(path='Prices', start_date=pd.Timestamp('2020-01-01'), end_date=pd.Timestamp('2020-01-02'))
        file_db.append(path='Prices', start_date=pd.Timestamp('2020-01-02'), end_date=pd.Timestamp('2020-01-03'))
        assert (
            file_db.read('Prices').coords['date'] == np.array([
                np.datetime64('2020-01-02'),
                np.datetime64('2020-01-03')
            ])
        ).all()
        assert file_db.read('Prices').sizes['security_id'] > 18000

    def test_add_file_settings(self):
        file_db = get_default_file_db()
        file_db.add_file_settings(
            data_field_name='Prices',
            file_settings=file_db._files_settings['Prices']
        )
        assert file_db.get_file_settings('Prices') == file_db._files_settings['Prices']


if __name__ == "__main__":
    test = TestFileDB()
    # test.test_prices()
    test.test_add_file_settings()


