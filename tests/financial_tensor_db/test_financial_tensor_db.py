import pandas as pd
import numpy as np

from loguru import logger
from financial_tensor_db import FinancialTensorDB
from config.config_root_dir import TEST_DIR_FINANCIAL_TENSOR_DB


def get_default_financial_db():
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
        'Prices Last Valid': {
            'read': {
                'personalized_method': 'read_from_formula',
            },
            'read_from_formula': {
                'formula': '`Prices`.notnull().cumsum("date").idxmax("date")',
            }
        },
        'Total Shares Outstanding Time Series': {
            'handler': {
                'dims': ['date', 'security_id'],
                'bucket_name': 'test.bitacore.data.2.0',
            },
            'store': {
                'data_methods': ['get_generic_from_to_data', 'reindex', 'ffill', 'replace_last_valid_dim'],
            },
            'append': {
                'data_methods': [
                    'concat_start_date',
                    'get_generic_from_to_data',
                    'reindex',
                    'ffill',
                    'replace_last_valid_dim'
                ],
            },
            'get_generic_from_to_data': {
                'table_name': 'ft_shares_outstanding',
                'value_name': 'value',
                'from_to_name': 'from_date',
                'to_date_name': 'to_date',
            },
            'reindex': {
                'coords_to_reindex': ["date"],
                'reindex_path': 'Prices',
                'method_fill_value': 'ffill'
            },
            'ffill': {
                'dim': 'date'
            },
            'replace_last_valid_dim': {
                'replace_path': 'Prices Last Valid',
                'dim': 'date'
            }
        }
    }
    return FinancialTensorDB(
        base_path=TEST_DIR_FINANCIAL_TENSOR_DB,
        use_env=False,
        files_settings=files_settings,
        s3_handler={
            'aws_access_key_id': "AKIAV5EJ3JJSZ5JQTD3K",
            'aws_secret_access_key': "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",
            'region_name': 'us-east-2',
        }
    )


class TestFinancialTensorDB:
    """
    TODO: Improve this unitary tests, basically they are integration tests.
    """
    def test_add_file_settings(self):
        financial_db = get_default_financial_db()
        for file_settings_id, file_settings in financial_db._files_settings.items():
            financial_db.add_file_settings(
                data_field_name=file_settings_id,
                file_settings=file_settings
            )
            assert financial_db.get_file_settings(file_settings_id) == file_settings

    def test_prices(self):
        financial_db = get_default_financial_db()
        financial_db.store(
            path='Prices',
            start_date=pd.Timestamp('2019-06-01'),
            end_date=pd.Timestamp('2019-12-01'),
            security_id=[8589934597, 8589934617]
        )
        financial_db.append(
            path='Prices',
            end_date=pd.Timestamp('2019-12-31'),
            security_id=[8589934597, 8589934617]
        )
        data = financial_db.read('Prices')
        assert data.coords['date'][0] == np.datetime64('2019-06-03')
        assert data.coords['date'][-1] == np.datetime64('2019-12-31')
        assert data.isnull().sum().compute().values == 10
        assert financial_db.read('Prices').sizes['security_id'] == 2

    def test_from_to_data(self):
        self.test_prices()
        financial_db = get_default_financial_db()

        financial_db.store(
            path='Total Shares Outstanding Time Series',
            start_date=pd.Timestamp('2019-01-01'),
            end_date=pd.Timestamp('2019-12-01'),
            security_id=[8589934597, 8589934617]
        )
        financial_db.append(
            path='Total Shares Outstanding Time Series',
            end_date=pd.Timestamp('2019-12-31'),
            security_id=[8589934597, 8589934617]
        )
        shares = financial_db.read(path='Total Shares Outstanding Time Series')
        prices = financial_db.read(path='Prices')
        assert all(size1 == size2 for size1, size2 in zip(shares.sizes.values(), prices.sizes.values()))


if __name__ == "__main__":
    test = TestFinancialTensorDB()
    # test.test_prices()
    # test.test_add_file_settings()
    test.test_from_to_data()


