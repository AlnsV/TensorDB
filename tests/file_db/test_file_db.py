from loguru import logger

from bitacore.file_db.file_db import FileDB
from store_core.utils import create_dummy_array, compare_dataset
from config_path.config_root_dir import TEST_DIR_FILE_DB


def get_default_file_db():


    return FileDB(
        base_path=TEST_DIR_FILE_DB,
        use_env=False,
        s3_handler={
            'aws_access_key_id': "AKIAV5EJ3JJSZ5JQTD3K",
            'aws_secret_access_key': "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",
            'region_name': 'us-east-2',
        }
    )


class TestFileDB:
    def test_store(self):
        file_db = get_default_file_db()



if __name__ == "__main__":
    test = TestFileDB()
    test.test_store()

