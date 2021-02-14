import os

from datetime import date

from store_core.s3_handler.s3_handler import S3Handler
from store_core.netcdf_handler.core_handler import CoreNetcdfHandler
from config_path.config_root_dir import TEST_DIR_S3


def get_default_s3_handler():
    return S3Handler(
        aws_access_key_id="AKIAV5EJ3JJSZ5JQTD3K",
        aws_secret_access_key="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",
        region_name='us-east-2',
    )


class TestS3Handler:
    def test_upload_file(self):
        s3_handler = get_default_s3_handler()
        s3_handler.upload_file(
            bucket_name='test.bitacore.data.2.0',
            local_path=os.path.join(TEST_DIR_S3, '0.nc'),
            s3_path='s3_test/0.nc'
        )

    def test_download_file(self):
        self.test_upload_file()
        s3_handler = get_default_s3_handler()
        arr = CoreNetcdfHandler.get_external_computed_array(os.path.join(TEST_DIR_S3, '0.nc'))
        s3_handler.download_file(
            bucket_name='test.bitacore.data.2.0',
            local_path=os.path.join(TEST_DIR_S3, '0.nc'),
            s3_path='s3_test/0.nc'
        )
        arr_download = CoreNetcdfHandler.get_external_computed_array(os.path.join(TEST_DIR_S3, '0.nc'))
        assert arr.sel(arr_download.coords).equals(arr_download)

    def test_get_head_object(self):
        self.test_upload_file()
        s3_handler = get_default_s3_handler()
        head = s3_handler.get_head_object(bucket_name='test.bitacore.data.2.0', s3_path='s3_test/0.nc')
        last_modified_date = s3_handler.get_last_modified_date(bucket_name='test.bitacore.data.2.0',
                                                               s3_path='s3_test/0.nc')
        etag = s3_handler.get_etag(bucket_name='test.bitacore.data.2.0', s3_path='s3_test/0.nc')
        assert etag == '"b0b3ef17682e548b33183b725f93397b"'
        assert last_modified_date.strftime('%Y-%m-%d') == head['LastModified'].strftime('%Y-%m-%d')


if __name__ == "__main__":
    test = TestS3Handler()
    test.test_upload_file()
    # test.test_download_file()
    # test.test_get_head_object()


