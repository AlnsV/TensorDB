import os
import json

from store_core.s3_handler.s3_handler import S3Handler
from config_path.config_root_dir import TEST_DIR_S3


def get_default_s3_handler():
    return S3Handler(
        aws_access_key_id="AKIAV5EJ3JJSZ5JQTD3K",
        aws_secret_access_key="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",
        region_name='us-east-2',
    )


class TestS3Handler:
    data = {'test': 'test'}

    def test_upload_file(self):
        s3_handler = get_default_s3_handler()

        with open(os.path.join(TEST_DIR_S3, 'test.json'), mode='w') as json_file:
            json.dump(TestS3Handler.data, json_file)

        s3_handler.upload_file(
            bucket_name='test.bitacore.data.2.0',
            local_path=os.path.join(TEST_DIR_S3, 'test.json'),
            s3_path=os.path.join('test_s3', 'test.json')
        )

    def test_download_file(self):
        self.test_upload_file()
        s3_handler = get_default_s3_handler()
        s3_handler.download_file(
            bucket_name='test.bitacore.data.2.0',
            local_path=os.path.join(TEST_DIR_S3, 'test.json'),
            s3_path=os.path.join('test_s3', 'test.json')
        )
        with open(os.path.join(TEST_DIR_S3, 'test.json'), mode='r') as json_file:
            data_s3 = json.load(json_file)

        assert TestS3Handler.data == data_s3

    def test_get_head_object(self):
        self.test_upload_file()
        s3_handler = get_default_s3_handler()
        head = s3_handler.get_head_object(
            bucket_name='test.bitacore.data.2.0',
            s3_path=os.path.join('test_s3', 'test.json')
        )
        last_modified_date = s3_handler.get_last_modified_date(
            bucket_name='test.bitacore.data.2.0',
            s3_path=os.path.join('test_s3', 'test.json')
        )
        etag = s3_handler.get_etag(
            bucket_name='test.bitacore.data.2.0',
            s3_path=os.path.join('test_s3', 'test.json')
        )
        assert etag == '"2359cdd9f6124ef769448b8f34c54d65"'
        assert last_modified_date.strftime('%Y-%m-%d') == head['LastModified'].strftime('%Y-%m-%d')


if __name__ == "__main__":
    test = TestS3Handler()
    test.test_upload_file()
    # test.test_download_file()
    # test.test_get_head_object()


