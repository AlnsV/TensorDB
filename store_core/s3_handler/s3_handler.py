import boto3
import os

from typing import Dict, List, Any, Union, Callable, Set
from loguru import logger
from boto3.s3.transfer import TransferConfig


class S3Handler:
    def __init__(self,
                 aws_access_key_id: str,
                 aws_secret_access_key: str,
                 region_name: str,
                 files_settings: Dict[str, Dict[str, Any]]):

        self.s3 = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )
        self.files_settings = files_settings

    def download_file(self, bucket_name, s3_path, local_path, *args, **kwargs):
        self.s3.download_file(
            bucket_name,
            s3_path,
            local_path,
            Config=TransferConfig(max_concurrency=20)
        )

    def upload_file(self, bucket_name, local_path, s3_path, *args, **kwargs):
        self.s3.upload_file(local_path, s3_path, bucket_name)

    def get_head_object(self, bucket_name, s3_path, *args, **kwargs):
        return self.s3.head_object(Bucket=bucket_name, Key=s3_path)

    def get_etag(self, bucket_name, s3_path, *args, **kwargs):
        return self.get_head_object(bucket_name, s3_path)['ETag']

    def get_last_modified_date(self, bucket_name, s3_path, *args, **kwargs):
        return self.get_head_object(bucket_name, s3_path)['LastModified']











