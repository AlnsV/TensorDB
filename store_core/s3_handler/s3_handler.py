import boto3
import os
import pandas as pd
import time

from typing import Dict, List, Any, Union, Callable, Set
from loguru import logger
from boto3.s3.transfer import TransferConfig
from multiprocessing.pool import ThreadPool


class S3Handler:
    def __init__(self,
                 aws_access_key_id: str,
                 aws_secret_access_key: str,
                 region_name: str,
                 **kwargs):

        self.s3 = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )

    @staticmethod
    def _multi_process_function(func: Callable, arguments: List[Dict[str, str]]):
        """
        TODO: Simplify or improve this code, probably would be better to use map
        """
        p = ThreadPool(processes=os.cpu_count())
        futures = [p.apply_async(func=func, kwds=kwds) for kwds in arguments]
        for future in futures:
            future.get()

    def download_file(self, bucket_name: str, local_path: str, s3_path: str = None, *args, **kwargs):
        s3_path = (os.path.dirname(local_path) if s3_path is None else s3_path).replace("\\", "/")

        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        self.s3.download_file(
            bucket_name,
            s3_path,
            local_path,
            Config=TransferConfig(max_concurrency=20),
        )

    def download_files(self, files_settings: List[Dict[str, str]]):
        S3Handler._multi_process_function(self.download_file, files_settings)

    def upload_files(self, files_settings: List[Dict[str, str]]):
        S3Handler._multi_process_function(self.upload_file, files_settings)

    def upload_file(self, bucket_name: str, local_path: str, s3_path: str = None, *args, **kwargs):
        s3_path = (os.path.dirname(local_path) if s3_path is None else s3_path).replace("\\", "/")
        self.s3.upload_file(
            local_path,
            bucket_name,
            s3_path,
            Config=TransferConfig(max_concurrency=20)
        )

    def get_head_object(self, bucket_name: str, s3_path: str, *args, **kwargs) -> Dict[str, Any]:
        return self.s3.head_object(Bucket=bucket_name, Key=s3_path.replace("\\", "/"))

    def get_etag(self, bucket_name: str, s3_path: str, *args, **kwargs) -> str:
        return self.get_head_object(bucket_name, s3_path.replace("\\", "/"))['ETag']

    def get_last_modified_date(self, bucket_name: str, s3_path: str, *args, **kwargs) -> pd.Timestamp:
        return pd.to_datetime(self.get_head_object(bucket_name, s3_path.replace("\\", "/"))['LastModified'])











