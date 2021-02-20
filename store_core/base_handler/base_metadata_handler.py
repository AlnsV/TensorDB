import xarray
import os
import pandas as pd

from abc import abstractmethod
from typing import Dict, List, Any, Union


class BaseMetadataHandler:
    def __init__(self, base_path: str, metadata_file_name: str, first_write: bool, *args, **kwargs):
        self.base_path = base_path
        self.index = None
        self.metadata_file_name = metadata_file_name
        self.metadata_path = os.path.join(self.base_path, self.metadata_file_name)
        self.first_write = first_write
        self.partitions_metadata: Dict[str, Dict[str, Any]] = {}
        self._partition_names: List[str] = None
        self.last_s3_modified_date = None

    @property
    def partition_names(self):
        if self._partition_names is None:
            self._partition_names = [name for name in self.partitions_metadata.keys()
                                     if name != self.metadata_file_name]
        return self._partition_names

    @abstractmethod
    def empty(self) -> xarray.Dataset:
        pass

    @abstractmethod
    def save(self):
        pass

    @abstractmethod
    def close(self):
        pass


