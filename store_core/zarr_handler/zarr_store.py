import xarray
import numpy as np
import zarr
import os
import pandas as pd

from typing import Dict, List, Any, Union, Callable, Generic
from loguru import logger
from datetime import datetime

from store_core.base_handler.base_store import BaseStore
from store_core.s3_handler.s3_handler import S3Handler


class ZarrStore(BaseStore):

    def __init__(self,
                 dims: List[str] = None,
                 name: str = "data",
                 chunks: Dict[str, int] = None,
                 group: str = None,
                 s3_handler: Union[S3Handler, Dict] = None,
                 bucket_name: str = None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.dims = dims
        self.name = name
        self.chunks = chunks
        self.group = group
        self.dataset = None
        self.s3_handler = s3_handler
        if isinstance(s3_handler, Dict):
            self.s3_handler = S3Handler(**s3_handler) if isinstance(s3_handler, dict) else s3_handler

        self.bucket_name = bucket_name
        self.chunks_modified_dates = self.get_chunks_modified_dates()
        self.check_modification = False

    def store_data(self,
                   new_data: Union[xarray.DataArray, xarray.Dataset],
                   *args,
                   **kwargs):

        self.close_dataset()
        new_data = self.transform_to_dataset(new_data)
        new_data.to_zarr(self.local_path, group=self.group, mode='w', *args, **kwargs)
        self.check_modification = True

    def append_data(self,
                    new_data: Union[xarray.DataArray, xarray.Dataset],
                    *args,
                    **kwargs):

        new_data = self.transform_to_dataset(new_data)
        act_coords = {k: coord.values for k, coord in self.get_dataset().coords.items()}
        self.close_dataset()

        for dim, new_coord in new_data.coords.items():
            new_coord = new_coord.values
            coord_to_append = new_coord[~np.isin(new_coord, act_coords[dim])]
            if len(coord_to_append) == 0:
                continue

            reindex_coords = {
                k: coord_to_append if k == dim else act_coord
                for k, act_coord in act_coords.items()
            }
            data_to_append = new_data.reindex(reindex_coords)
            act_coords[dim] = np.concatenate([act_coords[dim], coord_to_append])
            data_to_append.to_zarr(
                self.local_path,
                append_dim=dim,
                compute=True,
                *args,
                **kwargs
            )

            self.check_modification = True

    def update_data(self,
                    new_data: Union[xarray.DataArray, xarray.Dataset],
                    *args,
                    **kwargs):
        """
        TODO: Avoid loading the entire new data in memory
              Using the to_zarr method of xarray and updating in blocks with the region parameter is
              probably a good solution, the only problem is the time that could take to update,
              but I suppose that the block updating is ideal only when the new_data represent a big % of the entire data
        """
        if isinstance(new_data, xarray.Dataset):
            new_data = new_data.to_array()
        act_coords = {k: coord.values for k, coord in self.get_dataset().coords.items()}

        coords_names = list(act_coords.keys())
        bitmask = np.isin(act_coords[coords_names[0]], new_data.coords[coords_names[0]].values)
        for coord_name in coords_names[1:]:
            bitmask = bitmask & np.isin(act_coords[coord_name], new_data.coords[coord_name].values)[:, None]

        self.close_dataset()

        arr = zarr.open(os.path.join(self.local_path, self.name), mode='a')
        arr.set_mask_selection(bitmask, new_data.values.ravel())
        self.check_modification = True

    def get_dataset(self,
                    *args,
                    **kwargs) -> xarray.Dataset:

        if self.dataset is None:
            self.dataset = xarray.open_zarr(
                self.local_path,
                group=self.group,
                *args,
                **kwargs
            )

        return self.dataset

    def get_chunks_modified_dates(self):

        if not os.path.exists(os.path.join(self.local_path)):
            return {}

        chunks_dates = {}
        arr_store = zarr.open(self.local_path, mode='r')
        for chunk_name in arr_store.chunk_store.keys():
            total_path = os.path.join(self.local_path, chunk_name)
            date = pd.to_datetime(datetime.fromtimestamp(os.path.getmtime(total_path)))
            chunks_dates[total_path] = date
        return chunks_dates

    def transform_to_dataset(self, new_data) -> xarray.Dataset:

        new_data = new_data
        if isinstance(new_data, xarray.DataArray):
            new_data = new_data.to_dataset(name=self.name)
            new_data = new_data if self.chunks is None else new_data.chunk(self.chunks)
        return new_data

    def backup(self, *args, **kwargs):

        """
        TODO: Use threads for uploading the data to s3
        """
        if not self.check_modification or self.s3_handler is None:
            return

        self.check_modification = False
        arr_store = zarr.open(self.local_path, mode='r')
        for chunk_name in arr_store.chunk_store.keys():
            total_path = os.path.join(self.local_path, chunk_name)
            modified_date = pd.to_datetime(datetime.fromtimestamp(os.path.getmtime(total_path)))
            upload = False
            if chunk_name not in self.chunks_modified_dates or modified_date != self.chunks_modified_dates[total_path]:
                upload = True

            if upload:
                self.s3_handler.upload_file(
                    local_path=total_path,
                    s3_path=os.path.join(self.path, chunk_name),
                    bucket_name=self.bucket_name,
                    **kwargs
                )
        self.chunks_modified_dates = self.get_chunks_modified_dates()

    def update_from_backup(self, *args, **kwargs):

        """
        TODO: Use threads for downloading the data to s3
        """

        if self.s3_handler is None:
            return
        self.close_dataset()

        files_names = self.s3_handler.s3.list_objects(
            Bucket=self.bucket_name,
            Prefix=self.path
        )['Contents']

        for obj in files_names:
            local_path = os.path.join(self.base_path, obj['Key'])

            if not os.path.exists(os.path.dirname(local_path)):
                os.makedirs(os.path.dirname(local_path))

            if obj['Key'][-1] == '/':
                continue

            self.s3_handler.download_file(
                bucket_name=self.bucket_name,
                local_path=local_path,
                s3_path=obj['Key']
            )

    def close_dataset(self):

        if self.dataset is not None:
            self.dataset.close()
            self.dataset = None

    def close(self, *args, **kwargs):

        self.backup(*args, **kwargs)
        self.close_dataset()



