import xarray
import numpy as np
import zarr
import os
import pandas as pd
import json

from typing import Dict, List, Any, Union, Callable, Generic
from loguru import logger
from datetime import datetime

from store_core.base_handler.base_store import BaseStore
from store_core.s3_handler.s3_handler import S3Handler


class ZarrStore(BaseStore):
    """
    TODO: The next versions of zarr will add support for the modification dates of the chunks, that will simplify
        the code of backup, so It is a good idea modify the code after the modification being publish
    """
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

        new_data = self.transform_to_dataset(new_data)
        new_data.to_zarr(self.local_path, group=self.group, mode='w', *args, **kwargs)
        self.check_modification = True

    def append_data(self,
                    new_data: Union[xarray.DataArray, xarray.Dataset],
                    *args,
                    **kwargs):

        if not os.path.exists(self.local_path):
            self.update_from_backup(raise_error_missing_backup=False, *args, **kwargs)

        if not os.path.exists(self.local_path):
            return self.store_data(new_data=new_data, *args, **kwargs)

        new_data = self.transform_to_dataset(new_data)
        act_coords = {k: coord.values for k, coord in self.get_dataset().coords.items()}

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
        if not os.path.exists(self.local_path):
            self.update_from_backup(raise_error_missing_backup=True, *args, **kwargs)

        if isinstance(new_data, xarray.Dataset):
            new_data = new_data.to_array()
        act_coords = {k: coord.values for k, coord in self.get_dataset().coords.items()}

        coords_names = list(act_coords.keys())
        bitmask = np.isin(act_coords[coords_names[0]], new_data.coords[coords_names[0]].values)
        for coord_name in coords_names[1:]:
            bitmask = bitmask & np.isin(act_coords[coord_name], new_data.coords[coord_name].values)[:, None]

        arr = zarr.open(os.path.join(self.local_path, self.name), mode='a')
        arr.set_mask_selection(bitmask, new_data.values.ravel())
        self.check_modification = True

    def upsert_data(self, new_data: Union[xarray.DataArray, xarray.Dataset], *args, **kwargs):
        self.update_data(new_data, *args, **kwargs)
        self.append_data(new_data, *args, **kwargs)

    def get_dataset(self, *args, **kwargs) -> xarray.Dataset:
        return xarray.open_zarr(
            self.local_path,
            group=self.group,
            *args,
            **kwargs
        )

    def get_chunks_modified_dates(self):
        chunks_dates = {}
        try:
            arr_store = zarr.open(self.local_path, mode='r')
        except zarr.errors.PathNotFoundError:
            return chunks_dates

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

    def backup(self, overwrite_backup: bool = False, *args, **kwargs) -> bool:

        """
            TODO:
                1) Simplify the code, probably the best option is create a class to handle the extra metadata
                    and follow the logic used to update_from_backup
        """

        if not overwrite_backup:
            if not self.check_modification or self.s3_handler is None:
                return False

        self.check_modification = False
        arr_store = zarr.open(self.local_path, mode='a')
        files_modified = []

        for chunk_name in arr_store.chunk_store.keys():
            total_path = os.path.join(self.local_path, chunk_name)
            modified_date = pd.to_datetime(datetime.fromtimestamp(os.path.getmtime(total_path)))

            upload = False
            if (
                    overwrite_backup or
                    chunk_name not in self.chunks_modified_dates or
                    modified_date != self.chunks_modified_dates[total_path]
            ):
                upload = True

            if upload:
                files_modified.append(dict(
                    local_path=total_path,
                    s3_path=os.path.join(self.path, chunk_name).replace('\\', '/'),
                    bucket_name=self.bucket_name,
                    **kwargs
                ))

        if len(files_modified) > 0:
            backup_date = str(pd.Timestamp.now())

            # adding data about the backup, this is useful to avoid download all the information again and again
            with open(os.path.join(self.local_path, 'zbackup_date.json'), 'w') as json_file:
                json.dump({'backup_date': backup_date}, json_file)

            zchunks_backup_metadata = arr_store.attrs.get('zchunks_backup_metadata', {})
            zchunks_backup_metadata.update({
                file_modified['s3_path']: backup_date
                for file_modified in files_modified
            })
            arr_store.attrs['zchunks_backup_metadata'] = zchunks_backup_metadata

            for name in ['zbackup_date.json', '.zattrs']:
                files_modified.append(dict(
                    local_path=os.path.join(self.local_path, name),
                    s3_path=os.path.join(self.path, name).replace('\\', '/'),
                    bucket_name=self.bucket_name,
                    **kwargs
                ))

            # uploading all the files in parallel
            self.s3_handler.upload_files(files_modified)

            # update the chunks modified dates
            self.chunks_modified_dates = self.get_chunks_modified_dates()

        return True

    def equal_to_backup(self, *args, **kwargs) -> str:
        if not os.path.exists(os.path.join(self.local_path, 'zbackup_date.json')):
            return "not equal"

        with open(os.path.join(self.local_path, 'zbackup_date.json'), 'r') as json_file:
            backup_date = json.load(json_file)['backup_date']

        try:
            self.s3_handler.download_file(
                bucket_name=self.bucket_name,
                local_path=os.path.join(self.local_path, 'zbackup_date.json'),
                s3_path=os.path.join(self.path, 'zbackup_date.json').replace('\\', '/'),
                max_concurrency=1,
            )
        except KeyError:
            return "not backup"

        with open(os.path.join(self.local_path, 'zbackup_date.json'), 'r') as json_file:
            backup_date_s3 = json.load(json_file)['backup_date']

        if backup_date == backup_date_s3:
            return "equal"

    def update_from_backup(self,
                           force_update_from_backup: bool = False,
                           raise_error_missing_backup: bool = True,
                           *args,
                           **kwargs) -> bool:
        if self.s3_handler is None:
            return False

        force_update_from_backup = force_update_from_backup | (not os.path.exists(self.local_path))

        is_equal = self.equal_to_backup()
        if not force_update_from_backup:
            if is_equal == 'equal':
                return False
            if is_equal == 'not backup':
                if raise_error_missing_backup:
                    raise KeyError(f"The path: {self.path} not exist in s3 in the bucket: {self.bucket_name}")
                return False

        last_backup_dates = {}
        if os.path.exists(os.path.join(self.local_path, '.zattrs')):
            with open(os.path.join(self.local_path, '.zattrs'), mode='r') as json_file:
                last_backup_dates = json.load(json_file)['zchunks_backup_metadata']

        self.s3_handler.download_file(
            bucket_name=self.bucket_name,
            local_path=os.path.join(self.local_path, '.zattrs'),
            s3_path=os.path.join(self.path, '.zattrs').replace('\\', '/'),
            **kwargs
        )

        with open(os.path.join(self.local_path, '.zattrs'), mode='r') as json_file:
            last_backup_dates_s3 = json.load(json_file)['zchunks_backup_metadata']

        files_to_download = [
            dict(
                bucket_name=self.bucket_name,
                local_path=os.path.join(self.base_path, path),
                s3_path=path,
                **kwargs
            )
            for path, date in last_backup_dates_s3.items()
            if path != os.path.join(self.path, '.zattrs') and (
                    force_update_from_backup or date != last_backup_dates.get(path, '')
            )
        ]
        if len(files_to_download) == 0:
            return False

        self.s3_handler.download_files(files_to_download)

        return True

    def close(self, *args, **kwargs):
        self.backup(*args, **kwargs)



