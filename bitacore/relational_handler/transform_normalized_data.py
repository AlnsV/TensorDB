import pandas as pd
import xarray
import numpy as np


def transform_normalized_data(df: pd.DataFrame) -> xarray.DataArray:

    if 'from_date' in df.columns and 'security_id' in df.columns:
        return _transform_2d_normalized_from_to_to_dataset(df)

    if 'date' in df.columns and 'security_id' in df.columns:
        return _transform_2d_normalized_to_dataset(df)

    if 'security_id' in df.columns and len(df.columns) == 2:
        df = df.set_index('security_id')
        return xarray.DataArray(
            df.to_numpy(),
            coords={'security_id': list(df.index)},
            dims=['security_id']
        )

    if 'security_id' in df.columns and len(df.columns) == 3:
        c = df.columns[~df.columns.isin(['security_id', 'names'])][0]
        df = df.pivot(index='security_id', columns=c, values='value')
        return xarray.DataArray(
            df.to_numpy(),
            coords={
                'security_id': list(df.index),
                f'{c}': list(df.columns)
            },
            dims=['security_id', f'{c}']
        )

    if 'value' in df.columns and len(df.columns) == 3:
        df = df.pivot(*[df[c] for c in df.columns])
        return xarray.DataArray(
            df.to_numpy(),
            coords={
                df.index.name: list(df.index),
                df.columns.name: list(df.columns)
            },
            dims=[df.index.name, df.columns.name]
        )

    if 'value' in df.columns and len(df.columns) == 2:
        df = df.set_index(df.columns[0], inplace=True)
        return xarray.DataArray(
            df.to_numpy(),
            coords={df.index.name: list(df.index)},
            dims=[df.index.name]
        )

    return xarray.DataArray(
        df.to_numpy(),
        coords={
            df.index.name: list(df.index),
            df.columns.name: list(df.columns)
        },
        dims=[df.index.name, df.columns.name]
    )


def _transform_2d_normalized_to_dataset(df: pd.DataFrame):
    df = df.pivot(index='date', columns='security_id', values='value')
    return xarray.DataArray(
        df.to_numpy(),
        coords={
            'date': list(df.index),
            'security_id': list(df.columns)
        },
        dims=['date', 'security_id']
    )


def _transform_2d_normalized_from_to_to_dataset(df: pd.DataFrame):
    last_valid_date = df[['security_id', 'to_date']].groupby('security_id').max().iloc[:, 0]

    df = df.pivot(index='from_date', columns='security_id', values='value')
    df.ffill(inplace=True)

    last_valid_date.fillna(df.index[-1], inplace=True)
    last_valid_date.iloc[:] = pd.to_datetime(last_valid_date.values)

    valid_positions = np.tile(df.index, (len(df.columns), 1)).T <= last_valid_date.to_numpy()[None, :]
    df.where(valid_positions, np.nan, inplace=True)

    return xarray.DataArray(
        df.to_numpy(),
        coords={
            'date': list(df.index),
            'security_id': list(df.columns)
        },
        dims=['date', 'security_id']
    )



