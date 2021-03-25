import pandas as pd
import xarray
import numpy as np


def transform_2d_time_series_normalized_to_dataset(df: pd.DataFrame):
    if 'from_date' in df:
        return _transform_2d_normalized_from_to_to_dataset(df)
    return _transform_2d_normalized_to_dataset(df)


def _transform_2d_normalized_to_dataset(df: pd.DataFrame):
    df = df.pivot(index='dates', columns='security_id', values='value')
    return xarray.Dataset(
        df.to_numpy(),
        coords={
            'index': df.index,
            'columns': df.columns
        },
        dims=[df.index.name, df.columns.name]
    )


def _transform_2d_normalized_from_to_to_dataset(df: pd.DataFrame):
    last_valid_dates = df[['security_id', 'to_date']].groupby('security_id').max().iloc[:, 0]

    df = df.pivot(index='from_date', columns='security_id', values='value')
    df.ffill(inplace=True)

    last_valid_dates.fillna(df.index[-1], inplace=True)
    last_valid_dates.iloc[:] = pd.to_datetime(last_valid_dates.values)

    valid_positions = np.tile(df.index, (len(df.columns), 1)).T <= last_valid_dates.to_numpy()[None, :]
    df.where(valid_positions, np.nan, inplace=True)
    df.index.name = 'dates'

    return xarray.Dataset(
        df.to_numpy(),
        coords={
            'index': df.index,
            'columns': df.columns
        },
        dims=[df.index.name, df.columns.name]
    )



