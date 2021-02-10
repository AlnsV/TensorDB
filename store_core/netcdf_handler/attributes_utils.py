import xarray
import numpy as np
import pandas as pd
import netCDF4
import ast

from loguru import logger
from typing import Dict, List, Any, Union


def get_attribute(path: str, name, group: str = None, default: Any = None) -> Any:
    dataset = netCDF4.Dataset(path, mode='r')
    group_partition = dataset.groups.get(group, dataset)
    transform = set(ast.literal_eval(group_partition.transformed_to_str_data))

    data = transform_attribute(name, group_partition.__dict__.get(name, str(default)), transform)
    dataset.close()
    return data


def get_all_attributes(path: str, group: str = None):
    dataset = netCDF4.Dataset(path, mode='r')
    group_partition = dataset.groups.get(group, dataset)
    transform = set(ast.literal_eval(group_partition.transformed_to_str_data))

    attributes = {}
    for key, attribute in group_partition.__dict__.items():
        attributes[key] = transform_attribute(key, attribute, transform)

    dataset.close()

    return attributes


def transform_attribute(key: str, attribute: Any, transform: set):
    if key in transform:
        if attribute == 'True' or attribute == 'False':
            return bool(attribute)
        elif attribute == 'nan':
            return np.nan

        return ast.literal_eval(attribute)

    return attribute


def save_attributes(path, group: str = None, **kwargs):
    dataset = netCDF4.Dataset(path, mode='a')
    group_partition = dataset.groups.get(group, dataset)
    attributes = {}
    transformed_to_str_data = ['transformed_to_str_data']
    if "transformed_to_str_data" in group_partition.__dict__:
        transformed_to_str_data = get_attribute(path, 'transformed_to_str_data')

    for key, val in kwargs.items():
        if isinstance(val, (list, dict, bool)) or val != val or val is None:
            transformed_to_str_data.append(key)
            attributes[key] = str(val)
        else:
            attributes[key] = val

    attributes['transformed_to_str_data'] = str(transformed_to_str_data)
    group_partition.setncatts(attributes)
    dataset.close()

