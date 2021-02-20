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
    data = transform_saved_attribute(name, group_partition.__dict__.get(name, str(default)))
    dataset.close()
    return data


def get_all_attributes(path: str, group: str = None):
    dataset = netCDF4.Dataset(path, mode='r')
    group_partition = dataset.groups.get(group, dataset)
    attributes = {}
    for key, attribute in group_partition.__dict__.items():
        attributes[key] = transform_saved_attribute(key, attribute)

    dataset.close()

    return attributes


def transform_saved_attribute(key: str, attribute: Any):
    if not isinstance(attribute, str):
        return attribute

    attribute = attribute
    if "[" in attribute or "{" in attribute:
        attribute = ast.literal_eval(attribute)

    if isinstance(attribute, dict):
        return {key_in: transform_saved_attribute(key_in, attribute_in) for key_in, attribute_in in attribute.items()}
    if isinstance(attribute, list):
        return [transform_saved_attribute(key, attribute_in) for attribute_in in attribute]
    if attribute == 'True' or attribute == 'False':
        return bool(attribute)
    if attribute == 'nan':
        return np.nan
    if attribute == 'None':
        return None
    if "date" in key:
        return pd.to_datetime(attribute)

    return attribute


def save_attributes(path, group: str = None, **kwargs):
    dataset = netCDF4.Dataset(path, mode='a')
    group_partition = dataset.groups.get(group, dataset)
    attributes = {}

    for key, attribute in kwargs.items():
        attributes[key] = transform_attributes_to_save(attribute)
    group_partition.setncatts(attributes)
    dataset.close()


def transform_attributes_to_save(attribute):
    if isinstance(attribute, str):
        return attribute
    if isinstance(attribute, dict):
        return str({
            transform_attributes_to_save(k): transform_attributes_to_save(attribute_in)
            for k, attribute_in in attribute.items()
        })
    if isinstance(attribute, list):
        return str([transform_attributes_to_save(attribute_in) for attribute_in in attribute])
    if isinstance(attribute, pd.Timestamp):
        return attribute.isoformat()
    if isinstance(attribute, bool) or attribute != attribute or attribute is None:
        return str(attribute)

    return attribute

