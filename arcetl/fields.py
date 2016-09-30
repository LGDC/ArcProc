# -*- coding=utf-8 -*-
"""Field operations."""
import logging

import arcpy

from arcetl import dataset
from . import helpers, metadata


LOG = logging.getLogger(__name__)


@helpers.log_function
def duplicate_field(dataset_path, field_name, new_field_name, **kwargs):
    """Create new field as a duplicate of another.

    Args:
        dataset_path (str): Path of dataset.
        field_name (str): Name of field.
        new_field_name (str): Field name to call duplicate.
    Kwargs:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('dataset_where_sql', None), ('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    log_level = helpers.LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Duplicate field %s as %s on %s.",
            field_name, new_field_name, dataset_path)
    field_meta = metadata.field_metadata(dataset_path, field_name)
    field_meta['name'] = new_field_name
    # Cannot add OID-type field, so push to a long-type.
    if field_meta['type'].lower() == 'oid':
        field_meta['type'] = 'long'
    dataset.add_field_from_metadata(dataset_path, field_meta, log_level=None)
    LOG.log(log_level, "End: Duplicate.")
    return new_field_name


@helpers.log_function
def join_field(dataset_path, join_dataset_path, join_field_name,
               on_field_name, on_join_field_name, **kwargs):
    """Add field and its values from join-dataset.

    Args:
        dataset_path (str): Path of dataset.
        join_dataset_path (str): Path of dataset to join field from.
        join_field_name (str): Name of field to join.
        on_field_name (str): Name of field to join the dataset on.
        on_join_field_name (str): Name of field to join the join-dataset on.
    Kwargs:
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    log_level = helpers.LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Join field %s on %s from %s.",
            join_field_name, dataset_path, join_dataset_path)
    arcpy.management.JoinField(
        in_data=dataset_path, in_field=on_field_name,
        join_table=join_dataset_path, join_field=on_join_field_name,
        fields=join_field_name)
    LOG.log(log_level, "End: Join.")
    return join_field_name


@helpers.log_function
def rename_field(dataset_path, field_name, new_field_name, **kwargs):
    """Rename field.

    Args:
        dataset_path (str): Path of dataset.
        field_name (str): Name of field.
        new_field_name (str): Field name to change to.
    Kwargs:
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    log_level = helpers.LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Rename field %s to %s on %s.",
            field_name, new_field_name, dataset_path)
    arcpy.management.AlterField(
        in_table=dataset_path, field=field_name, new_field_name=new_field_name)
    LOG.log(log_level, "End: Rename.")
    return new_field_name
