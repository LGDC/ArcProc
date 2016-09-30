# -*- coding=utf-8 -*-
"""Field operations."""
import logging

import arcpy

from . import arcwrap, helpers, metadata
from arcetl.attributes import update_by_function


LOG = logging.getLogger(__name__)


@helpers.log_function
def add_field(dataset_path, field_name, field_type, **kwargs):
    """Add field to dataset.

    Wraps arcwrap.add_field.

    Args:
        dataset_path (str): Path of dataset.
        field_name (str): Name of field.
        field_type (str): Type of field.
    Kwargs:
        field_length (int): Length of field.
        field_precision (int): Precision of field.
        field_scale (int): Scale of field.
        field_is_nullable (bool): Flag indicating if field will be nullable.
        field_is_required (bool): Flag indicating if field will be required.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    # Other kwarg defaults set in the wrapped function.
    for kwarg_default in [('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    log_level = helpers.LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Add field %s to %s.", field_name, dataset_path)
    result = arcwrap.add_field(dataset_path, field_name, field_type, **kwargs)
    LOG.log(log_level, "End: Add.")
    return result


@helpers.log_function
def add_fields_from_metadata_list(dataset_path, metadata_list, **kwargs):
    """Add fields to dataset from list of metadata dictionaries.

    Args:
        dataset_path (str): Path of dataset.
        metadata_list (iter): Iterable of field metadata.
    Kwargs:
        log_level (str): Level at which to log this function.
    Returns:
        list.
    """
    for kwarg_default in [('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    log_level = helpers.LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(
        log_level, "Start: Add fields to %s from metadata list.", dataset_path)
    field_keywords = ['name', 'type', 'length', 'precision', 'scale',
                      'is_nullable', 'is_required']
    for field_meta in metadata_list:
        add_kwargs = {'field_{}'.format(kw): field_meta[kw]
                      for kw in field_keywords if kw in field_meta}
        field_name = arcwrap.add_field(dataset_path, **add_kwargs)
        LOG.log(log_level, "Added %s.", field_name)
    LOG.log(log_level, "End: Add.")
    return [field_meta['name'] for field_meta in metadata_list]


@helpers.log_function
def add_index(dataset_path, field_names, **kwargs):
    """Add index to dataset fields.

    Index names can only be applied to non-spatial indexes for geodatabase
    feature classes and tables. There is a limited length allowed from index
    names, which will be truncated to without warning.

    Args:
        dataset_path (str): Path of dataset.
        field_names (iter): Iterable of field names.
    Kwargs:
        fail_on_lock_ok (bool): Flag indicating success even if dataset locks
            prevent adding index.
        index_name (str): Optional name for index.
        is_ascending (bool): Flag indicating index built in ascending order.
        is_unique (bool): Flag indicating index built with unique constraint.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [
            ('fail_on_lock_ok', False),
            ('index_name', '_'.join(['ndx'] + field_names)),
            ('is_ascending', False), ('is_unique', False),
            ('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    log_level = helpers.LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Add index on field(s) %s on %s.",
            field_names, dataset_path)
    dataset_meta = metadata.dataset_metadata(dataset_path)
    index_types = {
        field['type'].lower() for field in dataset_meta['fields']
        if field['name'].lower() in (name.lower() for name in field_names)}
    if 'geometry' in index_types:
        if len(field_names) > 1:
            raise RuntimeError("Cannot create a composite spatial index.")
        add_function = arcpy.management.AddSpatialIndex
        add_kwargs = {'in_features': dataset_path}
    else:
        add_function = arcpy.management.AddIndex
        add_kwargs = {
            'in_table': dataset_path, 'fields': field_names,
            'index_name': kwargs['index_name'],
            'unique': kwargs['is_unique'], 'ascending': kwargs['is_ascending']}
    try:
        add_function(**add_kwargs)
    except arcpy.ExecuteError as error:
        if all([kwargs['fail_on_lock_ok'],
                error.message.startswith('ERROR 000464')]):
            LOG.warning("Lock on %s prevents adding index.")
        else:
            raise
    LOG.log(log_level, "End: Add.")
    return dataset_path


@helpers.log_function
def delete_field(dataset_path, field_name, **kwargs):
    """Delete field from dataset.

    Args:
        dataset_path (str): Path of dataset.
        field_name (str): Name of field.
    Kwargs:
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    log_level = helpers.LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(
        log_level, "Start: Delete field %s on %s.", field_name, dataset_path)
    arcpy.management.DeleteField(in_table=dataset_path, drop_field=field_name)
    LOG.log(log_level, "End: Delete.")
    return field_name


@helpers.log_function
def duplicate_field(dataset_path, field_name, new_field_name, **kwargs):
    """Create new field as a duplicate of another.

    Args:
        dataset_path (str): Path of dataset.
        field_name (str): Name of field.
        new_field_name (str): Field name to call duplicate.
    Kwargs:
        duplicate_values (bool): Flag to indicate duplicating values.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('dataset_where_sql', None),
                          ('duplicate_values', False), ('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    log_level = helpers.LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Duplicate field %s as %s on %s.",
            field_name, new_field_name, dataset_path)
    field_meta = metadata.field_metadata(dataset_path, field_name)
    field_meta['name'] = new_field_name
    # Cannot add OID-type field, so push to a long-type.
    if field_meta['type'].lower() == 'oid':
        field_meta['type'] = 'long'
    add_fields_from_metadata_list(dataset_path, [field_meta], log_level=None)
    if kwargs['duplicate_values']:
        update_by_function(
            dataset_path, field_meta['name'], function=(lambda x: x),
            field_as_first_arg=False, arg_field_names=[field_name],
            dataset_where_sql=kwargs['dataset_where_sql'], log_level=None)
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
