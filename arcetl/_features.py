# -*- coding=utf-8 -*-
"""Feature operations."""
import inspect
import logging

import arcpy

from arcetl import dataset
from arcetl.helpers import LOG_LEVEL_MAP, unique_name


LOG = logging.getLogger(__name__)


def count(dataset_path, **kwargs):
    """Return number of features in dataset.

    Wraps dataset.feature_count.

    Args:
        dataset_path (str): Path of dataset.
    Kwargs:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
    Returns:
        int.
    """
    return dataset.feature_count(dataset_path, **kwargs)


def delete(dataset_path, **kwargs):
    """Delete features.

    Args:
        dataset_path (str): Path of dataset.
    Kwargs:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('dataset_where_sql', None), ('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Delete features from %s.", dataset_path)
    truncate_type_error_codes = (
        # "Only supports Geodatabase tables and feature classes."
        'ERROR 000187',
        # "Operation not supported on a versioned table."
        'ERROR 001259',
        # "Operation not supported on table {table name}."
        'ERROR 001260',
        # Operation not supported on a feature class in a controller
        # dataset.
        'ERROR 001395'
        )
    dataset_view_name = dataset.create_view(
        unique_name('view'), dataset_path,
        dataset_where_sql=kwargs['dataset_where_sql'], log_level=None
        )
    # Can use (faster) truncate when no sub-selection
    run_truncate = kwargs.get('dataset_where_sql') is None
    if run_truncate:
        try:
            arcpy.management.TruncateTable(in_table=dataset_view_name)
        except arcpy.ExecuteError:
            # Avoid arcpy.GetReturnCode(); error code position inconsistent.
            # Search messages for 'ERROR ######' instead.
            if any(code in arcpy.GetMessages()
                   for code in truncate_type_error_codes):
                LOG.debug("Truncate unsupported; will try deleting rows.")
                run_truncate = False
            else:
                raise
    if not run_truncate:
        arcpy.management.DeleteRows(in_rows=dataset_view_name)
    dataset.delete(dataset_view_name, log_level=None)
    LOG.log(log_level, "End: Delete.")
    return dataset_path


def insert_from_dicts(dataset_path, insert_features, field_names, **kwargs):
    """Insert features from a collection of dictionaries.

    Args:
        dataset_path (str): Path of dataset.
        insert_features (iter): Iterable containing dictionaries representing
            features.
        field_names (iter): Iterable of field names to insert.
    Kwargs:
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Insert features from dictionaries into %s.",
            dataset_path)
    if inspect.isgeneratorfunction(insert_features):
        insert_features = insert_features()
    with arcpy.da.InsertCursor(dataset_path, field_names) as cursor:
        for feature in insert_features:
            cursor.insertRow([feature[name] for name in field_names])
    LOG.log(log_level, "End: Insert.")
    return dataset_path


def insert_from_iters(dataset_path, insert_features, field_names, **kwargs):
    """Insert features from a collection of iterables.

    Args:
        dataset_path (str): Path of dataset.
        insert_features (iter): Iterable containing iterables representing
            features.
        field_names (iter): Iterable of field names to insert.
    Kwargs:
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Insert features from iterables into %s.",
            dataset_path)
    if inspect.isgeneratorfunction(insert_features):
        insert_features = insert_features()
    with arcpy.da.InsertCursor(dataset_path, field_names) as cursor:
        for row in insert_features:
            cursor.insertRow(row)
    LOG.log(log_level, "End: Insert.")
    return dataset_path


def insert_from_path(dataset_path, insert_dataset_path, field_names=None,
                     **kwargs):
    """Insert features from a dataset referred to by a system path.

    Args:
        dataset_path (str): Path of dataset.
        insert_dataset_path (str): Path of insert-dataset.
        field_names (iter): Iterable of field names to insert.
    Kwargs:
        insert_where_sql (str): SQL where-clause for insert-dataset
            subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('insert_where_sql', None), ('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Insert features from dataset path %s into %s.",
            insert_dataset_path, dataset_path)
    dataset_meta = dataset.metadata(dataset_path)
    insert_dataset_meta = dataset.metadata(insert_dataset_path)
    insert_dataset_view_name = dataset.create_view(
        unique_name('view'), insert_dataset_path,
        dataset_where_sql=kwargs['insert_where_sql'],
        # Insert view must be nonspatial to append to nonspatial table.
        force_nonspatial=(not dataset_meta['is_spatial']), log_level=None
        )
    # Create field maps.
    # Added because ArcGIS Pro's no-test append is case-sensitive (verified
    # 1.0-1.1.1). BUG-000090970 - ArcGIS Pro 'No test' field mapping in
    # Append tool does not auto-map to the same field name if naming
    # convention differs.
    if field_names:
        field_names = [name.lower() for name in field_names]
    else:
        field_names = [field['name'].lower()
                       for field in dataset_meta['fields']]
    insert_field_names = [field['name'].lower()
                          for field in insert_dataset_meta['fields']]
    # Append takes care of geometry & OIDs independent of the field maps.
    for field_name_type in ['geometry_field_name', 'oid_field_name']:
        if dataset_meta.get(field_name_type):
            field_names.remove(dataset_meta[field_name_type].lower())
            insert_field_names.remove(
                insert_dataset_meta[field_name_type].lower()
                )
    field_maps = arcpy.FieldMappings()
    for field_name in field_names:
        if field_name in insert_field_names:
            field_map = arcpy.FieldMap()
            field_map.addInputField(insert_dataset_path, field_name)
            field_maps.addFieldMap(field_map)
    arcpy.management.Append(
        inputs=insert_dataset_view_name, target=dataset_path,
        schema_type='no_test', field_mapping=field_maps
        )
    dataset.delete(insert_dataset_view_name, log_level=None)
    LOG.log(log_level, "End: Insert.")
    return dataset_path
