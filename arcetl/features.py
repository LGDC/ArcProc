# -*- coding=utf-8 -*-
"""Feature operations."""
import datetime
import inspect
import logging

import arcpy

from arcetl import arcwrap, attributes, helpers
from arcetl.metadata import dataset_metadata, feature_count


LOG = logging.getLogger(__name__)


def adjust_features_for_shapefile(dataset_path, **kwargs):
    """Adjust features to meet shapefile requirements.

    Adjustments currently made:
    * Convert datetime values to date or time based on
    preserve_time_not_date flag.
    * Convert nulls to replacement value.
    Args:
        dataset_path (str): Path of dataset.
    Kwargs:
        datetime_null_replacement (datetime.date): Replacement value for nulls
            in datetime fields.
        integer_null_replacement (int): Replacement value for nulls in integer
            fields.
        numeric_null_replacement (float): Replacement value for nulls in
            numeric fields.
        string_null_replacement (str): Replacement value for nulls in string
            fields.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [
            ('datetime_null_replacement', datetime.date.min),
            ('integer_null_replacement', 0), ('numeric_null_replacement', 0.0),
            ('string_null_replacement', ''), ('log_level', 'info')
        ]:
        kwargs.setdefault(*kwarg_default)
    log_level = helpers.LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Adjust features for shapefile output in %s.",
            dataset_path)
    dataset_meta = dataset_metadata(dataset_path)
    type_function_map = {
        # Invalid shapefile field types: 'blob', 'raster'.
        # Shapefiles can only store dates, not times.
        'date': (lambda x: kwargs['datetime_null_replacement']
                 if x is None else x.date()),
        'double': (lambda x: kwargs['numeric_null_replacement']
                   if x is None else x),
        #'geometry',  # Passed-through: Shapefile loader handles this.
        #'guid': Not valid shapefile type.
        'integer': (lambda x: kwargs['integer_null_replacement']
                    if x is None else x),
        #'oid',  # Passed-through: Shapefile loader handles this.
        'single': (lambda x: kwargs['numeric_null_replacement']
                   if x is None else x),
        'smallinteger': (lambda x: kwargs['integer_null_replacement']
                         if x is None else x),
        'string': (lambda x: kwargs['string_null_replacement']
                   if x is None else x)
        }
    with arcpy.da.Editor(dataset_meta['workspace_path']):
        for field in dataset_meta['fields']:
            if field['type'].lower() in type_function_map:
                attributes.update_by_function(
                    dataset_path, field['name'],
                    function=type_function_map[field['type'].lower()],
                    log_level=None
                    )
    LOG.log(log_level, "End: Adjust.")
    return dataset_path


def delete_features(dataset_path, **kwargs):
    """Delete select features.

    Wraps arcwrap.delete_features.

    Args:
        dataset_path (str): Path of dataset.
    Kwargs:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    # Other kwarg defaults set in the wrapped function.
    for kwarg_default in [('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    log_level = helpers.LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Delete features from %s.", dataset_path)
    LOG.log(log_level, "%s features in dataset.", feature_count(dataset_path))
    result = arcwrap.delete_features(dataset_path, **kwargs)
    LOG.log(log_level, "%s features in dataset.", feature_count(dataset_path))
    LOG.log(log_level, "End: Delete (%s features in dataset).")
    return result


def insert_features_from_dicts(dataset_path, insert_features, field_names,
                               **kwargs):
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
    log_level = helpers.LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Insert features from dictionaries into %s.",
            dataset_path)
    LOG.log(log_level, "%s features in dataset.", feature_count(dataset_path))
    if inspect.isgeneratorfunction(insert_features):
        insert_features = insert_features()
    dataset_meta = dataset_metadata(dataset_path)
    with arcpy.da.Editor(dataset_meta['workspace_path']):
        with arcpy.da.InsertCursor(dataset_path, field_names) as cursor:
            for feature in insert_features:
                cursor.insertRow([feature[name] for name in field_names])
    LOG.log(log_level, "%s features in dataset.", feature_count(dataset_path))
    LOG.log(log_level, "End: Insert.")
    return dataset_path


def insert_features_from_iters(dataset_path, insert_features, field_names,
                               **kwargs):
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
    log_level = helpers.LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Insert features from iterables into %s.",
            dataset_path)
    LOG.log(log_level, "%s features in dataset.", feature_count(dataset_path))
    if inspect.isgeneratorfunction(insert_features):
        insert_features = insert_features()
    dataset_meta = dataset_metadata(dataset_path)
    with arcpy.da.Editor(dataset_meta['workspace_path']):
        with arcpy.da.InsertCursor(dataset_path, field_names) as cursor:
            for row in insert_features:
                cursor.insertRow(row)
    LOG.log(log_level, "%s features in dataset.", feature_count(dataset_path))
    LOG.log(log_level, "End: Insert.")
    return dataset_path


@helpers.log_function
def insert_features_from_path(dataset_path, insert_dataset_path,
                              field_names=None, **kwargs):
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
    # Other kwarg defaults set in the wrapped function.
    for kwarg_default in [('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    log_level = helpers.LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Insert features from dataset path %s into %s.",
            insert_dataset_path, dataset_path)
    LOG.log(log_level, "%s features in dataset.", feature_count(dataset_path))
    result = arcwrap.insert_features_from_path(
        dataset_path, insert_dataset_path, field_names, **kwargs
        )
    LOG.log(log_level, "%s features in dataset.", feature_count(dataset_path))
    LOG.log(log_level, "End: Insert.")
    return result
