# -*- coding=utf-8 -*-
"""Feature operations."""
import datetime
import inspect
import logging

import arcpy

from . import arcwrap, fields, helpers, metadata


LOG = logging.getLogger(__name__)


@helpers.log_function
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
            ('string_null_replacement', ''), ('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    log_level = helpers.LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Adjust features in %s for shapefile output.",
            dataset_path)
    dataset_meta = metadata.dataset_metadata(dataset_path)
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
                   if x is None else x)}
    for field in dataset_meta['fields']:
        if field['type'].lower() in type_function_map:
            fields.update_field_by_function(
                dataset_path, field['name'],
                function=type_function_map[field['type'].lower()],
                log_level=None)
    LOG.log(log_level, "End: Adjust.")
    return dataset_path


@helpers.log_function
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
    LOG.log(log_level, "%s features.", metadata.feature_count(dataset_path))
    result = arcwrap.delete_features(dataset_path, **kwargs)
    LOG.log(log_level, "End: Delete.")
    LOG.log(log_level, "%s features.", metadata.feature_count(dataset_path))
    return result


@helpers.log_function
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
    LOG.log(log_level, "Start: Insert features into %s from dictionaries.",
            dataset_path)
    LOG.log(log_level, "%s features.", metadata.feature_count(dataset_path))
    if inspect.isgeneratorfunction(insert_features):
        insert_features = insert_features()
    #pylint: disable=no-member
    with arcpy.da.InsertCursor(dataset_path, field_names) as cursor:
        #pylint: enable=no-member
        for feature in insert_features:
            cursor.insertRow([feature[name] for name in field_names])
    LOG.log(log_level, "End: Insert.")
    LOG.log(log_level, "%s features.", metadata.feature_count(dataset_path))
    return dataset_path


@helpers.log_function
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
    LOG.log(log_level, "Start: Insert features into %s from iterables.",
            dataset_path)
    LOG.log(log_level, "%s features.", metadata.feature_count(dataset_path))
    if inspect.isgeneratorfunction(insert_features):
        insert_features = insert_features()
    #pylint: disable=no-member
    with arcpy.da.InsertCursor(dataset_path, field_names) as cursor:
        #pylint: enable=no-member
        for row in insert_features:
            cursor.insertRow(row)
    LOG.log(log_level, "End: Insert.")
    LOG.log(log_level, "%s features.", metadata.feature_count(dataset_path))
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
    for kwarg_default in [('insert_where_sql', None), ('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    log_level = helpers.LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Insert features into %s from %s.",
            dataset_path, insert_dataset_path)
    LOG.log(log_level, "%s features.", metadata.feature_count(dataset_path))
    dataset_meta = metadata.dataset_metadata(dataset_path)
    insert_dataset_meta = metadata.dataset_metadata(insert_dataset_path)
    insert_dataset_view_name = arcwrap.create_dataset_view(
        helpers.unique_name('view'), insert_dataset_path,
        dataset_where_sql=kwargs['insert_where_sql'],
        # Insert view must be nonspatial to append to nonspatial table.
        force_nonspatial=(not dataset_meta['is_spatial']))
    # Create field maps.
    # Added because ArcGIS Pro's no-test append is case-sensitive (verified
    # 1.0-1.1.1). BUG-000090970 - ArcGIS Pro 'No test' field mapping in
    # Append tool does not auto-map to the same field name if naming
    # convention differs.
    if field_names:
        field_names = [name.lower() for name in field_names]
    else:
        field_names = [
            field['name'].lower() for field in dataset_meta['fields']]
    insert_field_names = [
        field['name'].lower() for field in insert_dataset_meta['fields']]
    # Append takes care of geometry & OIDs independent of the field maps.
    for field_name_type in ('geometry_field_name', 'oid_field_name'):
        if dataset_meta.get(field_name_type):
            field_names.remove(dataset_meta[field_name_type].lower())
            insert_field_names.remove(
                insert_dataset_meta[field_name_type].lower())
    field_maps = arcpy.FieldMappings()
    for field_name in field_names:
        if field_name in insert_field_names:
            field_map = arcpy.FieldMap()
            field_map.addInputField(insert_dataset_path, field_name)
            field_maps.addFieldMap(field_map)
    try:
        arcpy.management.Append(
            inputs=insert_dataset_view_name, target=dataset_path,
            schema_type='no_test', field_mapping=field_maps)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    arcpy.management.Delete(insert_dataset_view_name)
    LOG.log(log_level, "End: Insert.")
    LOG.log(log_level, "%s features.", metadata.feature_count(dataset_path))
    return dataset_path
