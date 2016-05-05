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
    meta = {
        'description': "Adjust features in {} for shapefile output.".format(
            dataset_path),
        'dataset': metadata.dataset_metadata(dataset_path),
        'type_function_map': {
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
                       if x is None else x)}}
    helpers.log_line('start', meta['description'], kwargs['log_level'])
    for field in meta['dataset']['fields']:
        if field['type'].lower() in meta['type_function_map']:
            fields.update_field_by_function(
                dataset_path, field['name'],
                function=meta['type_function_map'][field['type'].lower()],
                log_level=None)
    helpers.log_line('end', meta['description'], kwargs['log_level'])
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
    for kwarg_default in [('dataset_where_sql', None), ('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    meta = {'description': "Delete features from {}.".format(dataset_path)}
    helpers.log_line('start', meta['description'], kwargs['log_level'])
    helpers.log_line(
        'feature_count', feature_count(dataset_path), kwargs['log_level'])
    result = arcwrap.delete_features(dataset_path, **kwargs)
    helpers.log_line(
        'feature_count', feature_count(dataset_path), kwargs['log_level'])
    helpers.log_line('end', meta['description'], kwargs['log_level'])
    return result


def feature_count(dataset_path, **kwargs):
    """Return number of features in dataset.

    Args:
        dataset_path (str): Path of dataset.
    Kwargs:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
    Returns:
        int.
    """
    kwargs.setdefault('dataset_where_sql', None)
    #pylint: disable=no-member
    with arcpy.da.SearchCursor(
        #pylint: enable=no-member
        in_table=dataset_path, field_names=['oid@'],
        where_clause=kwargs['dataset_where_sql']) as cursor:
        return len([None for _ in cursor])


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
    kwargs.setdefault('log_level', 'info')
    meta = {
        'description': "Insert features into {} from dictionaries.".format(
            dataset_path)}
    helpers.log_line('start', meta['description'], kwargs['log_level'])
    helpers.log_line(
        'feature_count', feature_count(dataset_path), kwargs['log_level'])
    if inspect.isgeneratorfunction(insert_features):
        insert_features = insert_features()
    #pylint: disable=no-member
    with arcpy.da.InsertCursor(dataset_path, field_names) as cursor:
        #pylint: enable=no-member
        for feature in insert_features:
            cursor.insertRow([feature[name] for name in field_names])
    helpers.log_line(
        'feature_count', feature_count(dataset_path), kwargs['log_level'])
    helpers.log_line('end', meta['description'], kwargs['log_level'])
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
    kwargs.setdefault('log_level', 'info')
    meta = {
        'description': "Insert features into {} from iterables.".format(
            dataset_path)}
    helpers.log_line('start', meta['description'], kwargs['log_level'])
    helpers.log_line(
        'feature_count', feature_count(dataset_path), kwargs['log_level'])
    if inspect.isgeneratorfunction(insert_features):
        insert_features = insert_features()
    #pylint: disable=no-member
    with arcpy.da.InsertCursor(dataset_path, field_names) as cursor:
        #pylint: enable=no-member
        for row in insert_features:
            cursor.insertRow(row)
    helpers.log_line(
        'feature_count', feature_count(dataset_path), kwargs['log_level'])
    helpers.log_line('end', meta['description'], kwargs['log_level'])
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
    meta = {
        'description': "Insert features into {} from {}.".format(
            dataset_path, insert_dataset_path),
        'dataset': metadata.dataset_metadata(dataset_path),
        'insert_dataset': metadata.dataset_metadata(insert_dataset_path),
        'field_maps': arcpy.FieldMappings()}
    meta['insert_dataset_view_name'] = arcwrap.create_dataset_view(
        helpers.unique_name('view'), insert_dataset_path,
        dataset_where_sql=kwargs['insert_where_sql'],
        # Insert view must be nonspatial to append to nonspatial table.
        force_nonspatial=(not meta['dataset']['is_spatial']))
    helpers.log_line('start', meta['description'], kwargs['log_level'])
    helpers.log_line(
        'feature_count', feature_count(dataset_path), kwargs['log_level'])
    # Create field maps.
    # Added because ArcGIS Pro's no-test append is case-sensitive (verified
    # 1.0-1.1.1). BUG-000090970 - ArcGIS Pro 'No test' field mapping in
    # Append tool does not auto-map to the same field name if naming
    # convention differs.
    if field_names:
        meta['field_names'] = [name.lower() for name in field_names]
    else:
        meta['field_names'] = [
            field['name'].lower() for field in meta['dataset']['fields']]
    meta['insert_field_names'] = [
        field['name'].lower() for field in meta['insert_dataset']['fields']]
    # Append takes care of geometry & OIDs independent of the field maps.
    for field_name_type in ('geometry_field_name', 'oid_field_name'):
        if meta['dataset'].get(field_name_type):
            meta['field_names'].remove(
                meta['dataset'][field_name_type].lower())
            meta['insert_field_names'].remove(
                meta['insert_dataset'][field_name_type].lower())
    for field_name in meta['field_names']:
        if field_name in meta['insert_field_names']:
            field_map = arcpy.FieldMap()
            field_map.addInputField(insert_dataset_path, field_name)
            meta['field_maps'].addFieldMap(field_map)
    try:
        arcpy.management.Append(
            inputs=meta['insert_dataset_view_name'], target=dataset_path,
            schema_type='no_test', field_mapping=meta['field_maps'])
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    arcpy.management.Delete(meta['insert_dataset_view_name'])
    helpers.log_line(
        'feature_count', feature_count(dataset_path), kwargs['log_level'])
    helpers.log_line('end', meta['description'], kwargs['log_level'])
    return dataset_path
