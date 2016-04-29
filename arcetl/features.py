# -*- coding=utf-8 -*-
"""Module objects for feature reference & manipulation."""
import datetime
import inspect
import logging

import arcpy

from . import attributes, helpers, operations, properties


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
    _description = "Adjust features in {} for shapefile output.".format(
        dataset_path)
    helpers.log_line('start', _description, kwargs['log_level'])
    helpers.log_line(
        'feature_count', feature_count(dataset_path), kwargs['log_level'])
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
                   if x is None else x),
        }
    for field in properties.dataset_metadata(dataset_path)['fields']:
        if field['type'].lower() in type_function_map:
            attributes.update_field_by_function(
                dataset_path, field['name'],
                function=type_function_map[field['type'].lower()],
                log_level=None)
    helpers.log_line(
        'feature_count', feature_count(dataset_path), kwargs['log_level'])
    helpers.log_line('end', _description, kwargs['log_level'])
    return dataset_path


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
    _description = "Insert features into {} from dictionaries.".format(
        dataset_path)
    helpers.log_line('start', _description, kwargs['log_level'])
    helpers.log_line(
        'feature_count', feature_count(dataset_path), kwargs['log_level'])
    if inspect.isgeneratorfunction(insert_features):
        insert_features = insert_features()
    #pylint: disable=no-member
    with arcpy.da.InsertCursor(dataset_path, field_names) as cursor:
    #pylint: enable=no-member
        for _feature in insert_features:
            cursor.insertRow([_feature[name] for name in field_names])
    helpers.log_line(
        'feature_count', feature_count(dataset_path), kwargs['log_level'])
    helpers.log_line('end', _description, kwargs['log_level'])
    return dataset_path


##TODO: Rename insert_features_from_iters.
@helpers.log_function
def insert_features_from_iterables(dataset_path, insert_features, field_names,
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
    _description = "Insert features into {} from iterables.".format(
        dataset_path)
    helpers.log_line('start', _description, kwargs['log_level'])
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
    helpers.log_line('end', _description, kwargs['log_level'])
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
    kwargs.setdefault('insert_where_sql', None)
    kwargs.setdefault('log_level', 'info')
    _description = "Insert features into {} from {}.".format(
        dataset_path, insert_dataset_path)
    helpers.log_line('start', _description, kwargs['log_level'])
    helpers.log_line(
        'feature_count', feature_count(dataset_path), kwargs['log_level'])
    meta = {'dataset': properties.dataset_metadata(dataset_path),
            'insert_dataset': properties.dataset_metadata(insert_dataset_path)}
    # Create field maps.
    # Added because ArcGIS Pro's no-test append is case-sensitive (verified
    # 1.0-1.1.1). BUG-000090970 - ArcGIS Pro 'No test' field mapping in
    # Append tool does not auto-map to the same field name if naming
    # convention differs.
    if field_names:
        _field_names = [name.lower() for name in field_names]
    else:
        _field_names = [field['name'].lower()
                        for field in meta['dataset']['fields']]
    insert_field_names = [
        field['name'].lower() for field in meta['insert_dataset']['fields']]
    # Append takes care of geometry & OIDs independent of the field maps.
    for field_name_type in ('geometry_field_name', 'oid_field_name'):
        if meta['dataset'].get(field_name_type):
            _field_names.remove(meta['dataset'][field_name_type].lower())
            insert_field_names.remove(
                meta['insert_dataset'][field_name_type].lower())
    field_maps = arcpy.FieldMappings()
    for field_name in _field_names:
        if field_name in insert_field_names:
            field_map = arcpy.FieldMap()
            field_map.addInputField(insert_dataset_path, field_name)
            field_maps.addFieldMap(field_map)
    insert_dataset_view_name = operations.create_dataset_view(
        helpers.unique_name('insert_dataset_view'), insert_dataset_path,
        dataset_where_sql=kwargs['insert_where_sql'],
        # Insert view must be nonspatial to append to nonspatial table.
        force_nonspatial=(not meta['dataset']['is_spatial']), log_level=None)
    try:
        arcpy.management.Append(
            inputs=insert_dataset_view_name, target=dataset_path,
            schema_type='no_test', field_mapping=field_maps)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    operations.delete_dataset(insert_dataset_view_name, log_level=None)
    helpers.log_line(
        'feature_count', feature_count(dataset_path), kwargs['log_level'])
    helpers.log_line('end', _description, kwargs['log_level'])
    return dataset_path
