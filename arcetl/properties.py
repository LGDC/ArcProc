# -*- coding=utf-8 -*-
"""Data property objects."""
import logging

import arcpy


LOG = logging.getLogger(__name__)


##TODO: Move to helpers.py.
def _arc_field_object_as_metadata(field_object):
    """Return dictionary of field metadata from an ArcPy field object."""
    return {
        'name': getattr(field_object, 'name'),
        'alias_name': getattr(field_object, 'aliasName'),
        'base_name': getattr(field_object, 'baseName'),
        'type': getattr(field_object, 'type').lower(),
        'length': getattr(field_object, 'length'),
        'precision': getattr(field_object, 'precision'),
        'scale': getattr(field_object, 'scale'),
        # Leaving out certain field properties which aren't
        # necessary for ETL and are often problematic.
        #'default_value': getattr(field_object, 'defaultValue'),
        #'is_required': getattr(field_object, 'required'),
        #'is_editable': getattr(field_object, 'editable'),
        #'is_nullable': getattr(field_object, 'isNullable'),
        }


def dataset_metadata(dataset_path):
    """Return dictionary of dataset metadata.

    Args:
        dataset_path (str): Path of dataset.
    Returns:
        dict.
    """
    description = arcpy.Describe(dataset_path)
    return {
        'name': getattr(description, 'name'),
        'path': getattr(description, 'catalogPath'),
        'data_type': getattr(description, 'dataType'),
        'workspace_path': getattr(description, 'path'),
        'is_table': hasattr(description, 'hasOID'),
        'oid_field_name': getattr(description, 'OIDFieldName', None),
        'field_names': [field.name for field
                        in getattr(description, 'fields', [])],
        'fields': [_arc_field_object_as_metadata(field) for field
                   in getattr(description, 'fields', [])],
        'is_spatial': hasattr(description, 'shapeType'),
        'geometry_type': getattr(description, 'shapeType', None),
        'spatial_reference_id': (
            getattr(description, 'spatialReference').factoryCode
            if hasattr(description, 'spatialReference') else None),
        'geometry_field_name': getattr(description, 'shapeFieldName', None),
        }


def field_metadata(dataset_path, field_name):
    """Return dictionary of field metadata.

    Field name is case-insensitive.
    """
    try:
        return _arc_field_object_as_metadata(
            arcpy.ListFields(dataset=dataset_path, wild_card=field_name)[0])
    except IndexError:
        raise AttributeError(
            "Field {} not present on {}".format(field_name, dataset_path))


def field_values(dataset_path, field_names, **kwargs):
    """Generator for tuples of feature field values.

    Args:
        dataset_path (str): Path of dataset.
        field_names (iter): Iterable of field names.
    Kwargs:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        spatial_reference_id (int): EPSG code indicating the spatial reference
            output geometry will be in.
    Yields:
        tuple.
    """
    kwargs.setdefault('dataset_where_sql', None)
    kwargs.setdefault('spatial_reference_id', None)
    kwargs['spatial_reference'] = (
        arcpy.SpatialReference(kwargs['spatial_reference_id'])
        if kwargs.get('spatial_reference_id') else None)
    #pylint: disable=no-member
    with arcpy.da.SearchCursor(
        #pylint: enable=no-member
        dataset_path, field_names, kwargs['dataset_where_sql'],
        spatial_reference=kwargs['spatial_reference']) as cursor:
        for values in cursor:
            yield values


def is_valid_dataset(dataset_path):
    """Check whether dataset exists/is valid."""
    return (dataset_path is not None and arcpy.Exists(dataset_path)
            and dataset_metadata(dataset_path)['is_table'])


def oid_field_value(dataset_path, field_name, **kwargs):
    """Generator for tuples of (OID, field_value).

    Args:
        dataset_path (str): Path of dataset.
        field_name (iter): Name of field.
    Kwargs:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        spatial_reference_id (int): EPSG code indicating the spatial reference
            output geometry will be in.
    Yields:
        tuple.
    """
    kwargs.setdefault('dataset_where_sql', None)
    kwargs.setdefault('spatial_reference_id', None)
    for oid, value in field_values(
            dataset_path, ['oid@', field_name],
            dataset_where_sql=kwargs['dataset_where_sql'],
            spatial_reference_id=kwargs['spatial_reference_id']):
        yield (oid, value)


def oid_field_value_map(dataset_path, field_name, **kwargs):
    """Return dictionary mapping of field value for the feature OID.

    Args:
        dataset_path (str): Path of dataset.
        field_name (str): Name of field.
    Kwargs:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        spatial_reference_id (int): EPSG code indicating the spatial reference
            output geometry will be in.
    Returns:
        dict.
    """
    kwargs.setdefault('dataset_where_sql', None)
    kwargs.setdefault('spatial_reference_id', None)
    return {
        oid: value for oid, value in oid_field_value(
            dataset_path, field_name,
            dataset_where_sql=kwargs['dataset_where_sql'],
            spatial_reference_id=kwargs['spatial_reference_id'])}


def oid_geometry(dataset_path, **kwargs):
    """Generator for tuples of (OID, geometry).

    Args:
        dataset_path (str): Path of dataset.
    Kwargs:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        spatial_reference_id (int): EPSG code indicating the spatial reference
            output geometry will be in.
    Yields:
        tuple.
    """
    kwargs.setdefault('dataset_where_sql', None)
    kwargs.setdefault('spatial_reference_id', None)
    for oid, value in oid_field_value(
            dataset_path, 'shape@',
            dataset_where_sql=kwargs['dataset_where_sql'],
            spatial_reference_id=kwargs['spatial_reference_id']):
        yield (oid, value)


def oid_geometry_map(dataset_path, **kwargs):
    """Return dictionary mapping of geometry for the feature OID.

    Args:
        dataset_path (str): Path of dataset.
    Kwargs:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        spatial_reference_id (int): EPSG code indicating the spatial reference
            output geometry will be in.
    Returns:
        dict.
    """
    kwargs.setdefault('dataset_where_sql', None)
    kwargs.setdefault('spatial_reference_id', None)
    return oid_field_value_map(
        dataset_path, 'shape@', dataset_where_sql=kwargs['dataset_where_sql'],
        spatial_reference_id=kwargs['spatial_reference_id'])


def workspace_dataset_names(workspace_path, **kwargs):
    """Generator for workspace's dataset names.

    wildcard requires an * to indicate where open; case insensitive.

    Args:
        dataset_path (str): Path of dataset.
        field_name (str): Name of field.
    Kwargs:
        wildcard (str): String to indicate wildcard search.
        include_feature_classes (bool): Flag to include feature class datasets.
        include_rasters (bool): Flag to include raster datasets.
        include_tables (bool): Flag to include nonspatial tables.
        include_feature_datasets (bool): Flag to include contents of feature
            datasets.
    Yields:
        str.
    """
    for kwarg_default in [
            ('include_feature_classes', True),
            ('include_feature_datasets', True), ('include_rasters', True),
            ('include_tables', True), ('wildcard', None)]:
        kwargs.setdefault(*kwarg_default)
    old_workspace_path = arcpy.env.workspace
    arcpy.env.workspace = workspace_path
    dataset_names = []
    if kwargs['include_feature_classes']:
        # None-value represents the root level.
        feature_dataset_names = [None]
        if kwargs['include_feature_datasets']:
            feature_dataset_names.extend(arcpy.ListDatasets())
        for name in feature_dataset_names:
            dataset_names.extend(
                arcpy.ListFeatureClasses(
                    kwargs['wildcard'], feature_dataset=name))
    if kwargs['include_rasters']:
        dataset_names.extend(arcpy.ListRasters(kwargs['wildcard']))
    if kwargs['include_tables']:
        dataset_names.extend(arcpy.ListTables(kwargs['wildcard']))
    arcpy.env.workspace = old_workspace_path
    return dataset_names
