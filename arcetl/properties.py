# -*- coding=utf-8 -*-
"""Property functions."""
import collections
import csv
import datetime
import inspect
import logging
import os
import uuid

import arcpy

import helpers


LOG = logging.getLogger(__name__)


@helpers.log_function
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


@helpers.log_function
def dataset_metadata(dataset_path):
    """Return dictionary of dataset metadata."""
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


@helpers.log_function
def feature_count(dataset_path, dataset_where_sql=None):
    """Return number of features in dataset."""
    with arcpy.da.SearchCursor(in_table=dataset_path, field_names=['oid@'],
                               where_clause=dataset_where_sql) as cursor:
        return len([None for row in cursor])


@helpers.log_function
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


@helpers.log_function
def is_valid_dataset(dataset_path):
    """Check whether dataset exists/is valid."""
    if dataset_path and arcpy.Exists(dataset_path):
        return dataset_metadata(dataset_path)['is_table']
    else:
        return False
