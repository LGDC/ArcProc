# -*- coding=utf-8 -*-
"""Objects for dataset schema operations."""
import logging

import arcpy

from .. import helpers


LOG = logging.getLogger(__name__)
FIELD_TYPE_AS_ARC_TYPE_MAP = {'string': 'text', 'integer': 'long'}


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
def add_field(dataset_path, field_name, field_type, field_length=None,
              field_precision=None, field_scale=None, field_is_nullable=True,
              field_is_required=False, log_level='info'):
    """Add field to dataset."""
    logline = "Add field {}.{}.".format(dataset_path, field_name)
    helpers.log_line('start', logline, log_level)
    field_type = FIELD_TYPE_AS_ARC_TYPE_MAP.get(
        field_type, field_type)
    if field_type.lower() == 'text' and field_length is None:
        field_length = 64
    try:
        arcpy.management.AddField(
            dataset_path, field_name, field_type, field_length,
            field_precision, field_scale, field_is_nullable, field_is_required)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    helpers.log_line('end', logline, log_level)
    return field_name


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
