# -*- coding=utf-8 -*-
"""Interfaces for ArcObjects."""
import logging
import uuid


FIELD_TYPE_AS_ARC = {'string': 'text', 'integer': 'long'}
FIELD_TYPE_AS_PYTHON = {
    'double': float, 'single': float,
    'integer': int, 'long': int, 'short': int, 'smallinteger': int,
    'guid': uuid.UUID,
    'string': str, 'text': str}
LOG = logging.getLogger(__name__)


def dataset_as_metadata(describe_object):
    """Return dictionary of dataset metadata from an ArcPy describe object."""
    return {
        'name': getattr(describe_object, 'name'),
        'path': getattr(describe_object, 'catalogPath'),
        'data_type': getattr(describe_object, 'dataType'),
        'workspace_path': getattr(describe_object, 'path'),
        'is_table': hasattr(describe_object, 'hasOID'),
        'oid_field_name': getattr(describe_object, 'OIDFieldName', None),
        'field_names': [field.name for field
                        in getattr(describe_object, 'fields', [])],
        'fields': [field_as_metadata(field) for field
                   in getattr(describe_object, 'fields', [])],
        'is_spatial': hasattr(describe_object, 'shapeType'),
        'geometry_type': getattr(describe_object, 'shapeType', None),
        'spatial_reference_id': (
            getattr(describe_object, 'spatialReference').factoryCode
            if hasattr(describe_object, 'spatialReference') else None),
        'geometry_field_name': getattr(
            describe_object, 'shapeFieldName', None)}


def domain_as_metadata(domain_object):
    """Return dictionary of metadata from an ArcPy domain object."""
    return {
        'name': getattr(domain_object, 'name'),
        'description': getattr(domain_object, 'description'),
        'owner': getattr(domain_object, 'owner'),
        #'domain_type': getattr(domain_object, 'domainType'),
        'is_coded_value': getattr(domain_object, 'domainType') == 'CodedValue',
        'is_range': getattr(domain_object, 'domainType') == 'Range',
        #'merge_policy': getattr(domain_object, 'mergePolicy'),
        #'split_policy': getattr(domain_object, 'splitPolicy'),
        'code_description_map': getattr(domain_object, 'codedValues', {}),
        'range': getattr(domain_object, 'range', tuple()),
        'type': getattr(domain_object, 'type')}


def field_as_metadata(field_object):
    """Return dictionary of metadata from an ArcPy field object."""
    return {
        'name': getattr(field_object, 'name'),
        'alias_name': getattr(field_object, 'aliasName'),
        'base_name': getattr(field_object, 'baseName'),
        'type': getattr(field_object, 'type').lower(),
        'length': getattr(field_object, 'length'),
        'precision': getattr(field_object, 'precision'),
        'scale': getattr(field_object, 'scale')}


def spatial_reference_as_metadata(reference_object):
    """Return dictionary of metadata from an ArcPy spatial reference object."""
    ##TODO: Finish stub.
    ##http://desktop.arcgis.com/en/arcmap/latest/analyze/arcpy-classes/spatialreference.htm
    metadata = {}
    metadata['spatial_reference_id'] = reference_object.factoryCode
    metadata['angular_unit'] = getattr(
        reference_object, 'angularUnitName', None)
    metadata['linear_unit'] = getattr(reference_object, 'linearUnitName', None)
    return metadata
