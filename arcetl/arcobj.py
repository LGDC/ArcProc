# -*- coding=utf-8 -*-
"""Interfaces for ArcObjects."""

import logging
import uuid

import arcpy


LOG = logging.getLogger(__name__)

FIELD_TYPE_AS_ARC = {'string': 'text', 'integer': 'long'}
FIELD_TYPE_AS_PYTHON = {
    'double': float, 'single': float,
    'integer': int, 'long': int, 'short': int, 'smallinteger': int,
    'guid': uuid.UUID,
    'string': str, 'text': str}


def dataset_as_metadata(describe_object):
    """Return dictionary of dataset metadata from an ArcPy describe object."""
    meta = {
        'name': getattr(describe_object, 'name'),
        'path': getattr(describe_object, 'catalogPath'),
        'data_type': getattr(describe_object, 'dataType'),
        'workspace_path': getattr(describe_object, 'path'),
        # Do not use getattr! Tables can not have OIDs.
        'is_table': hasattr(describe_object, 'hasOID'),
        'is_versioned': getattr(describe_object, 'isVersioned', False),
        'oid_field_name': getattr(describe_object, 'OIDFieldName', None),
        'is_spatial': hasattr(describe_object, 'shapeType'),
        'geometry_type': getattr(describe_object, 'shapeType', None),
        'geometry_field_name': getattr(describe_object, 'shapeFieldName', None),
        'field_names': [], 'fields': [],
        }
    for field in getattr(describe_object, 'fields', []):
        meta['field_names'].append(field.name)
        meta['fields'].append(field_as_metadata(field))
    if hasattr(describe_object, 'spatialReference'):
        meta['spatial_reference_id'] = getattr(describe_object,
                                               'spatialReference').factoryCode
    else:
        meta['spatial_reference_id'] = None
    return meta


def domain_as_metadata(domain_object):
    """Return dictionary of metadata from an ArcPy domain object."""
    meta = {
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
        'type': getattr(domain_object, 'type'),
        }
    return meta


def field_as_metadata(field_object):
    """Return dictionary of metadata from an ArcPy field object."""
    meta = {
        'name': getattr(field_object, 'name'),
        'alias_name': getattr(field_object, 'aliasName'),
        'base_name': getattr(field_object, 'baseName'),
        'type': getattr(field_object, 'type').lower(),
        'length': getattr(field_object, 'length'),
        'precision': getattr(field_object, 'precision'),
        'scale': getattr(field_object, 'scale'),
        }
    return meta


def linear_unit_as_string(measure, spatial_reference):
    """Return unit of measure as a linear unit string."""
    linear_unit = getattr(spatial_reference_as_arc(spatial_reference),
                          'linearUnitName', 'Unknown'),
    return '{} {}'.format(measure, linear_unit)


def spatial_reference_as_metadata(reference_object):
    """Return dictionary of metadata from an ArcPy spatial reference object."""
    ##TODO: Finish stub.
    ##https://pro.arcgis.com/en/pro-app/arcpy/classes/spatialreference.htm
    meta = {
        'spatial_reference_id': reference_object.factoryCode,
        'angular_unit': getattr(reference_object, 'angularUnitName', None),
        'linear_unit': getattr(reference_object, 'linearUnitName', None),
        }
    return meta


def spatial_reference_as_arc(spatial_reference):
    """Return ArcPy spatial reference object from a Python reference.

    Args:
        spatial_reference (int): Spatial reference ID.
                          (str): Path of reference dataset/file.
                          (arcpy.Geometry): Reference geometry object.
    Returns:
        arcpy.SpatialReference.
    """
    try:
        describe_object = arcpy.Describe(spatial_reference)
    except AttributeError:
        if spatial_reference is None:
            arc_object = None
        else:
            raise
    except IOError:
        if isinstance(spatial_reference, int):
            arc_object = arcpy.SpatialReference(spatial_reference)
        else:
            raise
    except RuntimeError:
        if isinstance(spatial_reference, arcpy.Geometry):
            arc_object = getattr(spatial_reference, 'spatialReference')
        else:
            raise
    else:
        arc_object = getattr(describe_object, 'spatialReference')
    return arc_object


def workspace_as_metadata(describe_object):
    """Return dictionary of workspace metadata from an ArcPy describe object."""
    ##TODO: Finish stub.
    ##http://pro.arcgis.com/en/pro-app/arcpy/functions/workspace-properties.htm
    prog_id = getattr(describe_object, 'workspaceFactoryProgID', '')
    meta = {
        'name': getattr(describe_object, 'name'),
        'path': getattr(describe_object, 'catalogPath'),
        'data_type': getattr(describe_object, 'dataType'),
        'domain_names': [], 'domains': [],
        'is_geodatabase': any(['AccessWorkspace' in prog_id,
                               'FileGDBWorkspace' in prog_id,
                               'SdeWorkspace' in prog_id]),
        'is_folder': prog_id == '',
        'is_file_geodatabase': 'FileGDBWorkspace' in prog_id,
        'is_enterprise_database': 'SdeWorkspace' in prog_id,
        'is_personal_geodatabase': 'AccessWorkspace' in prog_id,
        'is_in_memory': 'InMemoryWorkspace' in prog_id,
        'domain_names': getattr(describe_object, 'domains', []),
        }
    return meta
