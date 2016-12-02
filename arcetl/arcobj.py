"""Interfaces for ArcObjects."""
import logging
import uuid

import arcpy

from arcetl import helpers


LOG = logging.getLogger(__name__)

FIELD_TYPE_AS_ARC = {'string': 'text', 'integer': 'long'}
FIELD_TYPE_AS_PYTHON = {
    'double': float, 'single': float,
    'integer': int, 'long': int, 'short': int, 'smallinteger': int,
    'guid': uuid.UUID,
    'string': str, 'text': str,
    }


class ArcExtension(object):
    """Context manager for an ArcGIS extension."""

    def __init__(self, name, activate_on_init=True):
        self.name = name
        # For now assume name & code are same.
        self.code = name
        self.activated = None
        self.result_activated_map = {'CheckedIn': False, 'CheckedOut': True,
                                     'Failed': False, 'NotInitialized': False,
                                     'Unavailable': False}
        self.result_log_level_map = {
            'CheckedIn': helpers.log_level('info'),
            'CheckedOut': helpers.log_level('info'),
            'Failed': helpers.log_level('warning'),
            'NotInitialized': helpers.log_level('warning'),
            'Unavailable': helpers.log_level('warning'),
            }
        self.result_log_message_map = {
            'CheckedIn': "{} extension deactivated.".format(self.code),
            'CheckedOut': "{} extension activated.".format(self.code),
            'NotInitialized': "No desktop license set.",
            'Unavailable': "Extension unavailable.",
            'Failed': "System failure."
            }
        if activate_on_init:
            self.activate()

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.deactivate()

    def _exec_activation(self, exec_function):
        """Execute extension (de)activation & return boolean of state."""
        result = exec_function(self.code)
        LOG.log(self.result_log_level_map.get(result, 0),
                self.result_log_message_map[result])
        return self.result_activated_map[result]

    def activate(self):
        """Activate extension."""
        self.activated = self._exec_activation(arcpy.CheckOutExtension)
        return self.activated

    def deactivate(self):
        """Deactivate extension."""
        self.activated = self._exec_activation(arcpy.CheckInExtension)
        return not self.activated


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
        'user_field_names': [], 'user_fields': [],
        }
    for field in getattr(describe_object, 'fields', ()):
        meta['field_names'].append(field.name)
        meta['fields'].append(field_as_metadata(field))
        if all([field.name != meta['oid_field_name'],
                '{}.'.format(meta['geometry_field_name']) not in field.name]):
            meta['user_field_names'].append(field.name)
            meta['user_fields'].append(field_as_metadata(field))
    if hasattr(describe_object, 'spatialReference'):
        meta['arc_spatial_reference'] = getattr(describe_object,
                                                'spatialReference')
        meta['spatial_reference_id'] = getattr(meta['arc_spatial_reference'],
                                               'factoryCode')
    else:
        meta['arc_spatial_reference'] = None
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
    if spatial_reference is None:
        arc_object = None
    elif isinstance(spatial_reference, int):
        arc_object = arcpy.SpatialReference(spatial_reference)
    elif isinstance(spatial_reference, arcpy.Geometry):
        arc_object = getattr(spatial_reference, 'spatialReference')
    else:
        arc_object = getattr(arcpy.Describe(spatial_reference),
                             'spatialReference')
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
        'is_geodatabase': any(['AccessWorkspace' in prog_id,
                               'FileGDBWorkspace' in prog_id,
                               'SdeWorkspace' in prog_id]),
        'is_folder': prog_id == '',
        'is_file_geodatabase': 'FileGDBWorkspace' in prog_id,
        'is_enterprise_database': 'SdeWorkspace' in prog_id,
        'is_personal_geodatabase': 'AccessWorkspace' in prog_id,
        'is_in_memory': 'InMemoryWorkspace' in prog_id,
        'domain_names': getattr(describe_object, 'domains', []),
        'arc_domains': [],
        'domains': [],
        }
    for domain_object in arcpy.da.ListDomains(meta['path']):
        meta['arc_domains'].append(domain_object)
        meta['domains'].append(domain_as_metadata(domain_object))
    return meta
