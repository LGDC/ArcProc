# -*- coding=utf-8 -*-
"""Interfaces for ArcObjects."""
import logging
import uuid


LOG = logging.getLogger(__name__)


FIELD_TYPE_AS_PYTHON = {
    'double': float, 'single': float,
    'integer': int, 'long': int, 'short': int, 'smallinteger': int,
    'guid': uuid.UUID,
    'string': str, 'text': str}


def domain_as_metadata(domain_object):
    """Return dictionary of domain metadata from an ArcPy domain object."""
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
        'type': getattr(domain_object, 'type'),
        }


def field_as_metadata(field_object):
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
        ##TODO: Get field metadata using functions to have defaults that work
        ## around this.
        #'default_value': getattr(field_object, 'defaultValue'),
        #'is_required': getattr(field_object, 'required'),
        #'is_editable': getattr(field_object, 'editable'),
        #'is_nullable': getattr(field_object, 'isNullable'),
        }


##TODO: geometry_as_map?


##TODO: spatial_reference_as_metadata.
