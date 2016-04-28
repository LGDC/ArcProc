# -*- coding=utf-8 -*-
"""Data property objects."""
import logging


LOG = logging.getLogger(__name__)


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
        #'default_value': getattr(field_object, 'defaultValue'),
        #'is_required': getattr(field_object, 'required'),
        #'is_editable': getattr(field_object, 'editable'),
        #'is_nullable': getattr(field_object, 'isNullable'),
        }

