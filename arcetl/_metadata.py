# -*- coding=utf-8 -*-
"""Metadata objects."""
import logging

import arcpy

from arcetl import arcobj, dataset


LOG = logging.getLogger(__name__)


def linear_unit_as_string(measure, spatial_reference):
    """Return unit of measure as a linear unit string."""
    linear_unit = spatial_reference_metadata(spatial_reference)['linear_unit']
    # if measure != 1 and linear_unit == 'Foot':
    #     linear_unit = 'Feet'
    return '{} {}'.format(measure, linear_unit)


def spatial_reference_metadata(spatial_reference):
    """Return dictionary of spatial reference metadata.

    Args:
        spatial_reference (str): Path of dataset, or spatial reference ID.
    Returns:
        dict.
    """
    if isinstance(spatial_reference, int):
        reference_object = arcpy.SpatialReference(spatial_reference)
    elif dataset.is_valid(spatial_reference):
        reference_object = arcpy.Describe(spatial_reference).spatialReference
    meta = arcobj.spatial_reference_as_metadata(reference_object)
    return meta
