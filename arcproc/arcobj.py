"""Interfaces for ArcObjects."""
import datetime
import logging
import uuid

import arcpy

from arcproc import geometry
from arcproc.metadata import SpatialReference


LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""

arcpy.SetLogHistory(False)


def linear_unit(measure_string, spatial_reference_item):
    """Return linear unit of measure in reference units from string.

    Args:
        unit_string (str): String description of linear unit of measure.
        spatial_reference_item: Item from which the linear unit"s spatial
            reference will be derived.

    Returns:
        float: Unit of measure in spatial reference"s units.

    """
    str_measure, str_unit = measure_string.split(" ")
    reference_unit = getattr(
        SpatialReference(spatial_reference_item), "linear_unit", "Unknown"
    )
    meter_measure = float(str_measure) * geometry.RATIO["meter"][str_unit.lower()]
    measure = meter_measure / geometry.RATIO["meter"][reference_unit.lower()]
    return measure


def linear_unit_string(measure, spatial_reference_item):
    """Return linear unit of measure as a string description.

    Args:
        measure (float, int, str): Count of measure.
        spatial_reference_item: Item from which spatial reference for the linear unit
            will be derived.

    Returns:
        str.
    """
    reference_unit = getattr(
        SpatialReference(spatial_reference_item), "linear_unit", "Unknown"
    )
    return "{} {}".format(measure, reference_unit)


def python_type(type_description):
    """Return object representing the Python type.

    Args:
        type_description (str): Arc-style type description/code.

    Returns:
        Python object representing the type.
    """
    instance = {
        "date": datetime.datetime,
        "double": float,
        "single": float,
        "integer": int,
        "long": int,
        "short": int,
        "smallinteger": int,
        "geometry": arcpy.Geometry,
        "guid": uuid.UUID,
        "string": str,
        "text": str,
    }
    return instance[type_description.lower()]
