"""Interfaces for ArcObjects."""
import logging

from arcproc.metadata import SpatialReference


LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""


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
