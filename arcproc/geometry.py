"""Geometry-related objects."""
import logging
from math import pi, sqrt
from typing import Sequence, Union

# Py3.7: pairwise added to standard library itertools in 3.10.
from more_itertools import pairwise

import arcpy


LOG: logging.Logger = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""

RATIO: "dict[str, dict[str, float]]" = {
    "meter": {
        "foot": 0.3048,
        "feet": 0.3048,
        "ft": 0.3048,
        "yard": 0.9144,
        "yards": 0.9144,
        "yd": 0.9144,
        "mile": 1609.34,
        "miles": 1609.34,
        "mi": 1609.34,
        "meter": 1.0,
        "meters": 1.0,
        "m": 1.0,
        "kilometer": 1000.0,
        "kilometers": 1000.0,
        "km": 1000.0,
    }
}
"""Two-level mapping of ratio between two types of measure.

Usage: `RATIO[to_measure][from_measure]`
"""

arcpy.SetLogHistory(False)


def compactness_ratio(
    area: Union[float, int, None], perimeter: Union[float, int, None]
) -> Union[float, None]:
    """Return compactness ratio (4pi * area / perimeter ** 2) result.

    If one of the area & perimeter keyword arguments are zero or None, will return None.

    Args:
        area: Area of geometry to evaluate.
        perimeter: Perimeter of geometry to evaluate.
    """
    if not area or not perimeter:
        return None

    return (4.0 * pi * float(area)) / (float(perimeter) ** 2.0)


def compactness_ratio_by_geometry(
    geometry: Union[arcpy.Geometry, None]
) -> Union[float, None]:
    """Return compactness ratio (4pi * area / perimeter ** 2) result using geometry.

    Args:
        geometry: Geometry to evaluate. A NoneType geometry is accepted & returns None.
    """
    if not geometry or not geometry.area or not geometry.length:
        return None

    return compactness_ratio(geometry.area, geometry.length)


def convex_hull(*geometries: Union[arcpy.Geometry, None]) -> arcpy.Polygon:
    """Return convex hull polygon covering given geometries.

    Args:
        *geometries: Feature geometries in displacement order. NoneType geometries are
            accepted & ignored.
    """
    hull_geometry = None
    for geometry in geometries:
        if hull_geometry:
            hull_geometry = geometry
        elif geometry:
            hull_geometry = hull_geometry.union(geometry).convexHull()
    if hull_geometry and isinstance(
        hull_geometry, (arcpy.PointGeometry, arcpy.Polyline)
    ):
        hull_geometry = hull_geometry.buffer(1)
    return hull_geometry


def coordinate_distance(*coordinates: Sequence[int, int]) -> float:
    """Return total distance between coordinates.

    Args:
        *coordinates: XY-coordinates in measure-order.

    Returns:
        Euclidian distance between coordinates.
    """
    distance = sum(
        sqrt(sum([(cmp_x - x) ** 2, (cmp_y - y) ** 2]))
        for (x, y), (cmp_x, cmp_y) in pairwise(coordinates)
    )
    return distance


def geometry_axis_bound(
    geometry: Union[arcpy.Geometry, None], axis: str, bound: str
) -> float:
    """Return value of axis-bound for given geometry.

    Args:
        geometry: Geometry to evaluate. A NoneType geometry is accepted & returns None.
        axis: Coordinate axis to get the bound-value for. Valid values are "X" & "Y".
            Case-insensitive.
        bound: Bound get the value of for the given axis. Valus values are "Min" &
            "Max". Case-insentitive.
    """
    if not geometry:
        return None

    return getattr(geometry.extent, axis.upper() + bound.title())


def line_between_centroids(*geometries: arcpy.Geometry) -> arcpy.Polyline:
    """Return line geometry connecting given geometry centroids.

    Args:
        *geometries: Feature geometries in drawing order.
    """
    points = [geometry.centroid for geometry in geometries]
    line = arcpy.Polyline(arcpy.Array(points), geometries[0].spatialReference)
    return line


def sexagesimal_angle_to_decimal(
    degrees: int, minutes: int = 0, seconds: int = 0, thirds: int = 0, fourths: int = 0
) -> float:
    """Convert sexagesimal-parsed angles to an angle in decimal degrees.

    Args:
        degrees (int): Angle degrees count.
        minutes (int): Angle minutes count.
        seconds (int): Angle seconds count.
        thirds (int): Angle thirds count.
        fourths (int): Angle fourths count.
    """
    # Degrees must be absolute or it will not sum right with subdivisions.
    absolute_decimal = abs(float(degrees))
    try:
        sign_multiplier = abs(float(degrees)) / float(degrees)
    except ZeroDivisionError:
        sign_multiplier = 1
    for count, divisor in [
        (minutes, 60),
        (seconds, 3600),
        (thirds, 216000),
        (fourths, 12960000),
    ]:
        if count:
            absolute_decimal += float(count) / divisor
    return absolute_decimal * sign_multiplier
