"""Geometry-related objects."""
from logging import Logger, getLogger
from math import pi, sqrt
from typing import Dict, Optional, Sequence, Union

from arcpy import Array, Geometry, PointGeometry, Polygon, Polyline, SetLogHistory
from more_itertools import pairwise


LOG: Logger = getLogger(__name__)
"""Module-level logger."""

SetLogHistory(False)

UNIT_PLURAL: Dict[str, str] = {"Foot": "Feet", "Meter": "Meters"}
"""Mapping of singular unit to plural.

Only need common ones from spatial references.
"""
UNIT_RATIO: Dict[str, Dict[str, float]] = {
    "Meter": {
        "Foot": 0.3048,
        "Feet": 0.3048,
        "Ft": 0.3048,
        "Yard": 0.9144,
        "Yards": 0.9144,
        "Yd": 0.9144,
        "Mile": 1609.34,
        "Miles": 1609.34,
        "Mi": 1609.34,
        "Meter": 1.0,
        "Meters": 1.0,
        "M": 1.0,
        "Kilometer": 1000.0,
        "Kilometers": 1000.0,
        "Km": 1000.0,
    }
}
"""Two-level mapping of ratio between two units of measure.

Usage: `UNIT_RATIO[to_measure][from_measure]`
"""


def angle_as_decimal(
    degrees: int, minutes: int = 0, seconds: int = 0, thirds: int = 0, fourths: int = 0
) -> float:
    """Convert sexagesimal-parsed angles to an angle in decimal degrees.

    Args:
        degrees: Angle degrees count.
        minutes: Angle minutes count.
        seconds: Angle seconds count.
        thirds: Angle thirds count.
        fourths: Angle fourths count.
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


def compactness_ratio(
    geometry: Optional[Geometry] = None,
    *,
    area: Optional[Union[float, int]] = None,
    perimeter: Optional[Union[float, int]] = None
) -> Union[float, None]:
    """Return compactness ratio for geometry: 4pi * area / perimeter ** 2.

    Notes:
        If geometry is None, will use area & perimeter arguments.
        If geometry is None & one of the area & perimeter arguments are zero or None,
            will return None.

    Args:
        geometry: Geometry to evaluate.
        area: Area of geometry to evaluate.
        perimeter: Perimeter of geometry to evaluate.
    """
    if geometry is not None:
        area = geometry.area
        perimeter = geometry.length
    if not area or not perimeter:
        return None

    return (4.0 * pi * float(area)) / (float(perimeter) ** 2.0)


def convex_hull(*geometries: Union[Geometry, None]) -> Polygon:
    """Return convex hull polygon geometry covering given geometries.

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
    if hull_geometry and isinstance(hull_geometry, (PointGeometry, Polyline)):
        hull_geometry = hull_geometry.buffer(1)
    return hull_geometry


def coordinate_distance(*coordinates: Sequence[int]) -> float:
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
    geometry: Union[Geometry, None], axis: str, bound: str
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


def line_between_centroids(*geometries: Geometry) -> Polyline:
    """Return line geometry connecting given geometry centroids.

    Args:
        *geometries: Feature geometries in drawing order.
    """
    points = [geometry.centroid for geometry in geometries]
    line = Polyline(Array(points), geometries[0].spatialReference)
    return line
