"""Geometry-related objects."""
import logging
import math


LOG = logging.getLogger(__name__)

RATIO = {
    'meter': {
        'foot': 0.3048, 'feet': 0.3048, 'ft': 0.3048,
        'yard': 0.9144, 'yards': 0.9144, 'yd': 0.9144,
        'mile': 1609.34, 'miles': 1609.34, 'mi': 1609.34,
        'meter': 1.0, 'meters': 1.0, 'm': 1.0,
        'kilometer': 1000.0, 'kilometers': 1000.0, 'km': 1000.0,
        },
    }
"""dict: Two-level mapping of ratio between two types of measure.

Usage: `RATIO['to_measure']['from_mesure']`

"""


def compactness_ratio(geometry=None, **kwargs):
    """Return compactness ratio (4pi*area/perimeter**2) result.

    If geometry is NoneType or one of the area & perimeter keyword arguments
    are undefined/NoneType, will return NoneType.

    Args:
        geometry (arcpy.Geometry): Geometry to evaluate.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        area (float): Area of geometry to evaluate. Only used if geometry
            argument is NoneType.
        perimeter (float): Perimeter of geometry to evaluate. Only used if
            geometry argument  is NoneType.

    Returns:
        float: Ratio of compactness.

    """
    if geometry:
        dim = {'area': geometry.area, 'perimeter': geometry.length}
    elif kwargs.get('area') and kwargs.get('perimeter'):
        dim = {'area': kwargs['area'], 'perimeter': kwargs['perimeter']}
    else:
        dim = {}
    return (4*math.pi*dim['area'])/(dim['perimeter']**2) if dim else None


def coordinate_distance(*coordinates):
    """Return total distance between coordinates.

    Args:
        *coordinates: Variable length coordinate collection. Coordinates can
            be x,y or x,y,z.

    Returns:
        float: Euclidian distance between the coordinates.

    """
    distance = 0.0
    p_coord = {}
    for i, coord in enumerate(coordinates):
        coord = tuple(coord)
        n_coord = {'x': coord[0], 'y': coord[1],
                   'z': coord[3] if len(coord) == 3 else 0}
        if i != 0:
            distance += math.sqrt((n_coord['x'] - p_coord['x'])**2
                                  + (n_coord['y'] - p_coord['y'])**2
                                  + (n_coord['z'] - p_coord['z'])**2)
        p_coord = n_coord
    return distance


def sexagesimal_angle_to_decimal(degrees, minutes=0, seconds=0, thirds=0,
                                 fourths=0):
    """Convert sexagesimal-parsed angles to a decimal.

    Args:
        degrees (int): Angle degrees count.
        minutes (int): Angle minutes count.
        seconds (int): Angle seconds count.
        thirds (int): Angle thirds count.
        fourths (int): Angle fourths count.

    Returns:
        float: Angle in decimal degrees.
    """
    if degrees is None:
        return None
    # The degrees must be absolute or it won't sum right with subdivisions.
    absolute_decimal = abs(float(degrees))
    try:
        sign_multiplier = abs(float(degrees))/float(degrees)
    except ZeroDivisionError:
        sign_multiplier = 1
    for count, divisor in ((minutes, 60), (seconds, 3600), (thirds, 216000),
                           (fourths, 12960000)):
        if count:
            absolute_decimal += float(count)/divisor
    return absolute_decimal * sign_multiplier
