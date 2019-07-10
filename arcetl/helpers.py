"""Internal module helper objects."""
try:
    from collections.abc import Iterable
except ImportError:
    from collections import Iterable
import datetime
import inspect
import logging
import math
import os
import random
import string
import sys
import uuid

from more_itertools import pairwise

import arcpy


if sys.version_info.major >= 3:
    basestring = str
    """Defining a basestring type instance for Py3+."""

if not hasattr(math, "isclose"):

    def isclose(a, b, rel_tol=1e-09, abs_tol=0.0):
        """Backporting Python 3.5+ `math.isclose()`."""
        return abs(a - b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)

    math.isclose = isclose

LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""

arcpy.SetLogHistory(False)


def contain(obj, nonetypes_as_empty=True):
    """Generate contained items if a collection, otherwise generate object.

    Args:
        obj: Any object, collection or otherwise.
        nontypes_as_empty (bool): If True `None` will be  treated as an empty
            collection; if False, they will be treated as an object to generate.

    Yields:
        obj or its contents.
    """
    if nonetypes_as_empty and obj is None:
        return

    if inspect.isgeneratorfunction(obj):
        obj = obj()
    if isinstance(obj, Iterable) and not isinstance(obj, basestring):
        for i in obj:
            yield i

    else:
        yield obj


def freeze_values(*values):
    """Generate "frozen" versions of mutable objects.

    Currently only freezes bytearrays to bytes.

    Args:
        values: Values to return.

    Yields:
        object: If a value is mutable, will yield value as immutable type of the value.
            Otherwise will yield as original value/type.
    """
    for val in values:
        if isinstance(val, bytearray):
            yield bytes(val)

        else:
            yield val


def leveled_logger(logger, level_repr=None):
    """Return logger function to log at the given level.

    Args:
        logger (logging.Logger): Logger to log to.
        level_repr: Representation of the logging level.

    Returns:
        function.
    """

    def _logger(msg, *args, **kwargs):
        return logger.log(log_level(level_repr), msg, *args, **kwargs)

    return _logger


def log_level(level_repr=None):
    """Return integer for logging module level.

    Args:
        level_repr: Representation of the logging level.

    Returns:
        int: Logging module level.

    """
    level = {
        None: 0,
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
    }
    if level_repr in level.values():
        result = level_repr
    elif level_repr is None:
        result = level[level_repr]
    elif isinstance(level_repr, basestring):
        result = level[level_repr.lower()]
    else:
        raise RuntimeError("level_repr invalid.")

    return result


def property_value(item, property_transform_map, *properties):
    """Return value of property via ordered item property tags.

    Args:
        item: Object to extract the property value from.
        property_transform_map (dict): Mapping of known substitute properties to a
            collection of the true properties they stand-in for.
        *properties: Collection of properties, ordered as they would be on the item
            itself (e.g. `item.property0.property1...`).

    Returns:
        Value of the property represented in the ordered properties.
    """
    if item is None:
        return None

    current_val = item
    for prop in properties:
        # Replace stand-ins with ordered properties.
        if isinstance(prop, basestring) and prop in property_transform_map:
            prop = property_transform_map.get(prop)
        if isinstance(prop, basestring):
            current_val = getattr(current_val, prop)
        elif isinstance(prop, Iterable):
            current_val = property_value(current_val, property_transform_map, *prop)
    return current_val


def same_feature(*features):
    """Determine whether feature representations are the same.

    Args:
        *features (iter of iter): Collection of features to compare.

    Returns:
        bool: True if same feature, False otherwise.
    """
    same = all(same_value(*vals) for pair in pairwise(features) for vals in zip(*pair))
    return same


def same_value(*values):
    """Determine whether values are the same.

    Notes:
        For datetime values, currently allows for a tolerance level of up to 10 ** -64.
        For geometry:
            Has not been tested on the following geometry types: multipoint, multipatch,
                dimension, annotation.
            Adding vertices that do not alter overall polygon shape do not appear to
                effect `geometry.equals()`.
            Adding those vertices does change `geometry.WKB` & `geometry.WKT`, so be
                aware that will make "different" values.
            Derived float values (e.g. geometry lengths & areas) can have slight
                differences between sources when they are essentially the same. Avoid
                comparisons between those.

    Args:
        *values (iter of iter): Collection of values to compare.

    Returns:
        bool: True if same value, False otherwise.
    """
    same = all(val1 == val2 for val1, val2 in pairwise(values))
    # Some types are not quite as simple.
    # Date-times & floats can have slight variations even when essentially the same.
    if all(isinstance(val, datetime.datetime) for val in values):
        same = all(
            abs(val1 - val2).total_seconds() < 10 ** -64
            for val1, val2 in pairwise(values)
        )
    if all(isinstance(val, float) for val in values):
        same = all(math.isclose(val1, val2) for val1, val2 in pairwise(values))
    # Geometry equality has extra considerations.
    elif all(isinstance(val, (arcpy.Geometry, arcpy.Point)) for val in values):
        same = all(val1.equals(val2) for val1, val2 in pairwise(values))
    return same


def unique_ids(data_type=uuid.UUID, string_length=4):
    """Generate unique IDs.

    Args:
        data_type: Type object to create unique IDs as.
        string_length (int): Length to make unique IDs of type string. Ignored if
            data_type is not a string type.

    Yields:
        Unique ID.
    """
    if data_type in [float, int]:
        # Skip 0 (problematic - some processing functions use 0 for null).
        unique_id = data_type(1)
        while True:
            yield unique_id

            unique_id += 1
    elif data_type in [uuid.UUID]:
        while True:
            yield uuid.uuid4()

    elif data_type in [str]:
        seed = string.ascii_letters + string.digits
        used_ids = set()
        while True:
            unique_id = "".join(random.choice(seed) for _ in range(string_length))
            if unique_id in used_ids:
                continue

            yield unique_id

            used_ids.add(unique_id)
    else:
        raise NotImplementedError(
            "Unique IDs for {} type not implemented.".format(data_type)
        )


def unique_name(prefix="", suffix="", unique_length=4, allow_initial_digit=True):
    """Generate unique name.

    Args:
        prefix (str): String to insert before the unique part of the name.
        suffix (str): String to append after the unique part of the name.
        unique_length (int): Number of unique characters to generate.
        allow_initial_number (bool): Flag indicating whether to let the initial
            character be a number. Default is True.

    Returns:
        str: Unique name.
    """
    name = prefix + next(unique_ids(str, unique_length)) + suffix
    if not allow_initial_digit and name[0].isdigit():
        name = unique_name(prefix, suffix, unique_length, allow_initial_digit)
    return name


def unique_path(prefix="", suffix="", unique_length=4, workspace_path="in_memory"):
    """Create unique temporary dataset path.

    Args:
        prefix (str): String to insert before the unique part of the name.
        suffix (str): String to append after the unique part of the name.
        unique_length (int): Number of unique characters to generate.
        workspace_path (str): Path of workspace to create the dataset in.

    Returns:
        str: Path of the created dataset.
    """
    name = unique_name(prefix, suffix, unique_length, allow_initial_digit=False)
    return os.path.join(workspace_path, name)
