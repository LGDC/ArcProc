"""Internal module helper objects."""
try:
    from collections.abc import Iterable
except ImportError:
    # Py2.
    from collections import Iterable
import datetime
import inspect
import logging
import math
import random
from pathlib import Path
import string
import sys
import uuid

# Py3.7: pairwise added to standard library itertools in 3.10.
from more_itertools import pairwise

import arcpy


# Py2.
if sys.version_info.major >= 3:
    basestring = str
    """Defining a basestring type instance for Py3+."""

# Py2.
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


def elapsed(start_time, logger=None, log_level=logging.INFO):
    """Return time-delta since start time.

    Args:
        start_time (datetime.datetime): Start to measure time elapsed since.
        logger (logging.Logger, None): If not None, logger to emit elapsed message.
        log_level (int): Level to emit elapsed message at.

    Returns:
        datetime.timedelta
    """
    span = datetime.datetime.now() - start_time
    if logger:
        logger.log(
            log_level,
            "Elapsed: %s hrs, %s min, %s sec.",
            (span.days * 24 + span.seconds // 3600),
            ((span.seconds // 60) % 60),
            (span.seconds % 60),
        )
    return span


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


def log_entity_states(entity_type, states, logger=None, **kwargs):
    """Log the counts for entities in each state from provided counter.

    Args:
        entity_type (str): Label for the entity type whose states are counted.
            Preferably plural, e.g. "datasets".
        states (collections.Counter): State-counts.
        logger (logging.Logger): Loger to handle emitted loglines.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        fmt (str): Format-string for logline. Use keywords in default value (`state` &
            `count` are the key & value of a single item in `states`). Default is
            "{count} {entity_type} {state}."
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).
    """
    if not logger:
        logger = LOG
    level = kwargs.get("log_level", logging.INFO)
    if sum(states.values()) == 0:
        logger.log(level, "No %s states to log.", entity_type)
    else:
        for state, count in sorted(states.items()):
            line = kwargs.get("fmt", "{count} {entity_type} {state}.").format(
                count=count, entity_type=entity_type, state=state
            )
            logger.log(level, line)


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
    if all(isinstance(val, datetime.datetime) for val in values):
        # Microsecond rounding occurs differently on different data formats. Ignore.
        same = all(
            getattr(val1, attr) == getattr(val2, attr)
            for val1, val2 in pairwise(values)
            for attr in ["year", "month", "day", "hour", "minute", "second"]
        )
    elif all(isinstance(val, float) for val in values):
        same = all(math.isclose(val1, val2) for val1, val2 in pairwise(values))
    # Geometry equality has extra considerations.
    elif all(isinstance(val, (arcpy.Geometry, arcpy.Point)) for val in values):
        same = all(val1.equals(val2) for val1, val2 in pairwise(values))
    else:
        same = all(val1 == val2 for val1, val2 in pairwise(values))
    return same


def slugify(text, separator="-", force_lowercase=True):
    """Return text in slug-form.

    Args:
        text (str): String to slugify.
        separator (str): String to use as separator.

    Returns:
        str
    """
    slug = text.lower() if force_lowercase else text
    for char in string.punctuation + string.whitespace:
        slug = slug.replace(char, separator)
    while separator * 2 in slug:
        slug = slug.replace(separator * 2, separator)
    while slug[-1] == separator:
        slug = slug[:-1]
    return slug


def unique_ids(data_type=uuid.UUID, string_length=4, initial_number=1):
    """Generate unique IDs.

    Args:
        data_type: Type object to create unique IDs as.
        string_length (int): Length to make unique IDs of type string. Ignored if
            data_type is not a string type.
        initial_number (int): Initial number for a proposed ID, if using a numeric data
            type. Default is 1.

    Yields:
        Unique ID.
    """
    if data_type in [float, int]:
        unique_id = data_type(initial_number)
        while True:
            yield unique_id

            unique_id += 1
    elif data_type in [uuid.UUID]:
        while True:
            # Brackets required for Arc UUIDs for some fucking reason.
            yield "{" + str(uuid.uuid4()) + "}"

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


def unique_path(prefix="", suffix="", unique_length=4, workspace_path=None):
    """Create unique temporary dataset path.

    Args:
        prefix (str): String to insert before the unique part of the name.
        suffix (str): String to append after the unique part of the name.
        unique_length (int): Number of unique characters to generate.
        workspace_path (pathlib.Path, str, None): Path of workspace to create the
            dataset in.

    Returns:
        str: Path of the created dataset.
    """
    workspace_path = Path(workspace_path) if workspace_path else Path("memory")
    name = unique_name(prefix, suffix, unique_length, allow_initial_digit=False)
    return workspace_path.joinpath(name)
