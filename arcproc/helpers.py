"""Internal module helper objects."""
import logging
import math
import random
import string
from collections import Counter
from datetime import datetime as _datetime
from datetime import timedelta
from functools import partial
from pathlib import Path
from types import BuiltinFunctionType, BuiltinMethodType, FunctionType, MethodType
from typing import Any, Iterator, Optional, Sequence, Union
from uuid import UUID, uuid4

import arcpy
from more_itertools import pairwise


LOG: logging.Logger = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""

EXECUTABLE_TYPES: tuple = (
    BuiltinFunctionType,
    BuiltinMethodType,
    FunctionType,
    MethodType,
    partial,
)
"""Executable object types. Useful for determining if an object can be executed."""


arcpy.SetLogHistory(False)


def elapsed(
    start_time: _datetime,
    logger: Optional[logging.Logger] = None,
    log_level: int = logging.INFO,
) -> timedelta:
    """Return time-delta since start time.

    Args:
        start_time: Starting point to measure time elapsed since.
        logger: Logger to emit elapsed message.
        log_level: Level to log elapsed message at.
    """
    delta = _datetime.now() - start_time
    if logger:
        logger.log(
            log_level,
            "Elapsed: %s hrs, %s min, %s sec.",
            (delta.days * 24 + delta.seconds // 3600),
            ((delta.seconds // 60) % 60),
            (delta.seconds % 60),
        )
    return delta


def freeze_values(*values: Any) -> Iterator[Any]:
    """Generate "frozen" versions of mutable objects.

    If a value is mutable, will yield value as immutable type of the value. Otherwise
        will yield as original value/type.
    Currently only freezes bytearrays to bytes.

    Args:
        *values: Values to freeze.
    """
    for val in values:
        if isinstance(val, bytearray):
            yield bytes(val)

        else:
            yield val


def log_entity_states(
    entity_label: str,
    states: Counter,
    *,
    logger: Optional[logging.Logger] = None,
    log_level: int = logging.INFO,
    logline_format: str = "{count:,} {entity_type} {state}.",
) -> None:
    """Log the counts for entities in each state from provided counter.

    Args:
        entity_label: Label for the entity type whose states are counted. Preferably
            plural, e.g. "datasets".
        states: State-counts.
        logger: Logger to handle emitted loglines. If not specified, will use module
            -level logger.
        log_level: Level to log the function at.
        logline_format: Formating string for logline. Use keywords in default value
            (`state` & `count` are the key & value of a single item in the the Counter
            `states`).
    """
    if not logger:
        logger = LOG
    if sum(states.values()) == 0:
        logger.log(log_level, "No %s states to log.", entity_label)
    else:
        for state, count in sorted(states.items()):
            logger.log(
                log_level,
                logline_format.format(
                    count=count, entity_type=entity_label, state=state
                ),
            )


def python_type(
    type_description: str,
) -> Union[_datetime, float, int, str, UUID, arcpy.Geometry]:
    """Return object constructor representing the Python type.

    Args:
        type_description: Arc-style type description/code.
    """
    instance = {
        "date": _datetime,
        "double": float,
        "single": float,
        "integer": int,
        "long": int,
        "short": int,
        "smallinteger": int,
        "geometry": arcpy.Geometry,
        "guid": UUID,
        "string": str,
        "text": str,
    }
    return instance[type_description.lower()]


def same_feature(*features: Sequence[Any]) -> bool:
    """Determine whether sequence feature representations are the same.

    Args:
        *features: Features to compare.

    Returns:
        True if same feature, False otherwise.
    """
    same = all(
        same_value(*values) for pair in pairwise(features) for values in zip(*pair)
    )
    return same


def same_value(*values: Any) -> bool:
    """Determine whether values are the same.

    Notes:
        Microsecond rounding occurs differently on different datetime source formats, so
            values that align down to the whole-second return True.
        For geometry:
            Has not been tested on the following geometry types: multipoint, multipatch,
                dimension, annotation.
            Adding vertices that do not alter overall polygon shape do not appear to
                effect `geometry.equals()`.
            Adding those vertices does change `geometry.WKB` & `geometry.WKT`, so be
                aware that those will be "different" values.
            Derived float values (e.g. geometry lengths & areas) can have slight
                differences between sources when they are essentially the same. Avoid
                comparisons between those.

    Args:
        *values: Values to compare.

    Returns:
        True if same value, False otherwise.
    """
    if not all(isinstance(value, type(values[0])) for value in values[1:]):
        same = False
    elif isinstance(values[0], _datetime):
        same = all(
            getattr(value, attr) == getattr(cmp_value, attr)
            for value, cmp_value in pairwise(values)
            for attr in ["year", "month", "day", "hour", "minute", "second"]
        )
    elif isinstance(values[0], float):
        same = all(
            math.isclose(value, cmp_value) for value, cmp_value in pairwise(values)
        )
    # Geometry equality has extra considerations.
    elif isinstance(values[0], (arcpy.Geometry, arcpy.Point)):
        same = all(value.equals(cmp_value) for value, cmp_value in pairwise(values))
    else:
        same = all(value == cmp_value for value, cmp_value in pairwise(values))
    return same


def slugify(text: str, *, separator: str = "-", force_lowercase: bool = True) -> str:
    """Return text in slug-form.

    Args:
        text: String to slugify.
        separator: Separator to replace punctuation & whitespace.
    """
    slug = text.lower() if force_lowercase else text
    for char in string.punctuation + string.whitespace:
        slug = slug.replace(char, separator)
    while separator * 2 in slug:
        slug = slug.replace(separator * 2, separator)
    if slug.startswith(separator):
        slug = slug[len(separator) :]
    if slug.endswith(separator):
        slug = slug[: -len(separator)]
    return slug


def unique_ids(
    data_type: Any = UUID, *, string_length: int = 4, initial_number: int = 1
) -> Union[float, int, str, UUID]:
    """Generate unique IDs.

    Args:
        data_type: Type of value with which to create unique IDs.
        string_length: Length to make unique IDs of type string. Ignored if data type is
            not string.
        initial_number: Initial number for a proposed ID, if using a numeric data type.
    """
    if data_type in [float, int]:
        unique_id = data_type(initial_number)
        while True:
            yield unique_id

            unique_id += 1
    elif data_type == UUID:
        while True:
            # Brackets required for Arc UUIDs for some fucking reason.
            yield "{" + str(uuid4()) + "}"

    elif data_type == str:
        seed = string.ascii_letters + string.digits
        used_ids = set()
        while True:
            unique_id = "".join(random.choice(seed) for _ in range(string_length))
            if unique_id in used_ids:
                continue

            yield unique_id

            used_ids.add(unique_id)
    else:
        raise NotImplementedError(f"Unique IDs for {data_type} type not implemented")


def unique_name(
    prefix: str = "",
    suffix: str = "",
    *,
    unique_length: int = 4,
    allow_initial_digit: bool = True,
) -> str:
    """Return unique name.

    Args:
        prefix: Prefix insert before the unique part of the name.
        suffix: Suffix to append after the unique part of the name.
        unique_length: Number of unique characters to include.
        allow_initial_number: Allow the initial character be a number if True.
    """
    name = prefix + next(unique_ids(str, string_length=unique_length)) + suffix
    if not allow_initial_digit and name[0].isdigit():
        name = unique_name(
            prefix,
            suffix,
            unique_length=unique_length,
            allow_initial_digit=allow_initial_digit,
        )
    return name


def unique_path(
    prefix: str = "",
    suffix: str = "",
    *,
    unique_length: int = 4,
    workspace_path: Union[Path, str] = Path("memory"),
) -> Path:
    """Return unique path in the given workspace.

    Args:
        prefix: Prefix insert before the unique part of the name.
        suffix: Suffix to append after the unique part of the name.
        unique_length: Number of unique characters to include in the name.
        workspace_path: Path of workspace to create the dataset in.
    """
    workspace_path = Path(workspace_path) if workspace_path else Path("memory")
    name = unique_name(
        prefix, suffix, unique_length=unique_length, allow_initial_digit=False
    )
    return workspace_path / name
