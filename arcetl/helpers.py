"""Internal module helper objects."""
from collections import Iterable
import inspect
import logging
import os
import random
import string
import uuid

import six


LOG = logging.getLogger(__name__)


def contain(obj, nonetypes_as_empty=True):
    """Generate contained items if a collection, otherwise generate object.

    Args:
        obj: Any object, collection or otherwise.
        nontypes_as_empty (bool): True if NoneTypes treated as an empty
            collection, otherwise False.

    """
    if nonetypes_as_empty and obj is None:
        return
    if inspect.isgeneratorfunction(obj):
        obj = obj()
    if isinstance(obj, Iterable) and not isinstance(obj, six.string_types):
        for i in obj:
            yield i
    else:
        yield obj


def freeze_values(*values):
    """Generator for iterable with bytearrays converted to bytes.

    Currently only freezes bytearrays to bytes.

    Args:
        values: Values to return.

    Yields:
        object: If a value is mutable, will yield value as immutable type of the
            value. Otherwise will yield as original value/type.

    """
    for val in values:
        if isinstance(val, bytearray):
            yield bytes(val)
        else:
            yield val


def leveled_logger(logger, level_repr=None):
    """Return function to log into logger at the given level.

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
    level = {None: 0, 'debug': logging.DEBUG, 'info': logging.INFO,
             'warning': logging.WARNING, 'error': logging.ERROR,
             'critical': logging.CRITICAL}
    if level_repr in level.values():
        result = level_repr
    elif level_repr is None:
        result = level[level_repr]
    elif isinstance(level_repr, six.string_types):
        result = level[level_repr.lower()]
    else:
        raise RuntimeError("level_repr invalid.")
    return result


def unique_dataset_path(prefix='', suffix='', unique_length=4,
                        workspace_path='in_memory'):
    """Create unique temporary dataset path.

    Args:
        prefix (str): String to insert before the unique part of the name.
        suffix (str): String to append after the unique part of the name.
        unique_length (int): Number of unique characters to generate.
        workspace_path (str): Path of workspace to create the dataset in.

    Returns:
        str: Path of the created dataset.

    """
    name = unique_name(prefix, suffix, unique_length,
                       allow_initial_digit=False)
    return os.path.join(workspace_path, name)


def unique_ids(data_type=uuid.UUID, string_length=4):
    """Generator for unique IDs.

    Args:
        data_type: Type object to create unique IDs as.
        string_length (int): Length to make unique IDs of type string.
            Ignored if data_type is not a stringtype.

    Yields:
        Unique ID.

    """
    if data_type in (float, int):
        # Skip 0 (problematic - some processing functions use 0 for null).
        unique_id = data_type(1)
        while True:
            yield unique_id
            unique_id += 1
    elif data_type in (uuid.UUID,):
        while True:
            yield uuid.uuid4()
    elif data_type in (str,):
        seed = string.ascii_letters + string.digits
        used_ids = set()
        while True:
            unique_id = ''.join(random.choice(seed)
                                for _ in range(string_length))
            if unique_id in used_ids:
                continue
            yield unique_id
    else:
        raise NotImplementedError(
            "Unique IDs for {} type not implemented.".format(data_type)
            )


def unique_name(prefix='', suffix='', unique_length=4,
                allow_initial_digit=True):
    """Generate unique name.

    Args:
        prefix (str): String to insert before the unique part of the name.
        suffix (str): String to append after the unique part of the name.
        unique_length (int): Number of unique characters to generate.
        allow_initial_number (bool): Flag indicating whether to let the
            initial character be a number. Defaults to True.

    Returns:
        str: Unique name.

    """
    name = prefix + next(unique_ids(str, unique_length)) + suffix
    if not allow_initial_digit and name[0].isdigit():
        name = unique_name(prefix, suffix, unique_length, allow_initial_digit)
    return name


unique_path = unique_dataset_path
