"""Internal module helper objects."""
import logging
import os
import random
import string
import uuid


LOG = logging.getLogger(__name__)


def log_level(name=None):
    """Return integer for logging module level.

    Args:
        name: Name/code of the logging level.

    Returns:
        int: Logging module level.
    """
    level = {None: 0, 'debug': logging.DEBUG, 'info': logging.INFO,
             'warning': logging.WARNING, 'error': logging.ERROR,
             'critical': logging.CRITICAL}
    return level[name]


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
        unique_id = data_type()
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


def unique_temp_dataset_path(prefix='', suffix='', unique_length=4,
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
