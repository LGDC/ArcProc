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
