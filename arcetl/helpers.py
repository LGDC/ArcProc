# -*- coding=utf-8 -*-
"""Helper objects."""
import functools
import logging
import os
import uuid

import decorator


LOG = logging.getLogger(__name__)


def log_function(function):
    """Decorator to log details of an function or method when called."""
    @functools.wraps(function)
    def wrapper(function, *args, **kwargs):
        """Function wrapper for decorator."""
        LOG.debug("@log_function - %s(*args=%s, **kwargs=%s)",
                  function, args, kwargs)
        return function(*args, **kwargs)
    return decorator.decorator(wrapper)(function)


def log_line(line_type, line, level='info'):
    """Log a line in formatted as expected for the type."""
    if not level:
        return
    if line_type == 'start':
        getattr(LOG, level)("Start: {}".format(line))
    elif line_type == 'end':
        getattr(LOG, level)("End: {}.".format(line.split()[0]))
    elif line_type == 'feature_count':
        getattr(LOG, level)("Feature count: {}.".format(line))
    else:
        raise ValueError("Invalid line_type: {}".format(line_type))
    return


def unique_ids(data_type=uuid.UUID, string_length=4):
    """Generator for unique IDs."""
    if data_type in (float, int):
        unique_id = data_type()
        while True:
            yield unique_id
            unique_id += 1
    elif data_type in [uuid.UUID]:
        while True:
            yield uuid.uuid4()
    elif data_type in [str]:
        used_ids = set()
        while True:
            unique_id = str(uuid.uuid4())[:string_length]
            while unique_id in used_ids:
                unique_id = str(uuid.uuid4())[:string_length]
            yield unique_id
    else:
        raise NotImplementedError(
            "Unique IDs for {} type not implemented.".format(data_type))


def unique_name(prefix='', suffix='', unique_length=4):
    """Generate unique name."""
    return '{}{}{}'.format(
        prefix, next(unique_ids(str, unique_length)), suffix)


def unique_temp_dataset_path(prefix='', suffix='', unique_length=4,
                             workspace='in_memory'):
    """Create unique temporary dataset path."""
    return os.path.join(workspace, unique_name(prefix, suffix, unique_length))
