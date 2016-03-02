# -*- coding=utf-8 -*-
"""Helper objects."""
import functools
import logging

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
