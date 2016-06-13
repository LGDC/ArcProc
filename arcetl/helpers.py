# -*- coding=utf-8 -*-
"""Internal module helper objects."""
import functools
import logging
import os
import uuid

import decorator
import arcpy


LOG = logging.getLogger(__name__)
LOG_LEVEL_MAP = {
    None: 0, 'debug': logging.DEBUG, 'info': logging.INFO,
    'warning': logging.WARNING, 'error': logging.ERROR,
    'critical': logging.CRITICAL}


def log_function(function):
    """Decorator to log details of an function or method when called."""
    @functools.wraps(function)
    def _wrapper(function, *args, **kwargs):
        """Function wrapper for decorator."""
        LOG.debug("@log_function - %s(*args=%s, **kwargs=%s)",
                  function, args, kwargs)
        return function(*args, **kwargs)
    return decorator.decorator(_wrapper)(function)


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
        getattr(LOG, level)(line)
    return


def sexagesimal_angle_to_decimal(degrees, minutes=0, seconds=0, thirds=0,
                                 fourths=0):
    """Convert sexagesimal-parsed angles to a decimal."""
    if degrees is None:
        return None
    # The degrees must be absolute or it won't sum right with subdivisions.
    absolute_decimal = abs(float(degrees))
    try:
        sign_multiplier = abs(float(degrees))/float(degrees)
    except ZeroDivisionError:
        sign_multiplier = 1
    if minutes:
        absolute_decimal += float(minutes)/60
    if seconds:
        absolute_decimal += float(seconds)/3600
    if thirds:
        absolute_decimal += float(thirds)/216000
    if fourths:
        absolute_decimal += float(fourths)/12960000
    return absolute_decimal * sign_multiplier


def toggle_arc_extension(extension_code, toggle_on=True, toggle_off=False):
    """Toggle extension on or off for use in ArcPy."""
    if toggle_on:
        status = arcpy.CheckOutExtension(extension_code)
    if toggle_off:
        status = arcpy.CheckInExtension(extension_code)
    return status in ('CheckedIn', 'CheckedOut')


def unique_ids(data_type=uuid.UUID, string_length=None):
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
        # Default if missing.
        if not string_length:
            string_length = 4
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
