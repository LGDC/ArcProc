# -*- coding=utf-8 -*-
"""Feature operations."""
import logging

from arcetl._features import (  # pylint: disable=unused-import
    clip, count, delete, dissolve, erase, insert_from_dicts, insert_from_iters,
    insert_from_path, keep_by_location
    )


LOG = logging.getLogger(__name__)
