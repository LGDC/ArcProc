# -*- coding=utf-8 -*-
"""Dataset operations."""
import logging

from arcetl._dataset import (  # pylint: disable=unused-import
    add_field, add_field_from_metadata, add_index, copy, create, create_view,
    delete, delete_field, duplicate_field, set_privileges
    )

LOG = logging.getLogger(__name__)
