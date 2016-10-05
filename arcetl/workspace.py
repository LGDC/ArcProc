# -*- coding=utf-8 -*-
"""Workspace operations."""
import logging

from arcetl._workspace import (  # pylint: disable=unused-import
    build_locator, build_network, compress, create_file_geodatabase,
    create_geodatabase_xml_backup, execute_sql, is_valid, metadata
    )


LOG = logging.getLogger(__name__)
