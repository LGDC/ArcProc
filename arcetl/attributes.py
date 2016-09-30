# -*- coding=utf-8 -*-
"""Attribute operations."""
import logging


from arcetl._attributes import (  # pylint: disable=unused-import
    update_by_domain_code, update_by_expression, update_by_feature_match,
    update_by_function, update_by_geometry, update_by_instance_method,
    update_by_joined_value, update_by_near_feature, update_by_overlay,
    update_by_unique_id, update_node_ids_by_geometry
    )


LOG = logging.getLogger(__name__)
