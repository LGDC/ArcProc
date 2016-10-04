# -*- coding=utf-8 -*-
"""Metadata objects."""
import logging

from arcetl._metadata import (  # pylint: disable=unused-import
    dataset_metadata, domain_metadata, feature_count, field_metadata,
    is_valid_dataset, linear_unit_as_string, spatial_reference_metadata,
    workspace_dataset_names
    )


LOG = logging.getLogger(__name__)
