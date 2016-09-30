# -*- coding=utf-8 -*-
"""Dataset operations."""
import logging

from arcetl._dataset import (  # pylint: disable=unused-import
    add_field, add_field_from_metadata, add_index, copy, create, create_view,
    delete, delete_field, set_privileges
    )

LOG = logging.getLogger(__name__)

# Replace references in LCOG_ETL with call to singleton.
def add_fields_from_metadata_list(dataset_path, metadata_list, **kwargs):
    """Temp wrapper."""
    for metadata in metadata_list:
        result = add_field_from_metadata(dataset_path, metadata, **kwargs)
    return result

##TODO: ANYTHING IN ATTRIBUTES, FEATURES, DATASET THAT CAME FROM ARCOBJ WILL NEED LOG-LEVEL ADDED TO REFERENCES!!!
