# -*- coding=utf-8 -*-
##TODO: This is temporary. Move back to operations after splitting that up.
"""Import operations for use in ETL object."""
import logging

#pylint: disable=unused-import
from .features import (
    adjust_features_for_shapefile, feature_count, insert_features_from_dicts,
    insert_features_from_iters, insert_features_from_path)
from .fields import (
    add_field, add_fields_from_metadata_list, add_index, delete_field,
    duplicate_field, join_field, rename_field, update_field_by_domain_code,
    update_field_by_expression, update_field_by_feature_match,
    update_field_by_function, update_field_by_geometry,
    update_field_by_instance_method, update_field_by_joined_value,
    update_field_by_near_feature, update_field_by_overlay,
    update_field_by_unique_id, update_fields_by_geometry_node_ids)
from .operations import (
    clip_features, delete_features, dissolve_features, erase_features,
    keep_features_by_location, identity_features, overlay_features,
    union_features, convert_polygons_to_lines,
    convert_table_to_spatial_dataset, generate_facility_service_rings,
    planarize_features, project, sort_features, write_rows_to_csvfile,
    xref_near_features, build_network, compress_geodatabase, copy_dataset,
    create_dataset, create_dataset_view, create_file_geodatabase,
    create_geodatabase_xml_backup, delete_dataset, execute_sql_statement,
    set_dataset_privileges)


LOG = logging.getLogger(__name__)
