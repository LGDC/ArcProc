# -*- coding=utf-8 -*-
"""ETL framework library based on ArcGIS/ArcPy."""
from .etl import ArcETL
from .helpers import (
    sexagesimal_angle_to_decimal, unique_ids, unique_name,
    unique_temp_dataset_path,
    )
from .operations import (
    add_field, add_fields_from_metadata_list, add_index,
    adjust_features_for_shapefile, clip_features, compress_geodatabase,
    convert_polygons_to_lines, convert_table_to_spatial_dataset, copy_dataset,
    create_dataset, create_dataset_view, create_file_geodatabase,
    create_geodatabase_xml_backup, delete_dataset, delete_features,
    delete_field, dissolve_features, duplicate_field, erase_features,
    execute_sql_statement, generate_facility_service_rings, identity_features,
    insert_features_from_iterables, insert_features_from_path, join_field,
    keep_features_by_location, overlay_features, planarize_features, project,
    rename_field, set_dataset_privileges, union_features,
    update_field_by_coded_value_domain, update_field_by_constructor_method,
    update_field_by_expression, update_field_by_feature_matching,
    update_field_by_function, update_field_by_geometry,
    update_field_by_joined_value, update_field_by_near_feature,
    update_field_by_overlay, update_field_by_unique_id,
    update_fields_by_geometry_node_ids, write_rows_to_csvfile,
    xref_near_features,
    )
from .properties import (
    dataset_metadata, feature_count, field_metadata, field_values,
    is_valid_dataset, oid_field_value, oid_field_value_map, oid_geometry,
    oid_geometry_map, workspace_dataset_names,
    )

__version__ = '1.0'
