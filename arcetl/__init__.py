# -*- coding=utf-8 -*-
"""ETL framework library based on ArcGIS/ArcPy."""
#pylint: disable=unused-import
from .etl import ArcETL
from .geometry.constructs import generate_facility_service_rings
from .geometry.sets import (
    clip_features, dissolve_features, erase_features, identity_features,
    keep_features_by_location, overlay_features, union_features)
from .geometry.transformations import (
    convert_dataset_to_spatial, convert_polygons_to_lines,
    planarize_features, project)
from .features import (
    adjust_features_for_shapefile, delete_features, insert_features_from_dicts,
    insert_features_from_iters, insert_features_from_path)
from .fields import (
    add_field, add_fields_from_metadata_list, add_index, delete_field,
    duplicate_field, join_field, rename_field, update_field_by_domain_code,
    update_field_by_expression, update_field_by_feature_match,
    update_field_by_function, update_field_by_geometry,
    update_field_by_instance_method, update_field_by_joined_value,
    update_field_by_near_feature, update_field_by_overlay,
    update_field_by_unique_id, update_geometry_node_id_fields)
from .helpers import (sexagesimal_angle_to_decimal, unique_ids, unique_name,
                      unique_temp_dataset_path)
from .metadata import (
    dataset_metadata, domain_metadata, feature_count, field_metadata,
    is_valid_dataset, workspace_dataset_names)
from .operations import write_rows_to_csvfile
from .values import (
    features_as_dicts, features_as_iters, near_features_as_dicts,
    oid_field_value_map, oid_field_values, oid_geometries, oid_geometry_map,
    sorted_feature_dicts, sorted_feature_iters)
from .workspace import (
    build_network, compress_geodatabase, copy_dataset, create_dataset,
    create_file_geodatabase, create_geodatabase_xml_backup, delete_dataset,
    execute_sql_statement, set_dataset_privileges)


__version__ = '1.0'
