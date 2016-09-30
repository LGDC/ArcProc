# -*- coding=utf-8 -*-
"""ETL framework library based on ArcGIS/ArcPy."""
#pylint: disable=unused-import
from .etl import ArcETL
from .geometry.constructs import (
    generate_service_areas, generate_service_rings
    )
from .geometry.sets import (
    clip_features, dissolve_features, erase_features, identity_features,
    keep_features_by_location, overlay_features, union_features
    )
from .geometry.transformations import (
    convert_dataset_to_spatial, convert_polygons_to_lines,
    eliminate_interior_rings, planarize_features, project
    )
from .fields import (
    join_field, rename_field
    )
from .helpers import (
    sexagesimal_angle_to_decimal, toggle_arc_extension, unique_ids,
    unique_name, unique_temp_dataset_path
    )
from .metadata import (
    dataset_metadata, domain_metadata, feature_count, field_metadata,
    is_valid_dataset, workspace_dataset_names
    )
from .network import (closest_facility_route)
from .services import (generate_token, toggle_service)
from .values import (
    features_as_dicts, features_as_iters, near_features_as_dicts,
    oid_field_value_map, oid_field_values, oid_geometries, oid_geometry_map,
    sorted_feature_dicts, sorted_feature_iters
    )
from .workspace import (
    build_locator, build_network, compress_geodatabase, create_file_geodatabase,
    create_geodatabase_xml_backup, execute_sql_statement, write_rows_to_csvfile
    )


__version__ = '1.0'
