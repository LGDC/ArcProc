"""Processing framework library based on ArcGIS/ArcPy."""
from arcproc.convert import (
    convert_lines_to_vertex_points,
    convert_points_to_multipoints,
    convert_points_to_thiessen_polygons,
    convert_polygons_to_lines,
    convert_projection,
    convert_rows_to_csvfile,
    convert_table_to_points,
    convert_to_planar_lines,
    split_lines_at_vertices,
)
from arcproc.dataset import (
    DatasetView,
    TempDatasetCopy,
    add_field,
    add_index,
    compress_dataset,
    copy_dataset,
    copy_dataset_features,
    create_dataset,
    dataset_as_feature_set,
    dataset_feature_count,
    delete_dataset,
    delete_field,
    duplicate_field,
    is_valid_dataset,
    remove_all_default_field_values,
    rename_field,
    set_default_field_value,
    unique_dataset_path,
)
from arcproc.exceptions import (
    DatasetNotFoundError,
    DomainNotFoundError,
    FieldNotFoundError,
)
from arcproc.features import (
    FEATURE_UPDATE_TYPES,
    delete_features,
    delete_features_with_ids,
    densify_features,
    eliminate_feature_inner_rings,
    features_as_dicts,
    features_as_tuples,
    insert_features_from_dataset,
    insert_features_from_mappings,
    insert_features_from_sequences,
    keep_features_within_location,
    replace_feature_true_curves,
    update_features_from_dataset,
    update_features_from_mappings,
    update_features_from_sequences,
)
from arcproc.field import (
    field_value_count,
    field_values,
    update_field_with_central_overlay,
    update_field_with_domain,
    update_field_with_dominant_overlay,
    update_field_with_expression,
    update_field_with_field,
    update_field_with_function,
    update_field_with_join,
    update_field_with_mapping,
    update_field_with_overlay_count,
    update_field_with_unique_id,
    update_field_with_value,
)
from arcproc.geometry import (
    UNIT_PLURAL,
    UNIT_RATIO,
    angle_as_decimal,
    compactness_ratio,
    convex_hull,
    coordinate_distance,
    geometry_axis_bound,
    line_between_centroids,
)
from arcproc.geoset import identity_features, join_features_at_center, union_features
from arcproc.managers import Procedure
from arcproc.metadata import (
    Dataset,
    Domain,
    Field,
    SpatialReference,
    SpatialReferenceSourceItem,
    Workspace,
)
from arcproc.network import (
    build_network,
    closest_facility_routes,
    coordinates_node_map,
    create_service_areas,
    create_service_rings,
    id_node_map,
    update_fields_with_node_ids,
)
from arcproc.proximity import (
    adjacent_neighbors_map,
    buffer_features,
    clip_features,
    dissolve_features,
    erase_features,
    nearest_features,
)
from arcproc.services import service_features_as_dicts
from arcproc.tracking import consolidate_tracking_rows, update_tracking_rows
from arcproc.workspace import (
    Session,
    build_locator,
    copy_workspace,
    create_file_geodatabase,
    create_geodatabase_xml_backup,
    delete_workspace,
    is_valid_workspace,
    workspace_dataset_names,
    workspace_dataset_paths,
)


__all__ = [
    # Convert.
    "convert_lines_to_vertex_points",
    "convert_points_to_multipoints",
    "convert_points_to_thiessen_polygons",
    "convert_polygons_to_lines",
    "convert_projection",
    "convert_rows_to_csvfile",
    "convert_table_to_points",
    "convert_to_planar_lines",
    "split_lines_at_vertices",
    # Dataset.
    "DatasetView",
    "TempDatasetCopy",
    "add_field",
    "add_index",
    "compress_dataset",
    "copy_dataset",
    "copy_dataset_features",
    "create_dataset",
    "dataset_as_feature_set",
    "dataset_feature_count",
    "delete_dataset",
    "delete_field",
    "duplicate_field",
    "is_valid_dataset",
    "remove_all_default_field_values",
    "rename_field",
    "set_default_field_value",
    "unique_dataset_path",
    # Exceptions.
    "DatasetNotFoundError",
    "DomainNotFoundError",
    "FieldNotFoundError",
    # Features.
    "FEATURE_UPDATE_TYPES",
    "delete_features",
    "delete_features_with_ids",
    "densify_features",
    "eliminate_feature_inner_rings",
    "features_as_dicts",
    "features_as_tuples",
    "insert_features_from_dataset",
    "insert_features_from_mappings",
    "insert_features_from_sequences",
    "keep_features_within_location",
    "replace_feature_true_curves",
    "update_features_from_dataset",
    "update_features_from_mappings",
    "update_features_from_sequences",
    # Field.
    "field_value_count",
    "field_values",
    "update_field_with_central_overlay",
    "update_field_with_dominant_overlay",
    "update_field_with_domain",
    "update_field_with_expression",
    "update_field_with_field",
    "update_field_with_function",
    "update_field_with_join",
    "update_field_with_mapping",
    "update_field_with_overlay_count",
    "update_field_with_unique_id",
    "update_field_with_value",
    # Geometry.
    "UNIT_PLURAL",
    "UNIT_RATIO",
    "angle_as_decimal",
    "compactness_ratio",
    "convex_hull",
    "coordinate_distance",
    "geometry_axis_bound",
    "line_between_centroids",
    # Geoset.
    "identity_features",
    "join_features_at_center",
    "union_features",
    # Managers.
    "Procedure",
    # Metadata.
    "Dataset",
    "Domain",
    "Field",
    "SpatialReference",
    "SpatialReferenceSourceItem",
    "Workspace",
    # Network.
    "build_network",
    "closest_facility_routes",
    "create_service_areas",
    "create_service_rings",
    "coordinates_node_map",
    "id_node_map",
    "update_fields_with_node_ids",
    # Proximity.
    "adjacent_neighbors_map",
    "buffer_features",
    "clip_features",
    "dissolve_features",
    "erase_features",
    "nearest_features",
    # Services.
    "service_features_as_dicts",
    # Tracking.
    "consolidate_tracking_rows",
    "update_tracking_rows",
    # Workspace.
    "Session",
    "build_locator",
    "copy_workspace",
    "create_file_geodatabase",
    "create_geodatabase_xml_backup",
    "delete_workspace",
    "is_valid_workspace",
    "workspace_dataset_names",
    "workspace_dataset_paths",
]
