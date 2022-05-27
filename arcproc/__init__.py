"""Processing framework library based on ArcGIS/ArcPy."""
import arcproc.dataset
import arcproc.features
import arcproc.geometry
import arcproc.geoset
import arcproc.helpers
import arcproc.managers
import arcproc.network
import arcproc.proximity
import arcproc.services
import arcproc.tracking
import arcproc.workspace  # noqa: F401
from arcproc.attributes import (
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


__all__ = [
    # Attributes.
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
]
