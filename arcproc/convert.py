"""Conversion operations."""
from collections import Counter
from collections.abc import Sequence
from csv import DictWriter, writer
from logging import DEBUG, INFO, Logger, getLogger
from pathlib import Path
from typing import Iterable, Optional, Union

from arcpy import Multipoint, SetLogHistory
from arcpy.analysis import CreateThiessenPolygons
from arcpy.da import InsertCursor, SearchCursor  # pylint: disable=no-name-in-module
from arcpy.management import (
    CreateFeatureclass,
    Delete,
    FeatureToLine,
    FeatureVerticesToPoints,
    MakeXYEventLayer,
    PolygonToLine,
    SplitLine,
)

from arcproc.attributes import update_field_with_join
from arcproc.dataset import (
    DatasetView,
    add_field,
    copy_dataset,
    create_dataset,
    dataset_feature_count,
    delete_field,
)
from arcproc.features import insert_from_path as insert_features_from_path
from arcproc.helpers import log_entity_states, unique_name
from arcproc.metadata import Dataset, SpatialReference, SpatialReferenceSourceItem


LOG: Logger = getLogger(__name__)
"""Module-level logger."""

SetLogHistory(False)


def convert_lines_to_vertex_points(
    dataset_path: Union[Path, str],
    *,
    output_path: Union[Path, str],
    dataset_where_sql: Optional[str] = None,
    endpoints_only: bool = False,
    log_level: int = INFO,
) -> Counter:
    """Convert line features to points at every vertex.

    Args:
        dataset_path: Path to dataset.
        output_path: Path to output dataset.
        dataset_where_sql: SQL where-clause for dataset subselection.
        endpoints_only: Output points should include line endpoints only if True.
        log_level: Level to log the function at.

    Returns:
        Feature counts for original and output datasets.
    """
    dataset_path = Path(dataset_path)
    output_path = Path(output_path)
    LOG.log(
        log_level,
        "Start: Convert lines in `%s` to vertex points in output `%s`.",
        dataset_path,
        output_path,
    )
    states = Counter()
    states["in original dataset"] = dataset_feature_count(dataset_path)
    view = DatasetView(dataset_path, dataset_where_sql=dataset_where_sql)
    with view:
        FeatureVerticesToPoints(
            in_features=view.name,
            # ArcPy2.8.0: Convert Path to str.
            out_feature_class=str(output_path),
            point_location="ALL" if not endpoints_only else "BOTH_ENDS",
        )
    delete_field(output_path, field_name="ORIG_FID", log_level=DEBUG)
    states["in output"] = dataset_feature_count(output_path)
    log_entity_states("features", states, logger=LOG, log_level=log_level)
    LOG.log(log_level, "End: Convert.")
    return states


def convert_points_to_multipoints(
    dataset_path: Union[Path, str],
    *,
    output_path: Union[Path, str],
    dataset_where_sql: Optional[str] = None,
    log_level: int = INFO,
) -> Counter:
    """Convert point features to multipoints.

    Args:
        dataset_path: Path to dataset.
        output_path: Path to output dataset.
        dataset_where_sql: SQL where-clause for dataset subselection.
        log_level: Level to log the function at.

    Returns:
        Feature counts for original and output datasets.
    """
    dataset_path = Path(dataset_path)
    output_path = Path(output_path)
    LOG.log(
        log_level,
        "Start: Convert points in `%s` to multipoints in output `%s`.",
        dataset_path,
        output_path,
    )
    _dataset = Dataset(dataset_path)
    # ArcPy2.8.0: Convert Path to str (2x).
    CreateFeatureclass(
        out_path=str(output_path.parent),
        out_name=output_path.name,
        geometry_type="MULTIPOINT",
        template=str(dataset_path),
        spatial_reference=_dataset.spatial_reference.object,
    )
    field_names = _dataset.user_field_names + ["SHAPE@"]
    # ArcPy2.8.0: Convert Path to str.
    multipoint_cursor = InsertCursor(in_table=str(output_path), field_names=field_names)
    # ArcPy2.8.0: Convert Path to str.
    point_cursor = SearchCursor(
        in_table=str(dataset_path),
        field_names=field_names,
        where_clause=dataset_where_sql,
    )
    states = Counter()
    states["in original dataset"] = dataset_feature_count(dataset_path)
    with multipoint_cursor, point_cursor:
        for point_feature in point_cursor:
            multipoint_geometry = Multipoint(point_feature[-1].firstPoint)
            multipoint_feature = point_feature[:-1] + (multipoint_geometry,)
            multipoint_cursor.insertRow(multipoint_feature)
    states["in output"] = dataset_feature_count(output_path)
    log_entity_states("features", states, logger=LOG, log_level=log_level)
    LOG.log(log_level, "End: Convert.")
    return states


def convert_points_to_thiessen_polygons(  # pylint: disable=invalid-name
    dataset_path: Union[Path, str],
    *,
    output_path: Union[Path, str],
    dataset_where_sql: Optional[str] = None,
    log_level: int = INFO,
) -> Counter:
    """Convert point features to Thiessen polygons.

    Args:
        dataset_path: Path to dataset.
        output_path: Path to output dataset.
        dataset_where_sql: SQL where-clause for dataset subselection.
        log_level: Level to log the function at.

    Returns:
        Feature counts for original and output datasets.
    """
    dataset_path = Path(dataset_path)
    output_path = Path(output_path)
    LOG.log(
        log_level,
        "Start: Convert points in `%s` to Thiessen polygons in output `%s`.",
        dataset_path,
        output_path,
    )
    states = Counter()
    states["in original dataset"] = dataset_feature_count(dataset_path)
    view = DatasetView(dataset_path, dataset_where_sql=dataset_where_sql)
    with view:
        # ArcPy2.8.0: Convert Path to str.
        CreateThiessenPolygons(
            in_features=view.name,
            out_feature_class=str(output_path),
            fields_to_copy="ALL",
        )
    states["in output"] = dataset_feature_count(output_path)
    log_entity_states("features", states, logger=LOG, log_level=log_level)
    LOG.log(log_level, "End: Convert.")
    return states


def convert_polygons_to_lines(
    dataset_path: Union[Path, str],
    *,
    output_path: Union[Path, str],
    dataset_where_sql: Optional[str] = None,
    id_field_name: Optional[str] = None,
    make_topological: bool = False,
    log_level: int = INFO,
) -> Counter:
    """Convert polygon features to lines.

    Note:
        If `make_topological` is set to True, shared outlines will be a single, separate
        feature. Note that one cannot pass attributes to a topological transformation
        (as the values would not apply to all adjacent features).

        If an id field name is specified, the output dataset will identify the input
        features that defined the line feature with the name & values from the provided
        field. This option will be ignored if the output is non-topological lines, as
        the field will pass over with the rest of the attributes.

    Args:
        dataset_path: Path to dataset.
        output_path: Path to output dataset.
        dataset_where_sql: SQL where-clause for dataset subselection.
        id_field_name: Name of ID field to apply on topological lines.
        make_topological: Make line output topological, or merged where lines overlap.
        log_level: Level to log the function at.

    Returns:
        Feature counts for original and output datasets.
    """
    dataset_path = Path(dataset_path)
    output_path = Path(output_path)
    LOG.log(
        log_level,
        "Start: Convert polgyons in `%s` to lines in output `%s`.",
        dataset_path,
        output_path,
    )
    states = Counter()
    states["in original dataset"] = dataset_feature_count(dataset_path)
    view = DatasetView(dataset_path, dataset_where_sql=dataset_where_sql)
    with view:
        # ArcPy2.8.0: Convert Path to str.
        PolygonToLine(
            in_features=view.name,
            out_feature_class=str(output_path),
            neighbor_option=(
                "IDENTIFY_NEIGHBORS" if make_topological else "IGNORE_NEIGHBORS"
            ),
        )
    if make_topological:
        _dataset = Dataset(dataset_path)
        for side in ["left", "right"]:
            oid_key = f"{side.upper()}_FID"
            if id_field_name:
                id_field = next(
                    _field
                    for _field in _dataset.fields
                    if _field.name.lower() == id_field_name.lower()
                )
                id_field.name = f"{side.upper()}_{id_field_name}"
                # Cannot create an OID-type field, so force to long.
                if id_field.type.upper() == "OID":
                    id_field.type = "LONG"
                add_field(output_path, log_level=DEBUG, **id_field.field_as_dict)
                update_field_with_join(
                    output_path,
                    field_name=id_field.name,
                    key_field_names=[oid_key],
                    join_dataset_path=dataset_path,
                    join_field_name=id_field_name,
                    join_key_field_names=[_dataset.oid_field_name],
                    log_level=DEBUG,
                )
            delete_field(output_path, field_name=oid_key, log_level=DEBUG)
    else:
        delete_field(output_path, field_name="ORIG_FID", log_level=DEBUG)
    states["in output"] = dataset_feature_count(output_path)
    log_entity_states("features", states, logger=LOG, log_level=log_level)
    LOG.log(log_level, "End: Convert.")
    return states


def convert_projection(
    dataset_path: Union[Path, str],
    *,
    output_path: Union[Path, str],
    dataset_where_sql: Optional[str] = None,
    spatial_reference_item: SpatialReferenceSourceItem = 4326,
    log_level: int = INFO,
) -> Counter:
    """Convert projection of features.

    Args:
        dataset_path: Path to dataset.
        output_path: Path to output dataset.
        dataset_where_sql: SQL where-clause for dataset subselection.
        spatial_reference_item: Item from which the spatial reference of the output
            geometry will be derived. Default is 4326 (EPSG code for unprojected WGS84).
        log_level: Level to log the function at.

    Returns:
        Feature counts for original and output datasets.
    """
    dataset_path = Path(dataset_path)
    output_path = Path(output_path)
    spatial_reference = SpatialReference(spatial_reference_item)
    LOG.log(
        log_level,
        "Start: Convert projection in `%s` to %s in output `%s`.",
        dataset_path,
        spatial_reference.name,
        output_path,
    )
    _dataset = Dataset(dataset_path)
    states = Counter()
    states["in original dataset"] = dataset_feature_count(dataset_path)
    # Project tool ignores view selections, so we create empty output & insert features.
    create_dataset(
        dataset_path=output_path,
        field_metadata_list=_dataset.user_fields,
        geometry_type=_dataset.geometry_type,
        spatial_reference_item=spatial_reference,
        log_level=DEBUG,
    )
    insert_features_from_path(
        output_path,
        field_names=_dataset.user_field_names,
        source_path=dataset_path,
        source_where_sql=dataset_where_sql,
        log_level=DEBUG,
    )
    states["in output"] = dataset_feature_count(output_path)
    log_entity_states("features", states, logger=LOG, log_level=log_level)
    LOG.log(log_level, "End: Convert.")
    return states


def convert_rows_to_csvfile(
    rows: Iterable[Union[dict, Sequence]],
    *field_names: Iterable[str],
    output_path: Union[Path, str],
    file_mode: str = "wb",
    include_header: bool = False,
    log_level: int = INFO,
) -> Counter:
    """Convert sequence of row objects to a CSV file.

    Args:
        rows: Rows to store.
        field_names: Names of fields to include in CSV file. If rows are dictionaries,
            field names must exist as keys in all rows. If rows are sequences, field
            names must be in same order as their corresponding attributes in all rows.
        output_path: Path to output dataset.
        file_mode: Code indicating the file mode for writing.
        include_header: Include a header as the first row in the CSV file if True.
        log_level: Level to log the function at.

    Returns:
        Feature counts for original rows and output dataset.

    Raises:
        TypeError: If any rows are not a dictionary or sequence.
    """
    output_path = Path(output_path)
    LOG.log(log_level, "Start: Convert rows to CSVfile `%s`.", output_path)
    field_names = list(field_names)
    csvfile = output_path.open(mode=file_mode)
    states = Counter()
    with csvfile:
        for index, row in enumerate(rows):
            states["in original rows"] += 1
            if index == 0:
                if isinstance(row, dict):
                    the_writer = DictWriter(csvfile, fieldnames=field_names)
                    if include_header:
                        the_writer.writeheader()
                elif isinstance(row, Sequence):
                    the_writer = writer(csvfile)
                    if include_header:
                        the_writer.writerow(field_names)
                else:
                    raise TypeError("Rows must be dictionaries or sequences.")

            the_writer.writerow(row)
            states["in output"] += 1
    LOG.log(log_level, "End: Convert.")
    return states


def convert_table_to_points(
    dataset_path: Union[Path, str],
    *,
    x_field_name: str,
    y_field_name: str,
    z_field_name: Optional[str] = None,
    output_path: Union[Path, str],
    dataset_where_sql: Optional[str] = None,
    spatial_reference_item: SpatialReferenceSourceItem = 4326,
    log_level: int = INFO,
) -> Counter:
    """Convert coordinate table rows to a point features dataset.

    Args:
        dataset_path: Path to dataset.
        output_path: Path to output dataset.
        dataset_where_sql: SQL where-clause for dataset subselection.
        x_field_name: Name of field with x-coordinate.
        y_field_name: Name of field with y-coordinate.
        z_field_name: Name of field with z-coordinate.
        spatial_reference_item: Item from which the spatial reference of the output
            geometry will be derived. Default is 4326 (EPSG code for unprojected WGS84).
        log_level: Level to log the function at.

    Returns:
        Feature counts for original and output datasets.
    """
    dataset_path = Path(dataset_path)
    output_path = Path(output_path)
    LOG.log(
        log_level,
        "Start: Convert table `%s` to points in output `%s`.",
        dataset_path,
        output_path,
    )
    layer_name = unique_name()
    states = Counter()
    states["in original dataset"] = dataset_feature_count(dataset_path)
    view = DatasetView(dataset_path, dataset_where_sql=dataset_where_sql)
    MakeXYEventLayer(
        table=view.name,
        out_layer=layer_name,
        in_x_field=x_field_name,
        in_y_field=y_field_name,
        in_z_field=z_field_name,
        spatial_reference=SpatialReference(spatial_reference_item).object,
    )
    copy_dataset(layer_name, output_path=output_path, log_level=DEBUG)
    Delete(layer_name)
    states["in output"] = dataset_feature_count(output_path)
    log_entity_states("features", states, logger=LOG, log_level=log_level)
    LOG.log(log_level, "End: Convert.")
    return states


def convert_to_planar_lines(
    dataset_path: Union[Path, str],
    *,
    output_path: Union[Path, str],
    dataset_where_sql: Optional[str] = None,
    log_level: int = INFO,
) -> Counter:
    """Convert feature geometry to planar lines.

    Note:
        This method does not make topological linework. However it does carry all
        attributes with it, rather than just an ID attribute.

        Since this method breaks the new line geometry at intersections, it can be
        useful to break line geometry features that cross.

    Args:
        dataset_path: Path to dataset.
        output_path: Path to output dataset.
        dataset_where_sql: SQL where-clause for dataset subselection.
        log_level: Level to log the function at.

    Returns:
        Feature counts for original and output datasets.
    """
    dataset_path = Path(dataset_path)
    output_path = Path(output_path)
    LOG.log(
        log_level,
        "Start: Covnert geometry in `%s` to planar lines in output `%s`.",
        dataset_path,
        output_path,
    )
    states = Counter()
    states["in original dataset"] = dataset_feature_count(dataset_path)
    view = DatasetView(dataset_path, dataset_where_sql=dataset_where_sql)
    with view:
        # ArcPy2.8.0: Convert Path to str.
        FeatureToLine(
            in_features=view.name, out_feature_class=str(output_path), attributes=True
        )
    states["in output"] = dataset_feature_count(output_path)
    log_entity_states("features", states, logger=LOG, log_level=log_level)
    LOG.log(log_level, "End: Convert.")
    return states


def split_lines_at_vertices(
    dataset_path: Union[Path, str],
    *,
    output_path: Union[Path, str],
    dataset_where_sql: Optional[str] = None,
    log_level: int = INFO,
) -> Counter:
    """Split lines into smaller lines between vertices.

    The original datasets can be lines or polygons. Polygons will be split along their
    rings.

    Args:
        dataset_path: Path to dataset.
        output_path: Path to output dataset.
        dataset_where_sql: SQL where-clause for dataset subselection.
        log_level: Level to log the function at.

    Returns:
        Feature counts for original and output datasets.
    """
    dataset_path = Path(dataset_path)
    output_path = Path(output_path)
    LOG.log(
        log_level,
        "Start: Split line geometry in `%s` into lines between vertices in output `%s`.",
        dataset_path,
        output_path,
    )
    states = Counter()
    states["in original dataset"] = dataset_feature_count(dataset_path)
    view = DatasetView(dataset_path, dataset_where_sql=dataset_where_sql)
    with view:
        # ArcPy2.8.0: Convert Path to str.
        SplitLine(in_features=view.name, out_feature_class=str(output_path))
    states["in output"] = dataset_feature_count(output_path)
    log_entity_states("features", states, logger=LOG, log_level=log_level)
    LOG.log(log_level, "End: Split.")
    return states
