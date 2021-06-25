"""Conversion operations."""
from collections import Counter

try:
    from collections.abc import Sequence
except ImportError:
    from collections import Sequence
import csv
import logging
import os

import arcpy

from arcproc.arcobj import (
    DatasetView,
    dataset_metadata,
    spatial_reference,
    spatial_reference_metadata,
)
from arcproc import attributes
from arcproc import dataset
from arcproc import features
from arcproc.helpers import contain, log_entity_states, unique_name


LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""

arcpy.SetLogHistory(False)


def planarize(dataset_path, output_path, **kwargs):
    """Planarize feature geometry into lines.

    Note:
        This method does not make topological linework. However it does carry all
        attributes with it, rather than just an ID attribute.

        Since this method breaks the new line geometry at intersections, it can be
        useful to break line geometry features that cross.

    Args:
        dataset_path (str): Path of the dataset.
        output_path (str): Path of the output dataset.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        tolerance (float): Tolerance for coincidence, in units of the dataset.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        str: Path of the converted dataset.
    """
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("tolerance")
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level,
        "Start: Planarize geometry in `%s` to lines in `%s`.",
        dataset_path,
        output_path,
    )
    view = DatasetView(dataset_path, kwargs["dataset_where_sql"])
    with view:
        arcpy.management.FeatureToLine(
            in_features=view.name,
            out_feature_class=output_path,
            cluster_tolerance=kwargs["tolerance"],
            attributes=True,
        )
    LOG.log(level, "End: Planarize.")
    return output_path


def points_to_multipoints(dataset_path, output_path, **kwargs):
    """Convert geometry from points to multipoints.

    Args:
        dataset_path (str): Path of the dataset.
        output_path (str): Path of the output dataset.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        str: Path of the converted dataset.
    """
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level,
        "Start: Convert points in `%s` to multipoints in `%s`.",
        dataset_path,
        output_path,
    )
    meta = {"dataset": dataset_metadata(dataset_path)}
    sref = spatial_reference(dataset_path)
    arcpy.management.CreateFeatureclass(
        out_path=os.path.dirname(output_path),
        out_name=os.path.basename(output_path),
        geometry_type="MULTIPOINT",
        template=dataset_path,
        spatial_reference=sref,
    )
    multipoint_cursor = arcpy.da.InsertCursor(
        output_path, field_names=meta["dataset"]["user_field_names"] + ["SHAPE@"]
    )
    point_cursor = arcpy.da.SearchCursor(
        dataset_path,
        field_names=meta["dataset"]["user_field_names"] + ["SHAPE@"],
        where_clause=kwargs.get("dataset_where_sql"),
    )
    with multipoint_cursor, point_cursor:
        for feature in point_cursor:
            geometry = arcpy.Multipoint(feature[-1].firstPoint)
            multipoint_cursor.insertRow(feature[:-1] + (geometry,))
    states = Counter(converted=dataset.feature_count(dataset_path))
    log_entity_states("features", states, LOG, log_level=level)
    LOG.log(level, "End: Convert.")
    return output_path


def polygons_to_lines(dataset_path, output_path, topological=False, **kwargs):
    """Convert geometry from polygons to lines.

    Note:
        If topological is set to True, shared outlines will be a single, separate
        feature. Note that one cannot pass attributes to a topological transformation
        (as the values would not apply to all adjacent features).

        If an id field name is specified, the output dataset will identify the input
        features that defined the line feature with the name & values from the provided
        field. This option will be ignored if the output is non-topological lines, as
        the field will pass over with the rest of the attributes.

    Args:
        dataset_path (str): Path of the dataset.
        output_path (str): Path of the output dataset.
        topological (bool): Flag to indicate lines should be topological, or merged
            where lines overlap.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        id_field_name (str): Name of the field to apply ID to lines from.
        tolerance (float): Tolerance for coincidence, in units of the dataset.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        str: Path of the converted dataset.
    """
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("id_field_name")
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level,
        "Start: Convert polgyons in `%s` to lines in `%s`.",
        dataset_path,
        output_path,
    )
    meta = {
        "dataset": dataset_metadata(dataset_path),
        "orig_tolerance": arcpy.env.XYTolerance,
    }
    view = DatasetView(dataset_path, kwargs["dataset_where_sql"])
    with view:
        if "tolerance" in kwargs:
            arcpy.env.XYTolerance = kwargs["tolerance"]
        arcpy.management.PolygonToLine(
            in_features=view.name,
            out_feature_class=output_path,
            neighbor_option=topological,
        )
        if "tolerance" in kwargs:
            arcpy.env.XYTolerance = meta["orig_tolerance"]
    if topological:
        for side in ["left", "right"]:
            meta[side] = {"oid_key": side.upper() + "_FID"}
            if kwargs["id_field_name"]:
                meta[side]["id_field"] = next(
                    field
                    for field in meta["dataset"]["fields"]
                    if field["name"].lower() == kwargs["id_field_name"].lower()
                )
                meta[side]["id_field"]["name"] = side + "_" + kwargs["id_field_name"]
                # Cannot create an OID-type field, so force to long.
                if meta[side]["id_field"]["type"].lower() == "oid":
                    meta[side]["id_field"]["type"] = "long"
                dataset.add_field(
                    output_path, log_level=logging.DEBUG, **meta[side]["id_field"]
                )
                attributes.update_by_joined_value(
                    output_path,
                    field_name=meta[side]["id_field"]["name"],
                    join_dataset_path=dataset_path,
                    join_field_name=kwargs["id_field_name"],
                    on_field_pairs=[
                        (meta[side]["oid_key"], meta["dataset"]["oid_field_name"])
                    ],
                    log_level=logging.DEBUG,
                )
            dataset.delete_field(
                output_path, meta[side]["oid_key"], log_level=logging.DEBUG
            )
    else:
        dataset.delete_field(output_path, "ORIG_FID", log_level=logging.DEBUG)
    LOG.log(level, "End: Convert.")
    return output_path


def project(dataset_path, output_path, spatial_reference_item=4326, **kwargs):
    """Project dataset features to a new dataset.

    Args:
        dataset_path (str): Path of the dataset.
        output_path (str): Path of the output dataset.
        spatial_reference_item: Item from which the spatial reference of the output
            geometry will be derived. Default is 4326 (EPSG code for unprojected WGS84).
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        str: Path of the converted dataset.
    """
    kwargs.setdefault("dataset_where_sql")
    meta = {"spatial": spatial_reference_metadata(spatial_reference_item)}
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level,
        "Start: Project `%s` to srid=%s in `%s`.",
        dataset_path,
        meta["spatial"]["object"].factoryCode,
        output_path,
    )
    meta["dataset"] = dataset_metadata(dataset_path)
    # Project tool cannot output to an in-memory workspace (will throw error 000944).
    # This is not a bug. Esri"s Project documentation (as of v10.6) specifically states:
    # "The in_memory workspace is not supported as a location to write the output
    # dataset."
    # https://desktop.arcgis.com/en/arcmap/latest/tools/data-management-toolbox/project.htm
    # https://pro.arcgis.com/en/pro-app/tool-reference/data-management/project.htm
    # To avoid all this ado, using create to clone a (reprojected)
    # dataset & insert features into it.
    dataset.create(
        dataset_path=output_path,
        field_metadata_list=meta["dataset"]["user_fields"],
        geometry_type=meta["dataset"]["geometry_type"],
        spatial_reference_item=meta["spatial"]["object"],
        log_level=logging.DEBUG,
    )
    features.insert_from_path(
        dataset_path=output_path,
        insert_dataset_path=dataset_path,
        field_names=meta["dataset"]["user_field_names"],
        insert_where_sql=kwargs["dataset_where_sql"],
        log_level=logging.DEBUG,
    )
    LOG.log(level, "End: Project.")
    return output_path


def rows_to_csvfile(rows, output_path, field_names, header=False, **kwargs):
    """Write collection of rows to a CSV-file.

    Note: Rows can be represented by either dictionaries or sequences.

    Args:
        rows (iter): Collection of dictionaries or sequences representing rows.
        output_path (str): Path of the output dataset.
        field_names (iter): Collection of the field names, in the desired order or
            output.
        header (bool): Write a header in the CSV output if True.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        file_mode (str): Code indicating the file mode for writing. Default is "wb".
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        str: Path of the CSV-file.
    """
    kwargs.setdefault("file_mode", "wb")
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(level, "Start: Write rows to CSV file `%s`.", output_path)
    field_names = list(contain(field_names))
    with open(output_path, kwargs["file_mode"]) as csvfile:
        for index, row in enumerate(rows):
            if index == 0:
                if isinstance(row, dict):
                    writer = csv.DictWriter(csvfile, field_names)
                    if header:
                        writer.writeheader()
                elif isinstance(row, Sequence):
                    writer = csv.writer(csvfile)
                    if header:
                        writer.writerow(field_names)
                else:
                    raise TypeError("Rows must be dictionaries or sequences.")

            writer.writerow(row)
    LOG.log(level, "End: Write.")
    return output_path


def table_to_points(
    dataset_path,
    output_path,
    x_field_name,
    y_field_name,
    spatial_reference_item=4326,
    **kwargs
):
    """Convert coordinate table to a new point dataset.

    Args:
        dataset_path (str): Path of the dataset.
        output_path (str): Path of the output dataset.
        x_field_name (str): Name of field with x-coordinate.
        y_field_name (str): Name of field with y-coordinate.
        spatial_reference_item: Item from which the spatial reference of the output
            geometry will be derived. Default is 4326 (EPSG code for unprojected WGS84).
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        z_field_name (str): Name of the field with z-coordinate.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        str: Path of the converted dataset.
    """
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("z_field_name")
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level, "Start: Convert `%s` to point dataset `%s`.", dataset_path, output_path
    )
    view_name = unique_name()
    arcpy.management.MakeXYEventLayer(
        table=dataset_path,
        out_layer=view_name,
        in_x_field=x_field_name,
        in_y_field=y_field_name,
        in_z_field=kwargs.get("z_field_name"),
        spatial_reference=spatial_reference(spatial_reference_item),
    )
    dataset.copy(
        view_name,
        output_path,
        dataset_where_sql=kwargs["dataset_where_sql"],
        log_level=logging.DEBUG,
    )
    dataset.delete(view_name, log_level=logging.DEBUG)
    LOG.log(level, "End: Convert.")
    return output_path
