"""Analysis result operations."""
import logging

import arcpy

from arcproc import arcobj
from arcproc import attributes
from arcproc import dataset
from arcproc.helpers import contain, unique_path


LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""

arcpy.SetLogHistory(False)


def buffer(dataset_path, output_path, distance, dissolve_field_names=None, **kwargs):
    """Buffer features a given distance & (optionally) dissolve on given fields.

    Args:
        dataset_path (str): Path of the dataset.
        output_path (str): Path of the output dataset.
        distance (float): Distance to buffer from feature, in the units of the dataset.
        dissolve_field_names (iter): Iterable of field names to dissolve on.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        str: Path of the output dataset.
    """
    level = kwargs.get("log_level", logging.INFO)
    keys = {"dissolve": tuple(contain(dissolve_field_names))}
    line = "Start: Buffer features in `{}` into `{}`".format(dataset_path, output_path)
    if keys["dissolve"]:
        line += " & dissolve on fields `{}`".format(keys["dissolve"])
    line += "."
    LOG.log(level, line)
    view = {
        "dataset": arcobj.DatasetView(dataset_path, kwargs.get("dataset_where_sql")),
    }
    with view["dataset"]:
        arcpy.analysis.Buffer(
            in_features=view["dataset"].name,
            out_feature_class=output_path,
            buffer_distance_or_field=distance,
            dissolve_option="list" if keys["dissolve"] else "none",
            dissolve_field=keys["dissolve"],
        )
    for field_name in ["BUFF_DIST", "ORIG_FID"]:
        arcpy.management.DeleteField(in_table=output_path, drop_field=field_name)
    LOG.log(level, "End: Buffer.")
    return output_path


def id_near_info_map(
    dataset_path,
    dataset_id_field_name,
    near_dataset_path,
    near_id_field_name,
    max_near_distance=None,
    **kwargs
):
    """Return mapping dictionary of feature IDs/near-feature info.

    Args:
        dataset_path (str): Path of the dataset.
        dataset_id_field_name (str): Name of ID field.
        near_dataset_path (str): Path of the near-dataset.
        near_id_field_name (str): Name of the near ID field.
        max_near_distance (float): Maximum distance to search for near-features, in
            units of the dataset's spatial reference.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        near_where_sql (str): SQL where-clause for near-dataset subselection.
        near_rank (int): Nearness rank of the feature to map info for. Default is 1.

    Returns:
        dict: Mapping of the dataset ID to a near-feature info dictionary.
            Info dictionary keys: "id", "near_id", "rank", "distance",
            "angle", "near_x", "near_y".
            "distance" value (float) will match linear unit of the dataset"s
            spatial reference.
            "angle" value (float) is in decimal degrees.
    """
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("near_where_sql")
    kwargs.setdefault("near_rank", 1)
    view = {
        "dataset": arcobj.DatasetView(dataset_path, kwargs["dataset_where_sql"]),
        "near": arcobj.DatasetView(near_dataset_path, kwargs["near_where_sql"]),
    }
    with view["dataset"], view["near"]:
        temp_near_path = unique_path("near")
        arcpy.analysis.GenerateNearTable(
            in_features=view["dataset"].name,
            near_features=view["near"].name,
            out_table=temp_near_path,
            search_radius=max_near_distance,
            location=True,
            angle=True,
            closest=False,
            closest_count=kwargs["near_rank"],
        )
        oid_id_map = attributes.id_values_map(
            view["dataset"].name, "oid@", dataset_id_field_name
        )
        near_oid_id_map = attributes.id_values_map(
            view["near"].name, "oid@", near_id_field_name
        )
    field_names = [
        "in_fid",
        "near_fid",
        "near_dist",
        "near_angle",
        "near_x",
        "near_y",
        "near_rank",
    ]
    near_info_map = {}
    for near_info in attributes.as_dicts(temp_near_path, field_names):
        if near_info["near_rank"] == kwargs["near_rank"]:
            _id = oid_id_map[near_info["in_fid"]]
            near_info_map[_id] = {
                "id": _id,
                "near_id": near_oid_id_map[near_info["near_fid"]],
                "rank": near_info["near_rank"],
                "distance": near_info["near_dist"],
                "angle": near_info["near_angle"],
                "near_x": near_info["near_x"],
                "near_y": near_info["near_y"],
            }
    dataset.delete(temp_near_path, log_level=logging.DEBUG)
    return near_info_map
