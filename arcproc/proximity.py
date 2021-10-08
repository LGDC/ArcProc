"""Analysis result operations."""
from collections import Counter
import logging

import arcpy

from arcproc import arcobj
from arcproc import attributes
from arcproc import dataset
from arcproc.helpers import contain, log_entity_states, unique_path


LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""

arcpy.SetLogHistory(False)


def adjacent_neighbors_map(dataset_path, id_field_names, **kwargs):
    """Return mapping of feature ID to set of adjacent feature IDs.

    Only works for polygon geometries.

    Args:
        dataset_path (str): Path of dataset.
        id_field_names (iter): Ordered collection of fields used to identify a feature.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection. Default is
            None.
        exclude_overlap (bool): Exclude features that overlap, but do not have adjacent
            edges or nodes if True. Default is False.
        include_corner (bool): Include features that have adjacent corner nodes, but no
            adjacent edges. Default is False.
        tolerance (float): Tolerance for coincidence, in units of the dataset.

    Returns:
        dict
    """
    keys = {"id": list(contain(id_field_names))}
    keys["id"] = [key.lower() for key in keys["id"]]
    view = {
        "dataset": arcobj.DatasetView(
            dataset_path, kwargs.get("dataset_where_sql"), field_names=keys["id"],
        ),
    }
    with view["dataset"]:
        # Shim: Convert to str.
        temp_neighbor_path = str(unique_path("near"))
        arcpy.analysis.PolygonNeighbors(
            in_features=view["dataset"].name,
            out_table=temp_neighbor_path,
            in_fields=keys["id"],
            area_overlap=(not kwargs.get("exclude_overlap", False)),
            both_sides=True,
            cluster_tolerance=kwargs.get("tolerance"),
        )
    neighbors_map = {}
    for row in attributes.as_dicts(temp_neighbor_path):
        row = {key.lower(): val for key, val in row.items()}
        if len(keys["id"]) == 1:
            source_id = row["src_" + keys["id"][0]]
            neighbor_id = row["nbr_" + keys["id"][0]]
        else:
            source_id = tuple(row["src_" + key] for key in keys["id"])
            neighbor_id = tuple(row["nbr_" + key] for key in keys["id"])
        if source_id not in neighbors_map:
            neighbors_map[source_id] = set()
        if not kwargs.get("include_corner") and not row["length"] and not row["area"]:
            continue

        neighbors_map[source_id].add(neighbor_id)
    dataset.delete(temp_neighbor_path, log_level=logging.DEBUG)
    return neighbors_map


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
            dissolve_option="LIST" if keys["dissolve"] else "NONE",
            dissolve_field=keys["dissolve"],
        )
    for field_name in ["BUFF_DIST", "ORIG_FID"]:
        arcpy.management.DeleteField(in_table=output_path, drop_field=field_name)
    LOG.log(level, "End: Buffer.")
    return output_path


def clip(dataset_path, clip_dataset_path, output_path, **kwargs):
    """Clip feature geometry where it overlaps clip-dataset geometry.

    Args:
        dataset_path (str): Path of the dataset.
        clip_dataset_path (str): Path of dataset whose features define the clip area.
        output_path (str): Path of the output dataset.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        clip_where_sql (str): SQL where-clause for clip dataset subselection.
        tolerance (float): Tolerance for coincidence, in dataset's units.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        collections.Counter: Counts of features for each clip-state.
    """
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level,
        "Start: Clip features in `%s` where overlapping `%s` into `%s`.",
        dataset_path,
        clip_dataset_path,
        output_path,
    )
    view = {
        "clip": arcobj.DatasetView(clip_dataset_path, kwargs.get("clip_where_sql")),
        "dataset": arcobj.DatasetView(dataset_path, kwargs.get("dataset_where_sql")),
    }
    with view["dataset"], view["clip"]:
        arcpy.analysis.Clip(
            in_features=view["dataset"].name,
            clip_features=view["clip"].name,
            out_feature_class=output_path,
            cluster_tolerance=kwargs.get("tolerance"),
        )
        states = Counter()
        states["remaining"] = dataset.feature_count(output_path)
        states["deleted"] = view["dataset"].count - states["remaining"]
    log_entity_states("features", states, LOG, log_level=level)
    LOG.log(level, "End: Clip.")
    return states


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
        # Shim: Convert to str.
        temp_near_path = str(unique_path("near"))
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
