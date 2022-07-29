"""Proximity-related operations."""
from collections import Counter
from logging import DEBUG, INFO, Logger, getLogger
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, Optional, Set, Tuple, Union

from arcpy import SetLogHistory
from arcpy.analysis import Buffer, Clip, Erase, GenerateNearTable, PolygonNeighbors
from arcpy.management import Dissolve

from arcproc.dataset import (
    DatasetView,
    add_field,
    dataset_feature_count,
    delete_dataset,
    unique_dataset_path,
)
from arcproc.features import features_as_dicts, features_as_tuples
from arcproc.field import delete_field
from arcproc.helpers import log_entity_states
from arcproc.metadata import Dataset


LOG: Logger = getLogger(__name__)
"""Module-level logger."""

SetLogHistory(False)


def adjacent_neighbors_map(
    dataset_path: Union[Path, str],
    *,
    id_field_names: Iterable[str],
    dataset_where_sql: Optional[str] = None,
    exclude_overlap: bool = False,
    include_corner: bool = False,
) -> Dict[Union[Tuple[Any], Any], Set[Union[Tuple[Any], Any]]]:
    """Return mapping of feature ID to set of adjacent feature IDs.

    Notes:
        Only works for polygon geometries.
        If id_field_names only has one field name, feature IDs in the mapping will be
            the single value of that field, not a tuple.

    Args:
        dataset_path: Path to dataset.
        id_field_names: Names of the feature ID fields.
        dataset_where_sql: SQL where-clause for dataset subselection.
        exclude_overlap: Exclude features that overlap, but do not have adjacent edges
            or nodes if True.
        include_corner: Include features that have adjacent corner nodes, but no
            adjacent edges if True.
    """
    dataset_path = Path(dataset_path)
    id_field_names = list(id_field_names)
    # Lowercase to avoid casing mismatch.
    # id_field_names = [name.lower() for name in id_field_names]
    view = DatasetView(
        dataset_path, field_names=id_field_names, dataset_where_sql=dataset_where_sql
    )
    with view:
        temp_neighbor_path = unique_dataset_path("neighbor")
        # ArcPy2.8.0: Convert Path to str.
        PolygonNeighbors(
            in_features=view.name,
            out_table=str(temp_neighbor_path),
            in_fields=id_field_names,
            area_overlap=not exclude_overlap,
            both_sides=True,
        )
    adjacent_neighbors = {}
    for row in features_as_dicts(temp_neighbor_path):
        # Lowercase to avoid casing mismatch.
        row = {key.lower(): val for key, val in row.items()}
        if len(id_field_names) == 1:
            source_id = row[f"src_{id_field_names[0]}"]
            neighbor_id = row[f"nbr_{id_field_names[0]}"]
        else:
            source_id = tuple(row[f"src_{name}"] for name in id_field_names)
            neighbor_id = tuple(row[f"nbr_{name}"] for name in id_field_names)
        if source_id not in adjacent_neighbors:
            adjacent_neighbors[source_id] = set()
        if not include_corner and not row["length"] and not row["area"]:
            continue

        adjacent_neighbors[source_id].add(neighbor_id)
    delete_dataset(temp_neighbor_path)
    return adjacent_neighbors


def buffer_features(
    dataset_path: Union[Path, str],
    *,
    dataset_where_sql: Optional[str] = None,
    output_path: Union[Path, str],
    distance: Union[float, int],
    log_level: int = INFO,
) -> Counter:
    """Buffer feature geometries a given distance.

    Args:
        dataset_path: Path to dataset.
        dataset_where_sql: SQL where-clause for dataset subselection.
        output_path: Path to output dataset.
        distance: Distance to buffer from feature, in the units of the dataset.
        log_level: Level to log the function at.

    Returns:
        Feature counts for original and output datasets.
    """
    dataset_path = Path(dataset_path)
    output_path = Path(output_path)
    LOG.log(log_level, "Start: Buffer features in `%s`.", dataset_path)
    states = Counter()
    states["in original dataset"] = dataset_feature_count(dataset_path)
    view = DatasetView(dataset_path, dataset_where_sql=dataset_where_sql)
    with view:
        # ArcPy2.8.0: Convert Path to str.
        Buffer(
            in_features=view.name,
            out_feature_class=str(output_path),
            buffer_distance_or_field=distance,
        )
    for field_name in ["BUFF_DIST", "ORIG_FID"]:
        delete_field(output_path, field_name=field_name)
    states["in output"] = dataset_feature_count(output_path)
    log_entity_states("features", states, logger=LOG, log_level=log_level)
    LOG.log(log_level, "End: Buffer.")
    return states


def clip_features(
    dataset_path: Union[Path, str],
    *,
    clip_path: Union[Path, str],
    dataset_where_sql: Optional[str] = None,
    clip_where_sql: Optional[str] = None,
    output_path: Union[Path, str],
    log_level: int = INFO,
) -> Counter:
    """Clip feature geometries where it overlaps clip-dataset geometries.

    Args:
        dataset_path: Path to dataset.
        clip_path: Path to clip-dataset.
        dataset_where_sql: SQL where-clause for dataset subselection.
        clip_where_sql: SQL where-clause for clip-dataset subselection.
        output_path: Path to output dataset.
        log_level: Level to log the function at.

    Returns:
        Feature counts for original and output datasets.
    """
    clip_path = Path(clip_path)
    dataset_path = Path(dataset_path)
    output_path = Path(output_path)
    LOG.log(
        log_level,
        "Start: Clip features in `%s` where overlapping `%s`.",
        dataset_path,
        clip_path,
    )
    states = Counter()
    states["in original dataset"] = dataset_feature_count(dataset_path)
    view = DatasetView(dataset_path, dataset_where_sql=dataset_where_sql)
    clip_view = DatasetView(clip_path, dataset_where_sql=clip_where_sql)
    with view, clip_view:
        # ArcPy2.8.0: Convert Path to str.
        Clip(
            in_features=view.name,
            clip_features=clip_view.name,
            out_feature_class=str(output_path),
        )
    states["in output"] = dataset_feature_count(output_path)
    log_entity_states("features", states, logger=LOG, log_level=log_level)
    LOG.log(log_level, "End: Clip.")
    return states


def dissolve_features(
    dataset_path: Union[Path, str],
    *,
    dataset_where_sql: Optional[str] = None,
    output_path: Union[Path, str],
    dissolve_field_names: Optional[Iterable[str]] = None,
    all_fields_in_output: bool = False,
    allow_multipart: bool = True,
    unsplit_lines: bool = False,
    log_level: int = INFO,
) -> Counter:
    """Dissolve feature geometries that share values in given fields.

    Args:
        dataset_path: Path to dataset.
        dataset_where_sql: SQL where-clause for dataset subselection.
        output_path: Path to output dataset.
        dissolve_field_names: Names of fields to base dissolve on.
        all_fields_in_output: All fields in the dataset will persist in the output
            dataset if True. Otherwise, only the dissolve fields will persist. Non-
            dissolve fields will have default values, of course.
        allow_multipart: Allow multipart features in output if True.
        unsplit_lines: Merge line features when endpoints meet without crossing features
            if True.
        log_level: Level to log the function at.

    Returns:
        Feature counts for original and output datasets.
    """
    dataset_path = Path(dataset_path)
    output_path = Path(output_path)
    if dissolve_field_names is not None:
        dissolve_field_names = list(dissolve_field_names)
    LOG.log(
        log_level,
        "Start: Dissolve features in `%s` on fields `%s`.",
        dataset_path,
        dissolve_field_names,
    )
    states = Counter()
    states["in original dataset"] = dataset_feature_count(dataset_path)
    view = DatasetView(
        dataset_path,
        field_names=dissolve_field_names,
        dataset_where_sql=dataset_where_sql,
    )
    with view:
        # ArcPy2.8.0: Convert Path to str.
        Dissolve(
            in_features=view.name,
            out_feature_class=str(output_path),
            dissolve_field=dissolve_field_names,
            multi_part=allow_multipart,
            unsplit_lines=unsplit_lines,
        )
    if all_fields_in_output:
        for _field in Dataset(dataset_path).user_fields:
            # Cannot add a non-nullable field to existing features.
            _field.is_nullable = True
            add_field(
                output_path,
                exist_ok=True,
                log_level=DEBUG,
                **_field.field_as_dict,
            )
    states["in output"] = dataset_feature_count(output_path)
    log_entity_states("features", states, logger=LOG, log_level=log_level)
    LOG.log(log_level, "End: Dissolve.")
    return states


def erase_features(
    dataset_path: Union[Path, str],
    *,
    erase_path: Union[Path, str],
    dataset_where_sql: Optional[str] = None,
    erase_where_sql: Optional[str] = None,
    output_path: Union[Path, str],
    log_level: int = INFO,
) -> Counter:
    """Erase feature geometries where it overlaps erase-dataset geometries.

    Args:
        dataset_path: Path to dataset.
        erase_path: Path to erase-dataset.
        dataset_where_sql: SQL where-clause for dataset subselection.
        erase_where_sql: SQL where-clause for erase-dataset subselection.
        output_path: Path to output dataset.
        log_level: Level to log the function at.

    Returns:
        Feature counts for original and output datasets.
    """
    dataset_path = Path(dataset_path)
    erase_path = Path(erase_path)
    output_path = Path(output_path)
    LOG.log(
        log_level,
        "Start: Erase features in `%s` where overlapping features in `%s`.",
        dataset_path,
        erase_path,
    )
    states = Counter()
    states["in original dataset"] = dataset_feature_count(dataset_path)
    view = DatasetView(dataset_path, dataset_where_sql=dataset_where_sql)
    erase_view = DatasetView(
        erase_path, field_names=[], dataset_where_sql=erase_where_sql
    )
    with view, erase_view:
        # ArcPy2.8.0: Convert Path to str.
        Erase(
            in_features=view.name,
            erase_features=erase_view.name,
            out_feature_class=str(output_path),
        )
    states["in output"] = dataset_feature_count(output_path)
    log_entity_states("features", states, logger=LOG, log_level=log_level)
    LOG.log(log_level, "End: Erase.")
    return states


def nearest_features(
    dataset_path: Union[Path, str],
    *,
    id_field_name: str,
    near_path: Union[Path, str],
    near_id_field_name: str,
    dataset_where_sql: Optional[str] = None,
    near_where_sql: Optional[str] = None,
    max_distance: Optional[Union[float, int]] = None,
    near_rank: int = 1,
) -> Iterator[Dict[str, Any]]:
    """Generate info dictionaries for relationship with Nth-nearest near-feature.

    Args:
        dataset_path: Path to dataset.
        id_field_name: Name of dataset ID field.
        near_path: Path to near-dataset.
        near_id_field_name: Name of the near-dataset ID field.
        dataset_where_sql: SQL where-clause for dataset subselection.
        near_where_sql: SQL where-clause for near-dataset subselection.
        max_distance: Maximum distance to search for near-features, in units of the
            dataset.
        near_rank: Nearness rank of the feature to map info for (Nth-nearest).

    Yields:
        Nearest feature details.
        Keys:
            * dataset_id
            * near_id
            * angle: Angle from dataset feature & near-feature, in decimal degrees.
            * distance: Distance between feature & near-feature, in units of the
                dataset.
    """
    dataset_path = Path(dataset_path)
    near_path = Path(near_path)
    view = DatasetView(dataset_path, dataset_where_sql=dataset_where_sql)
    near_view = DatasetView(near_path, dataset_where_sql=near_where_sql)
    with view, near_view:
        temp_near_path = unique_dataset_path("near")
        # ArcPy2.8.0: Convert Path to str.
        GenerateNearTable(
            in_features=view.name,
            near_features=near_view.name,
            out_table=str(temp_near_path),
            search_radius=max_distance,
            angle=True,
            closest=(near_rank == 1),
            closest_count=near_rank,
        )
        oid_id_map = dict(
            features_as_tuples(view.name, field_names=["OID@", id_field_name])
        )
        near_oid_id_map = dict(
            features_as_tuples(near_view.name, field_names=["OID@", near_id_field_name])
        )
    _features = features_as_dicts(
        temp_near_path,
        field_names=["IN_FID", "NEAR_FID", "NEAR_ANGLE", "NEAR_DIST"],
        dataset_where_sql=f"NEAR_RANK = {near_rank}" if near_rank != 1 else None,
    )
    for feature in _features:
        yield {
            "dataset_id": oid_id_map[feature["IN_FID"]],
            "near_id": near_oid_id_map[feature["NEAR_FID"]],
            "angle": feature["NEAR_ANGLE"],
            "distance": feature["NEAR_DIST"],
        }

    delete_dataset(temp_near_path)
