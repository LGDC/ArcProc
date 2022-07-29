"""Set-theoretic geometry operations."""
from collections import Counter
from logging import DEBUG, INFO, Logger, getLogger
from pathlib import Path
from typing import Any, Optional
from typing import Union as TUnion

from arcpy import SetLogHistory
from arcpy.analysis import Identity, SpatialJoin, Union

from arcproc.dataset import DatasetView, dataset_feature_count
from arcproc.field import delete_field, update_field_with_join, update_field_with_value
from arcproc.helpers import log_entity_states
from arcproc.metadata import Dataset


LOG: Logger = getLogger(__name__)
"""Module-level logger."""

SetLogHistory(False)


def identity_features(
    dataset_path: TUnion[Path, str],
    *,
    field_name: str,
    identity_path: TUnion[Path, str],
    identity_field_name: str,
    dataset_where_sql: Optional[str] = None,
    identity_where_sql: Optional[str] = None,
    output_path: TUnion[Path, str],
    replacement_value: Optional[Any] = None,
    log_level: int = INFO,
) -> Counter:
    """Intersect features (identity-style).


    Notes:
        Features with multiple identity-features will be split.

    Args:
        dataset_path: Path to dataset.
        field_name: Name of field to place identity values.
        identity_path: Path to identity-dataset.
        identity_field_name: Name of identity-field.
        dataset_where_sql: SQL where-clause for dataset subselection.
        identity_where_sql: SQL where-clause for the identity-dataset subselection.
        output_path: Path to output dataset.
        replacement_value: Value to replace a present identity-field value with. If set
            to None, no replacement will occur.
        log_level: Level to log the function at.

    Returns:
        Feature counts for original and output datasets.
    """
    dataset_path = Path(dataset_path)
    identity_path = Path(identity_path)
    LOG.log(
        log_level,
        "Start: Intersect features in `%s` with features in `%s` (identity-style).",
        dataset_path,
        identity_path,
    )
    states = Counter()
    states["in original dataset"] = dataset_feature_count(dataset_path)
    view = DatasetView(dataset_path, dataset_where_sql=dataset_where_sql)
    # Do not include any field names - we do not want them added to output.
    identity_view = DatasetView(
        identity_path, field_names=[], dataset_where_sql=identity_where_sql
    )
    with view, identity_view:
        # ArcPy2.8.0: Convert Path to str.
        Identity(
            in_features=view.name,
            identity_features=identity_view.name,
            out_feature_class=str(output_path),
            join_attributes="ALL",
            relationship=False,
        )
    fid_field_names = [
        name for name in Dataset(output_path).field_names if name.startswith("FID_")
    ]
    if replacement_value is not None:
        update_field_with_value(
            output_path,
            field_name,
            value=replacement_value,
            dataset_where_sql=f"{fid_field_names[-1]} <> -1",
            log_level=DEBUG,
        )
    else:
        update_field_with_join(
            output_path,
            field_name,
            key_field_names=[fid_field_names[-1]],
            join_dataset_path=identity_path,
            join_field_name=identity_field_name,
            join_key_field_names=["OID@"],
            dataset_where_sql=f"{fid_field_names[-1]} <> -1",
            join_dataset_where_sql=identity_where_sql,
            log_level=DEBUG,
        )
    update_field_with_value(
        output_path,
        field_name,
        value=None,
        dataset_where_sql=f"{fid_field_names[-1]} = -1",
        log_level=DEBUG,
    )
    for name in fid_field_names:
        delete_field(output_path, field_name=name, log_level=DEBUG)
    states["in output"] = dataset_feature_count(output_path)
    log_entity_states("features", states, logger=LOG, log_level=log_level)
    LOG.log(log_level, "End: Identity.")
    return states


def join_features_at_center(
    dataset_path: TUnion[Path, str],
    *,
    field_name: str,
    join_path: TUnion[Path, str],
    join_field_name: str,
    dataset_where_sql: Optional[str] = None,
    join_where_sql: Optional[str] = None,
    output_path: TUnion[Path, str],
    replacement_value: Optional[Any] = None,
    log_level: int = INFO,
) -> Counter:
    """Join features spatially at their center.

    Notes:
        Features joined with multiple join-features will be duplicated.

    Args:
        dataset_path: Path to dataset.
        field_name: Name of field to place joined values.
        join_path: Path to join-dataset.
        join_field_name: Name of join-field.
        dataset_where_sql: SQL where-clause for dataset subselection.
        join_where_sql: SQL where-clause for the join-dataset subselection.
        output_path: Path to output dataset.
        replacement_value: Value to replace a present join-field value with. If set to
            None, no replacement will occur.
        log_level: Level to log the function at.

    Returns:
        Feature counts for original and output datasets.
    """
    dataset_path = Path(dataset_path)
    join_path = Path(join_path)
    LOG.log(
        log_level,
        "Start: Join features in `%s` at their center with features in `%s`.",
        dataset_path,
        join_path,
    )
    states = Counter()
    states["in original dataset"] = dataset_feature_count(dataset_path)
    view = DatasetView(dataset_path, dataset_where_sql=dataset_where_sql)
    # Do not include any field names - we do not want them added to output.
    join_view = DatasetView(join_path, field_names=[], dataset_where_sql=join_where_sql)
    with view, join_view:
        # ArcPy2.8.0: Convert Path to str.
        SpatialJoin(
            target_features=view.name,
            join_features=join_view.name,
            out_feature_class=str(output_path),
            join_operation="JOIN_ONE_TO_MANY",
            join_type="KEEP_ALL",
            match_option="HAVE_THEIR_CENTER_IN",
        )
    if replacement_value is not None:
        update_field_with_value(
            output_path,
            field_name,
            value=replacement_value,
            dataset_where_sql="JOIN_FID <> -1",
            log_level=DEBUG,
        )
    else:
        update_field_with_join(
            output_path,
            field_name,
            key_field_names=["JOIN_FID"],
            join_dataset_path=join_path,
            join_field_name=join_field_name,
            join_key_field_names=["OID@"],
            dataset_where_sql="JOIN_FID <> -1",
            join_dataset_where_sql=join_where_sql,
            log_level=DEBUG,
        )
    update_field_with_value(
        output_path,
        field_name,
        value=None,
        dataset_where_sql="JOIN_FID = -1",
        log_level=DEBUG,
    )
    for name in ["Join_Count", "TARGET_FID", "JOIN_FID"]:
        delete_field(output_path, field_name=name, log_level=DEBUG)
    states["in output"] = dataset_feature_count(output_path)
    log_entity_states("features", states, logger=LOG, log_level=log_level)
    LOG.log(log_level, "End: Join.")
    return states


def union_features(
    dataset_path: TUnion[Path, str],
    *,
    field_name: str,
    union_path: TUnion[Path, str],
    union_field_name: str,
    dataset_where_sql: Optional[str] = None,
    union_where_sql: Optional[str] = None,
    output_path: TUnion[Path, str],
    replacement_value: Optional[Any] = None,
    log_level: int = INFO,
) -> Counter:
    """Union features.

    Notes:
        Features with multiple union-features will be split.

    Args:
        dataset_path: Path to dataset.
        field_name: Name of field to place union values.
        union_path: Path to union-dataset.
        union_field_name: Name of union-field.
        dataset_where_sql: SQL where-clause for dataset subselection.
        union_where_sql: SQL where-clause for the union-dataset subselection.
        replacement_value: Value to replace a present union-field value with. If set to
            None, no replacement will occur.
        log_level: Level to log the function at.

    Returns:
        Feature counts for original and output datasets.
    """
    dataset_path = Path(dataset_path)
    union_path = Path(union_path)
    LOG.log(
        log_level,
        "Start: Union features in `%s` with features in `%s`.",
        dataset_path,
        union_path,
    )
    states = Counter()
    states["in original dataset"] = dataset_feature_count(dataset_path)
    view = DatasetView(dataset_path, dataset_where_sql=dataset_where_sql)
    # Do not include any field names - we do not want them added to output.
    union_view = DatasetView(
        union_path, field_names=[], dataset_where_sql=union_where_sql
    )
    with view, union_view:
        # ArcPy2.8.0: Convert Path to str.
        Union(
            in_features=[view.name, union_view.name],
            out_feature_class=str(output_path),
            join_attributes="ALL",
        )
    fid_field_names = [
        name for name in Dataset(output_path).field_names if name.startswith("FID_")
    ]
    if replacement_value is not None:
        update_field_with_value(
            output_path,
            field_name,
            value=replacement_value,
            dataset_where_sql=f"{fid_field_names[-1]} <> -1",
            log_level=DEBUG,
        )
    else:
        update_field_with_join(
            output_path,
            field_name,
            key_field_names=[fid_field_names[-1]],
            join_dataset_path=union_path,
            join_field_name=union_field_name,
            join_key_field_names=["OID@"],
            dataset_where_sql=f"{fid_field_names[-1]} <> -1",
            join_dataset_where_sql=union_where_sql,
            log_level=DEBUG,
        )
    update_field_with_value(
        output_path,
        field_name,
        value=None,
        dataset_where_sql=f"{fid_field_names[-1]} = -1",
        log_level=DEBUG,
    )
    for name in fid_field_names:
        delete_field(output_path, field_name=name, log_level=DEBUG)
    states["in output"] = dataset_feature_count(output_path)
    log_entity_states("features", states, logger=LOG, log_level=log_level)
    LOG.log(log_level, "End: Union.")
    return states
