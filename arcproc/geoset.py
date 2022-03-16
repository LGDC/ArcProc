"""Set-theoretic geometry operations."""
from collections import Counter
import logging
from pathlib import Path
from typing import Any, Optional, Union

import arcpy

from arcproc import attributes
from arcproc import dataset
from arcproc.dataset import DatasetView
from arcproc.dataset import TempDatasetCopy
from arcproc import features
from arcproc.helpers import log_entity_states, unique_name, unique_path
from arcproc.metadata import Dataset


LOG: logging.Logger = logging.getLogger(__name__)
"""Module-level logger."""


arcpy.SetLogHistory(False)


def identity(
    dataset_path: Union[Path, str],
    *,
    field_name: str,
    identity_path: Union[Path, str],
    identity_field_name: str,
    dataset_where_sql: Optional[str] = None,
    identity_where_sql: Optional[str] = None,
    output_path: Union[Path, str],
    replacement_value: Optional[Any] = None,
    log_level: int = logging.INFO,
) -> Counter:
    """Assign identity attribute, splitting features where necessary.

    Args:
        dataset_path: Path to dataset.
        field_name: Name of field to place identity values.
        identity_path: Path to identity-dataset.
        identity_field_name: Name of identity-field.
        dataset_where_sql: SQL where-clause for dataset subselection.
        identity_where_sql: SQL where-clause for the identity-dataset subselection.
        output_path: Path to output dataset.
        replacement_value: Value to replace a present overlay-field value with. If set
            to None, no replacement will occur.
        log_level: Level to log the function at.

    Returns:
        Feature counts for original and output datasets.
    """
    dataset_path = Path(dataset_path)
    identity_path = Path(identity_path)
    LOG.log(
        log_level,
        "Start: Identity-set attributes in `%s.%s` by overlay values in `%s.%s`.",
        dataset_path,
        field_name,
        identity_path,
        identity_field_name,
    )
    states = Counter()
    states["in original dataset"] = dataset.feature_count(dataset_path)
    view = DatasetView(dataset_path, dataset_where_sql=dataset_where_sql)
    # Do not include any field names - we do not want them added to output.
    identity_view = DatasetView(
        identity_path, field_names=[], dataset_where_sql=identity_where_sql
    )
    with view, identity_view:
        # ArcPy2.8.0: Convert Path to str.
        arcpy.analysis.Identity(
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
        attributes.update_by_value(
            output_path,
            field_name,
            value=replacement_value,
            dataset_where_sql=f"{fid_field_names[-1]} <> -1",
            log_level=logging.DEBUG,
        )
    else:
        attributes.update_by_joined_value(
            output_path,
            field_name,
            key_field_names=[fid_field_names[-1]],
            join_dataset_path=identity_path,
            join_field_name=identity_field_name,
            join_key_field_names=["OID@"],
            dataset_where_sql=f"{fid_field_names[-1]} <> -1",
            join_dataset_where_sql=identity_where_sql,
            log_level=logging.DEBUG,
        )
    attributes.update_by_value(
        output_path,
        field_name,
        value=None,
        dataset_where_sql=f"{fid_field_names[-1]} = -1",
        log_level=logging.DEBUG,
    )
    for name in fid_field_names:
        dataset.delete_field(output_path, field_name=name, log_level=logging.DEBUG)
    states["in output"] = dataset.feature_count(output_path)
    log_entity_states("features", states, logger=LOG, log_level=log_level)
    LOG.log(log_level, "End: Identity.")
    return states


def overlay(
    dataset_path, field_name, overlay_dataset_path, overlay_field_name, **kwargs
):
    """Assign overlay attribute to features, splitting where necessary.

    Note:
        Only one overlay flag at a time can be used. If mutliple are set to True, the
        first one referenced in the code will be used. If no overlay flags are set, the
        operation will perform a basic intersection check, and the result will be at
        the whim of the geoprocessing environment's merge rule for the update field.

        This function has a 'chunking' loop routine in order to avoid an unhelpful
        output error that occurs when the inputs are rather large. For some reason the
        identity will 'succeed' with an empty output warning, but not create an output
        dataset. Running the identity against smaller sets of data generally avoids this
        conundrum.

    Args:
        dataset_path (pathlib.Path, str): Path of the dataset.
        field_name (str): Name of field in dataset to assign to.
        overlay_dataset_path (pathlib.Path, str): Path of the overlay dataset.
        overlay_field_name (str): Name of field in overlay dataset with values to
            assign.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        chunk_size (int): Number of features to process per loop. Default is 4096.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        overlay_central_coincident (bool): Flag to overlay the centrally-coincident
            value. Default is False.
        overlay_most_coincident (bool): Flag to overlay the most coincident value.
            Default is False.
        overlay_where_sql (str): SQL where-clause for the overlay dataset subselection.
        replacement_value: Value to replace overlay field values with.
        tolerance (float): Tolerance for coincidence, in dataset's units.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        pathlib.Path: Path of the dataset updated.
    """
    dataset_path = Path(dataset_path)
    overlay_dataset_path = Path(overlay_dataset_path)
    kwargs.setdefault("chunk_size", 4096)
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("overlay_central_coincident", False)
    kwargs.setdefault("overlay_most_coincident", False)
    kwargs.setdefault("overlay_where_sql")
    kwargs.setdefault("replacement_value")
    kwargs.setdefault("tolerance")
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level,
        "Start: Overlay-set attributes in `%s.%s` by overlay values in `%s.%s`.",
        dataset_path,
        field_name,
        overlay_dataset_path,
        overlay_field_name,
    )
    if kwargs["overlay_most_coincident"]:
        raise NotImplementedError("overlay_most_coincident not yet implemented")

    dataset_view = DatasetView(
        dataset_path, dataset_where_sql=kwargs["dataset_where_sql"]
    )
    temp_overlay = TempDatasetCopy(
        overlay_dataset_path,
        # BUG-000134367 - Cannot rename field later.
        copy_path=unique_path(prefix="temp", workspace_path="in_memory"),
        field_names=[overlay_field_name],
        dataset_where_sql=kwargs["overlay_where_sql"],
    )
    with dataset_view, temp_overlay:
        # Avoid field name collisions with neutral field name.
        temp_overlay.field_name = dataset.rename_field(
            temp_overlay.copy_path,
            field_name=overlay_field_name,
            new_field_name=unique_name(overlay_field_name, unique_length=1),
            log_level=logging.DEBUG,
        ).name
        if kwargs["tolerance"]:
            original_tolerance = arcpy.env.XYTolerance
            arcpy.env.XYTolerance = kwargs["tolerance"]
        for chunk_view in dataset_view.as_chunks(kwargs["chunk_size"]):
            temp_output_path = unique_path("output")
            arcpy.analysis.SpatialJoin(
                target_features=chunk_view.name,
                # ArcPy2.8.0: Convert to str.
                join_features=str(temp_overlay.copy_path),
                # ArcPy2.8.0: Convert to str.
                out_feature_class=str(temp_output_path),
                join_operation="JOIN_ONE_TO_MANY",
                join_type="KEEP_ALL",
                match_option=(
                    "HAVE_THEIR_CENTER_IN"
                    if kwargs["overlay_central_coincident"]
                    else "INTERSECT"
                ),
            )
            # Push overlay value from temp to update field.
            attributes.update_by_function(
                temp_output_path,
                field_name,
                function=lambda x: x,
                field_as_first_arg=False,
                arg_field_names=[temp_overlay.field_name],
                log_level=logging.DEBUG,
            )
            # Apply replacement value if necessary.
            if kwargs["replacement_value"] is not None:
                attributes.update_by_function(
                    temp_output_path,
                    field_name,
                    function=lambda x: kwargs["replacement_value"] if x else None,
                    log_level=logging.DEBUG,
                )
            # Replace original chunk features with new features.
            features.delete(chunk_view.name, log_level=logging.DEBUG)
            features.insert_from_path(
                dataset_path, source_path=temp_output_path, log_level=logging.DEBUG
            )
            # ArcPy2.8.0: Convert to str.
            arcpy.management.Delete(str(temp_output_path))
        if kwargs["tolerance"]:
            arcpy.env.XYTolerance = original_tolerance
    LOG.log(level, "End: Overlay.")
    return dataset_path


def union(
    dataset_path: Union[Path, str],
    *,
    field_name: str,
    union_path: Union[Path, str],
    union_field_name: str,
    dataset_where_sql: Optional[str] = None,
    union_where_sql: Optional[str] = None,
    output_path: Union[Path, str],
    replacement_value: Optional[Any] = None,
    log_level: int = logging.INFO,
) -> Counter:
    """Assign union attribute to features, splitting where necessary.

    Args:
        dataset_path: Path to dataset.
        field_name: Name of field to place union values.
        union_path: Path to union-dataset.
        union_field_name: Name of union-field.
        dataset_where_sql: SQL where-clause for dataset subselection.
        union_where_sql: SQL where-clause for the union-dataset subselection.
        replacement_value: Value to replace a present overlay-field value with. If set
            to None, no replacement will occur.
        log_level: Level to log the function at.

    Returns:
        Feature counts for original and output datasets.
    """
    dataset_path = Path(dataset_path)
    union_path = Path(union_path)
    LOG.log(
        log_level,
        "Start: Union-set attributes in `%s.%s` by overlay values in `%s.%s`.",
        dataset_path,
        field_name,
        union_path,
        union_field_name,
    )
    states = Counter()
    states["in original dataset"] = dataset.feature_count(dataset_path)
    view = DatasetView(dataset_path, dataset_where_sql=dataset_where_sql)
    # Do not include any field names - we do not want them added to output.
    union_view = DatasetView(
        union_path, field_names=[], dataset_where_sql=union_where_sql
    )
    with view, union_view:
        # ArcPy2.8.0: Convert Path to str.
        arcpy.analysis.Union(
            in_features=[view.name, union_view.name],
            out_feature_class=str(output_path),
            join_attributes="ALL",
        )
    fid_field_names = [
        name for name in Dataset(output_path).field_names if name.startswith("FID_")
    ]
    if replacement_value is not None:
        attributes.update_by_value(
            output_path,
            field_name,
            value=replacement_value,
            dataset_where_sql=f"{fid_field_names[-1]} <> -1",
            log_level=logging.DEBUG,
        )
    else:
        attributes.update_by_joined_value(
            output_path,
            field_name,
            key_field_names=[fid_field_names[-1]],
            join_dataset_path=union_path,
            join_field_name=union_field_name,
            join_key_field_names=["OID@"],
            dataset_where_sql=f"{fid_field_names[-1]} <> -1",
            join_dataset_where_sql=union_where_sql,
            log_level=logging.DEBUG,
        )
    attributes.update_by_value(
        output_path,
        field_name,
        value=None,
        dataset_where_sql=f"{fid_field_names[-1]} = -1",
        log_level=logging.DEBUG,
    )
    for name in fid_field_names:
        dataset.delete_field(output_path, field_name=name, log_level=logging.DEBUG)
    states["in output"] = dataset.feature_count(output_path)
    log_entity_states("features", states, logger=LOG, log_level=log_level)
    LOG.log(log_level, "End: Union.")
    return states
