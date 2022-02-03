"""Set-theoretic geometry operations."""
import logging
from pathlib import Path

import arcpy

from arcproc.arcobj import DatasetView
from arcproc import attributes
from arcproc import dataset
from arcproc.dataset import TempDatasetCopy
from arcproc import features
from arcproc.helpers import unique_name, unique_path


LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""

arcpy.SetLogHistory(False)


def identity(
    dataset_path, field_name, identity_dataset_path, identity_field_name, **kwargs
):
    """Assign identity attribute, splitting features where necessary.

    Note:
        This function has a 'chunking' loop routine in order to avoid an unhelpful
        output error that occurs when the inputs are rather large. For some reason the
        identity will 'succeed' with an empty output warning, but not create an output
        dataset. Running the identity against smaller sets of data generally avoids
        this conundrum.

    Args:
        dataset_path (pathlib.Path, str): Path of the dataset.
        field_name (str): Name of field in dataset to assign to.
        identity_dataset_path (pathlib.Path, str): Path of the identity dataset.
        identity_field_name (str): Name of field in identity dataset with values to
            assign.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        chunk_size (int): Number of features to process per loop. Default is 4096.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        identity_where_sql (str): SQL where-clause for the identity dataset
            subselection.
        replacement_value: Value to replace identity field values with.
        tolerance (float): Tolerance for coincidence, in dataset's units.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        pathlib.Path: Path of the dataset updated.
    """
    dataset_path = Path(dataset_path)
    identity_dataset_path = Path(identity_dataset_path)
    kwargs.setdefault("chunk_size", 4096)
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("identity_where_sql")
    kwargs.setdefault("tolerance")
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level,
        "Start: Identity-set attributes in `%s.%s` by overlay values in `%s.%s`.",
        dataset_path,
        field_name,
        identity_dataset_path,
        identity_field_name,
    )
    dataset_view = DatasetView(dataset_path, kwargs["dataset_where_sql"])
    identity_copy = TempDatasetCopy(
        identity_dataset_path,
        dataset_where_sql=kwargs["identity_where_sql"],
        field_names=[identity_field_name],
        # BUG-000134367 - Cannot rename field later.
        output_path=unique_path(prefix="temp", workspace_path="in_memory"),
    )
    with dataset_view, identity_copy:
        # Avoid field name collisions with neutral holding field.
        temp_field_name = unique_name(identity_field_name, unique_length=1)
        if len(temp_field_name) > 31:
            temp_field_name = temp_field_name[len(temp_field_name) - 31 :]
        dataset.rename_field(
            identity_copy.path,
            identity_field_name,
            new_field_name=temp_field_name,
            log_level=logging.DEBUG,
        )
        for chunk_view in dataset_view.as_chunks(kwargs["chunk_size"]):
            temp_output_path = unique_path("output")
            arcpy.analysis.Identity(
                in_features=chunk_view.name,
                # ArcPy2.8.0: Convert to str.
                identity_features=str(identity_copy.path),
                # ArcPy2.8.0: Convert to str.
                out_feature_class=str(temp_output_path),
                join_attributes="ALL",
                cluster_tolerance=kwargs["tolerance"],
                relationship=False,
            )
            # Push identity value from temp to update field.
            # Identity puts empty string when identity feature not present; fix to None.
            attributes.update_by_function(
                temp_output_path,
                field_name,
                function=lambda x: None if x == "" else x,
                field_as_first_arg=False,
                arg_field_names=[temp_field_name],
                log_level=logging.DEBUG,
            )
            # Apply replacement value if necessary.
            if kwargs.get("replacement_value") is not None:
                attributes.update_by_function(
                    temp_output_path,
                    field_name,
                    function=lambda x: kwargs["replacement_value"] if x else None,
                    log_level=logging.DEBUG,
                )
            # Replace original chunk features with new features.
            features.delete(chunk_view.name, log_level=logging.DEBUG)
            features.insert_from_path(
                dataset_path, temp_output_path, log_level=logging.DEBUG
            )
            dataset.delete(temp_output_path, log_level=logging.DEBUG)
    LOG.log(level, "End: Identity.")
    return dataset_path


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

    dataset_view = DatasetView(dataset_path, kwargs["dataset_where_sql"])
    overlay_copy = TempDatasetCopy(
        overlay_dataset_path,
        kwargs["overlay_where_sql"],
        field_names=[overlay_field_name],
        # BUG-000134367 - Cannot rename field later.
        output_path=unique_path(prefix="temp", workspace_path="in_memory"),
    )
    with dataset_view, overlay_copy:
        # Avoid field name collisions with neutral field name.
        overlay_copy.field_name = dataset.rename_field(
            overlay_copy.path,
            overlay_field_name,
            new_field_name=unique_name(overlay_field_name, unique_length=1),
            log_level=logging.DEBUG,
        )
        if kwargs["tolerance"]:
            original_tolerance = arcpy.env.XYTolerance
            arcpy.env.XYTolerance = kwargs["tolerance"]
        for chunk_view in dataset_view.as_chunks(kwargs["chunk_size"]):
            temp_output_path = unique_path("output")
            arcpy.analysis.SpatialJoin(
                target_features=chunk_view.name,
                # ArcPy2.8.0: Convert to str.
                join_features=str(overlay_copy.path),
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
                arg_field_names=[overlay_copy.field_name],
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
                dataset_path, temp_output_path, log_level=logging.DEBUG
            )
            dataset.delete(temp_output_path, log_level=logging.DEBUG)
        if kwargs["tolerance"]:
            arcpy.env.XYTolerance = original_tolerance
    LOG.log(level, "End: Overlay.")
    return dataset_path


def union(dataset_path, field_name, union_dataset_path, union_field_name, **kwargs):
    """Assign union attribute to features, splitting where necessary.

    Note:
        This function has a 'chunking' loop routine in order to avoid an unhelpful
        output error that occurs when the inputs are rather large. For some reason the
        identity will 'succeed' with an empty output warning, but not create an output
        dataset. Running the identity against smaller sets of data generally avoids
        this conundrum.

    Args:
        dataset_path (pathlib.Path, str): Path of the dataset.
        field_name (str): Name of field in dataset to assign to.
        union_dataset_path (pathlib.Path, str): Path of the union dataset.
        union_field_name (str): Name of field in union dataset with values to assign.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        chunk_size (int): Number of features to process per loop. Default is 4096.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        union_where_sql (str): SQL where-clause for the union dataset subselection.
        replacement_value: Value to replace overlay field values with.
        tolerance (float): Tolerance for coincidence, in dataset's units.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        pathlib.Path: Path of the dataset updated.
    """
    dataset_path = Path(dataset_path)
    union_dataset_path = Path(union_dataset_path)
    kwargs.setdefault("chunk_size", 4096)
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("union_where_sql")
    kwargs.setdefault("tolerance")
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level,
        "Start: Union-set attributes in `%s.%s` by overlay values in `%s.%s`.",
        dataset_path,
        field_name,
        union_dataset_path,
        union_field_name,
    )
    dataset_view = DatasetView(dataset_path, kwargs["dataset_where_sql"])
    union_copy = TempDatasetCopy(
        union_dataset_path, kwargs["union_where_sql"], field_names=[union_field_name]
    )
    with dataset_view, union_copy:
        # Avoid field name collisions with neutral field name.
        union_copy.field_name = dataset.rename_field(
            union_copy.path,
            union_field_name,
            new_field_name=unique_name(union_field_name, unique_length=1),
            log_level=logging.DEBUG,
        )
        for chunk_view in dataset_view.as_chunks(kwargs["chunk_size"]):
            temp_output_path = unique_path("output")
            arcpy.analysis.Union(
                # ArcPy2.8.0: Convert to str.
                in_features=[chunk_view.name, str(union_copy.path)],
                # ArcPy2.8.0: Convert to str.
                out_feature_class=str(temp_output_path),
                join_attributes="ALL",
                cluster_tolerance=kwargs["tolerance"],
                gaps=False,
            )
            # Push union value from temp to update field.
            # Union puts empty string when union feature not present; fix to None.
            attributes.update_by_function(
                temp_output_path,
                field_name,
                function=lambda x: None if x == "" else x,
                field_as_first_arg=False,
                arg_field_names=[union_copy.field_name],
                log_level=logging.DEBUG,
            )
            # Apply replacement value if necessary.
            if kwargs.get("replacement_value") is not None:
                attributes.update_by_function(
                    temp_output_path,
                    field_name,
                    function=lambda x: kwargs["replacement_value"] if x else None,
                    log_level=logging.DEBUG,
                )
            # Replace original chunk features with new features.
            features.delete(chunk_view.name, log_level=logging.DEBUG)
            features.insert_from_path(
                dataset_path, temp_output_path, log_level=logging.DEBUG
            )
            dataset.delete(temp_output_path, log_level=logging.DEBUG)
    LOG.log(level, "End: Union.")
    return dataset_path
