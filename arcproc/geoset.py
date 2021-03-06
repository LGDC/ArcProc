"""Set-theoretic geometry operations."""
import logging
import sys

import arcpy

from arcproc import arcobj
from arcproc import attributes
from arcproc import dataset
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
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the dataset's field to assign to.
        identity_dataset_path (str): Path of the identity dataset.
        identity_field_name (str): Name of identity dataset's field with values to
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
        str: Path of the dataset updated.
    """
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
    view = {"dataset": arcobj.DatasetView(dataset_path, kwargs["dataset_where_sql"])}
    # Create a temporary copy of the overlay dataset.
    temp_identity = arcobj.TempDatasetCopy(
        identity_dataset_path,
        kwargs["identity_where_sql"],
        field_names=[identity_field_name],
        # BUG-000134367 - Cannot rename field later.
        output_path=unique_path(prefix="temp", workspace_path="in_memory"),
    )
    with view["dataset"], temp_identity:
        # Avoid field name collisions with neutral holding field.
        temp_identity.field_name = dataset.rename_field(
            temp_identity.path,
            identity_field_name,
            new_field_name=unique_name(identity_field_name),
            log_level=logging.DEBUG,
        )
        for view["chunk"] in view["dataset"].as_chunks(kwargs["chunk_size"]):
            temp_output_path = unique_path("output")
            arcpy.analysis.Identity(
                in_features=view["chunk"].name,
                identity_features=temp_identity.path,
                out_feature_class=temp_output_path,
                join_attributes="all",
                cluster_tolerance=kwargs["tolerance"],
                relationship=False,
            )
            # Py2: Clean up bad or null geometry created in processing.
            if sys.version_info.major < 3:
                arcpy.management.RepairGeometry(temp_output_path)
            # Push identity value from temp to update field.
            # Identity puts empty string when identity feature not present; fix to null.
            attributes.update_by_function(
                temp_output_path,
                field_name,
                function=lambda x: None if x == "" else x,
                field_as_first_arg=False,
                arg_field_names=[temp_identity.field_name],
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
            features.delete(view["chunk"].name, log_level=logging.DEBUG)
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
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the dataset's field to assign to.
        overlay_dataset_path (str): Path of the overlay dataset.
        overlay_field_name (str): Name of overlay dataset's field with values to
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
        str: Path of the dataset updated.
    """
    kwargs.setdefault("chunk_size", 4096)
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("overlay_central_coincident", False)
    kwargs.setdefault("overlay_most_coincident", False)
    kwargs.setdefault("overlay_where_sql")
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level,
        "Start: Overlay-set attributes in `%s.%s` by overlay values in `%s.%s`.",
        dataset_path,
        field_name,
        overlay_dataset_path,
        overlay_field_name,
    )
    # Check flags & set details for spatial join call.
    join_kwargs = {"join_operation": "join_one_to_many", "join_type": "keep_all"}
    if kwargs["overlay_central_coincident"]:
        join_kwargs["match_option"] = "have_their_center_in"
    elif kwargs["overlay_most_coincident"]:
        raise NotImplementedError("overlay_most_coincident not yet implemented.")

    else:
        join_kwargs["match_option"] = "intersect"
    meta = {"orig_tolerance": arcpy.env.XYTolerance}
    view = {"dataset": arcobj.DatasetView(dataset_path, kwargs["dataset_where_sql"])}
    # Create temporary copy of overlay dataset.
    temp_overlay = arcobj.TempDatasetCopy(
        overlay_dataset_path,
        kwargs["overlay_where_sql"],
        field_names=[overlay_field_name],
    )
    with view["dataset"], temp_overlay:
        # Avoid field name collisions with neutral field name.
        temp_overlay.field_name = dataset.rename_field(
            temp_overlay.path,
            overlay_field_name,
            new_field_name=unique_name(overlay_field_name),
            log_level=logging.DEBUG,
        )
        if "tolerance" in kwargs:
            arcpy.env.XYTolerance = kwargs["tolerance"]
        for view["chunk"] in view["dataset"].as_chunks(kwargs["chunk_size"]):
            temp_output_path = unique_path("output")
            arcpy.analysis.SpatialJoin(
                target_features=view["chunk"].name,
                join_features=temp_overlay.path,
                out_feature_class=temp_output_path,
                **join_kwargs
            )
            # Py2: Clean up bad or null geometry created in processing.
            if sys.version_info.major < 3:
                arcpy.management.RepairGeometry(temp_output_path)
            # Push overlay value from temp to update field.
            attributes.update_by_function(
                temp_output_path,
                field_name,
                function=lambda x: x,
                field_as_first_arg=False,
                arg_field_names=[temp_overlay.field_name],
                # log_level=logging.DEBUG,
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
            features.delete(view["chunk"].name, log_level=logging.DEBUG)
            features.insert_from_path(
                dataset_path, temp_output_path, log_level=logging.DEBUG
            )
            dataset.delete(temp_output_path, log_level=logging.DEBUG)
        if "tolerance" in kwargs:
            arcpy.env.XYTolerance = meta["orig_tolerance"]
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
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the dataset's field to assign to.
        union_dataset_path (str): Path of the union dataset.
        union_field_name (str): Name of union dataset's field with values to assign.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        chunk_size (int): Number of features to process per loop. Default is 4096.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        union_where_sql (str): SQL where-clause for the union dataset subselection.
        replacement_value: Value to replace overlay field values with.
        tolerance (float): Tolerance for coincidence, in dataset's units.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        str: Path of the dataset updated.
    """
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
    view = {"dataset": arcobj.DatasetView(dataset_path, kwargs["dataset_where_sql"])}
    # Create a temporary copy of the union dataset.
    temp_union = arcobj.TempDatasetCopy(
        union_dataset_path, kwargs["union_where_sql"], field_names=[union_field_name]
    )
    with view["dataset"], temp_union:
        # Avoid field name collisions with neutral field name.
        temp_union.field_name = dataset.rename_field(
            temp_union.path,
            union_field_name,
            new_field_name=unique_name(union_field_name),
            log_level=logging.DEBUG,
        )
        for view["chunk"] in view["dataset"].as_chunks(kwargs["chunk_size"]):
            temp_output_path = unique_path("output")
            arcpy.analysis.Union(
                in_features=[view["chunk"].name, temp_union.path],
                out_feature_class=temp_output_path,
                join_attributes="all",
                cluster_tolerance=kwargs["tolerance"],
                gaps=False,
            )
            # Py2: Clean up bad or null geometry created in processing.
            if sys.version_info.major < 3:
                arcpy.management.RepairGeometry(temp_output_path)
            # Push union value from temp to update field.
            # Union puts empty string when union feature not present; fix to null.
            attributes.update_by_function(
                temp_output_path,
                field_name,
                function=lambda x: None if x == "" else x,
                field_as_first_arg=False,
                arg_field_names=[temp_union.field_name],
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
            features.delete(view["chunk"].name, log_level=logging.DEBUG)
            features.insert_from_path(
                dataset_path, temp_output_path, log_level=logging.DEBUG
            )
            dataset.delete(temp_output_path, log_level=logging.DEBUG)
    LOG.log(level, "End: Union.")
    return dataset_path
