"""Set-theoretic geometry operations."""
import logging

import arcpy

from arcetl import arcobj
from arcetl import attributes
from arcetl import dataset
from arcetl import features
from arcetl import helpers


LOG = logging.getLogger(__name__)


def identity(dataset_path, field_name, identity_dataset_path,
             identity_field_name, **kwargs):
    """Assign identity attribute, splitting features where necessary.

    Note:
        This function has a 'chunking' loop routine in order to avoid an
        unhelpful output error that occurs when the inputs are rather large.
        For some reason the identity will 'succeed' with an empty output
        warning, but not create an output dataset. Running the identity
        against smaller sets of data generally avoids this conundrum.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the dataset's field to assign to.
        identity_dataset_path (str): Path of the identity dataset.
        identity_field_name (str): Name of identity dataset's field with
            values to assign.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        chunk_size (int): Number of features to process per loop. Defaults to
            4096.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        identity_where_sql (str): SQL where-clause for the identity dataset
            subselection.
        log_level (str): Level to log the function at. Defaults to 'info'.
        replacement_value: Value to replace identity field values with.
        tolerance (float): Tolerance for coincidence, in dataset's units.

    Returns:
        str: Path of the dataset updated.
    """
    log_level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(log_level, ("Start: Identity-set attributes in %s on %s"
                        " by overlay values in %s on %s."), field_name,
            dataset_path, identity_field_name, identity_dataset_path)
    if kwargs.get('replacement_value') is not None:
        update_function = (lambda x: kwargs['replacement_value'] if x else None)
    else:
        # Identity puts empty string when identity feature not present.
        # Fix to null (replacement value function does this inherently).
        update_function = (lambda x: None if x == '' else x)
    dataset_view = arcobj.DatasetView(dataset_path,
                                      kwargs.get('dataset_where_sql'))
    # Create a temporary copy of the overlay dataset.
    temp_identity = arcobj.TempDatasetCopy(identity_dataset_path,
                                           kwargs.get('identity_where_sql'))
    with dataset_view, temp_identity:
        # Avoid field name collisions with neutral holding field.
        temp_identity.field_name = dataset.duplicate_field(
            temp_identity.path, identity_field_name,
            new_field_name=helpers.unique_name(identity_field_name),
            log_level=None
            )
        attributes.update_by_function(
            temp_identity.path, temp_identity.field_name,
            function=(lambda x: x), field_as_first_arg=False,
            arg_field_names=(identity_field_name,), log_level=None
            )
        for chunk_view in dataset_view.as_chunks(
                kwargs.get('chunk_size', 4096)
            ):
            temp_output_path = helpers.unique_temp_dataset_path('output')
            arcpy.analysis.Identity(
                in_features=chunk_view.name,
                identity_features=temp_identity.path,
                out_feature_class=temp_output_path, join_attributes='all',
                cluster_tolerance=kwargs.get('tolerance'), relationship=False
                )
            # Clean up bad or null geometry created in processing.
            arcpy.management.RepairGeometry(in_features=temp_output_path)
            # Push identity (or replacement) value from temp to update field.
            attributes.update_by_function(
                temp_output_path, field_name, update_function,
                field_as_first_arg=False,
                arg_field_names=(temp_identity.field_name,), log_level=None
                )
            # Replace original chunk features with identity features.
            features.delete(chunk_view.name, log_level=None)
            features.insert_from_path(dataset_path, temp_output_path,
                                      log_level=None)
            dataset.delete(temp_output_path, log_level=None)
    LOG.log(log_level, "End: Identity.")
    return dataset_path


def overlay(dataset_path, field_name, overlay_dataset_path, overlay_field_name,
            **kwargs):
    """Assign overlay attribute to features, splitting where necessary.

    Note:
        Only one overlay flag at a time can be used. If mutliple are set to
        True, the first one referenced in the code will be used. If no
        overlay flags are set, the operation will perform a basic
        intersection check, and the result will be at the whim of the
        geoprocessing environment's merge rule for the update field.

        This function has a 'chunking' loop routine in order to avoid an
        unhelpful output error that occurs when the inputs are rather large.
        For some reason the identity will 'succeed' with an empty output
        warning, but not create an output dataset. Running the identity
        against smaller sets of data generally avoids this conundrum.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the dataset's field to assign to.
        overlay_dataset_path (str): Path of the overlay dataset.
        overlay_field_name (str): Name of overlay dataset's field with values
            to assign.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        chunk_size (int): Number of features to process per loop. Defaults to
            4096.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        overlay_central_coincident (bool): Flag to overlay the centrally-
            coincident value. Defaults to False.
        overlay_most_coincident (bool): Flag to overlay the most
            coincident value. Defaults to False.
        overlay_where_sql (str): SQL where-clause for the overlay dataset
            subselection.
        log_level (str): Level to log the function at. Defaults to 'info'.
        replacement_value: Value to replace overlay field values with.
        tolerance (float): Tolerance for coincidence, in dataset's units.

    Returns:
        str: Path of the dataset updated.
    """
    log_level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(log_level, ("Start: Overlay-set attributes in %s on %s"
                        " by overlay values in %s on %s."),
            field_name, dataset_path, overlay_field_name, overlay_dataset_path)
    # Check flags & set details for spatial join call.
    if kwargs.get('overlay_most_coincident'):
        raise NotImplementedError(
            "overlay_most_coincident not yet implemented."
            )
    elif kwargs.get('overlay_central_coincident'):
        join_kwargs = {'join_operation': 'join_one_to_many',
                       'join_type': 'keep_all',
                       'match_option': 'have_their_center_in'}
    else:
        join_kwargs = {'join_operation': 'join_one_to_many',
                       'join_type': 'keep_all',
                       'match_option': 'intersect'}
    if kwargs.get('replacement_value') is not None:
        update_function = (lambda x: kwargs['replacement_value'] if x else None)
    else:
        update_function = (lambda x: x)
    dataset_view = arcobj.DatasetView(dataset_path,
                                      kwargs.get('dataset_where_sql'))
    # Create temporary copy of overlay dataset.
    temp_overlay = arcobj.TempDatasetCopy(overlay_dataset_path,
                                          kwargs.get('overlay_where_sql'))
    with dataset_view, temp_overlay:
        # Avoid field name collisions with neutral holding field.
        temp_overlay.field_name = dataset.duplicate_field(
            temp_overlay.path, overlay_field_name,
            new_field_name=helpers.unique_name(overlay_field_name),
            log_level=None
            )
        attributes.update_by_function(
            temp_overlay.path, temp_overlay.field_name, function=(lambda x: x),
            field_as_first_arg=False, arg_field_names=(overlay_field_name,),
            log_level=None
            )
        if kwargs.get('tolerance') is not None:
            old_tolerance = arcpy.env.XYTolerance
            arcpy.env.XYTolerance = kwargs['tolerance']
        for chunk_view in dataset_view.as_chunks(
                kwargs.get('chunk_size', 4096)
            ):
            temp_output_path = helpers.unique_temp_dataset_path('output')
            arcpy.analysis.SpatialJoin(target_features=chunk_view.name,
                                       join_features=temp_overlay.path,
                                       out_feature_class=temp_output_path,
                                       **join_kwargs)
            # Clean up bad or null geometry created in processing.
            arcpy.management.RepairGeometry(in_features=temp_output_path)
            # Push identity (or replacement) value from temp to update field.
            attributes.update_by_function(
                temp_output_path, field_name, update_function,
                field_as_first_arg=False,
                arg_field_names=(temp_overlay.field_name,), log_level=None
                )
            features.delete(chunk_view.name, log_level=None)
            features.insert_from_path(dataset_path, temp_output_path,
                                      log_level=None)
            dataset.delete(temp_output_path, log_level=None)
        if kwargs.get('tolerance') is not None:
            arcpy.env.XYTolerance = old_tolerance
        LOG.log(log_level, "End: Overlay.")
    return dataset_path


def union(dataset_path, field_name, union_dataset_path, union_field_name,
          **kwargs):
    """Assign union attribute to features, splitting where necessary.

    Note:
        This function has a 'chunking' loop routine in order to avoid an
        unhelpful output error that occurs when the inputs are rather large.
        For some reason the identity will 'succeed' with an empty output
        warning, but not create an output dataset. Running the identity
        against smaller sets of data generally avoids this conundrum.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the dataset's field to assign to.
        union_dataset_path (str): Path of the union dataset.
        union_field_name (str): Name of union dataset's field with values to
            assign.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        chunk_size (int): Number of features to process per loop. Defaults to
            4096.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        union_where_sql (str): SQL where-clause for the union dataset
            subselection.
        log_level (str): Level to log the function at. Defaults to 'info'.
        replacement_value: Value to replace overlay field values with.
        tolerance (float): Tolerance for coincidence, in dataset's units.

    Returns:
        str: Path of the dataset updated.
    """
    log_level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(log_level, ("Start: Union-set attributes in %s on %s"
                        " by overlay values in %s on %s."),
            field_name, dataset_path, union_field_name, union_dataset_path)
    if kwargs.get('replacement_value') is not None:
        update_function = (lambda x: kwargs['replacement_value'] if x else None)
    else:
        # Union puts empty string when identity feature not present.
        # Fix to null (replacement value function does this inherently).
        update_function = (lambda x: None if x == '' else x)
    dataset_view = arcobj.DatasetView(dataset_path,
                                      kwargs.get('dataset_where_sql'))
    # Create a temporary copy of the union dataset.
    temp_union = arcobj.TempDatasetCopy(union_dataset_path,
                                        kwargs.get('union_where_sql'))
    with dataset_view, temp_union:
        # Avoid field name collisions with neutral holding field.
        temp_union.field_name = dataset.duplicate_field(
            temp_union.path, union_field_name,
            new_field_name=helpers.unique_name(union_field_name), log_level=None
            )
        attributes.update_by_function(
            temp_union.path, temp_union.field_name, function=(lambda x: x),
            field_as_first_arg=False, arg_field_names=(union_field_name,),
            log_level=None
            )
        for chunk_view in dataset_view.as_chunks(
                kwargs.get('chunk_size', 4096)
            ):
            temp_output_path = helpers.unique_temp_dataset_path('output')
            arcpy.analysis.Union(
                in_features=(chunk_view.name, temp_union.path),
                out_feature_class=temp_output_path, join_attributes='all',
                cluster_tolerance=kwargs.get('tolerance'), gaps=False
                )
            # Clean up bad or null geometry created in processing.
            arcpy.management.RepairGeometry(in_features=temp_output_path)
            # Push identity (or replacement) value from temp to update field.
            attributes.update_by_function(
                temp_output_path, field_name, update_function,
                field_as_first_arg=False,
                arg_field_names=(temp_union.field_name,), log_level=None
                )
            features.delete(chunk_view.name, log_level=None)
            features.insert_from_path(dataset_path, temp_output_path,
                                      log_level=None)
            dataset.delete(temp_output_path, log_level=None)
    LOG.log(log_level, "End: Union.")
    return dataset_path
