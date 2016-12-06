"""Set-theoretic geometry operations."""
import logging

import arcpy

from arcetl import attributes
from arcetl import combo
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
        dataset_path (str): The path of the dataset.
        field_name (str): The name of the dataset's field to assign to.
        identity_dataset_path (str): The path of the identity dataset.
        identity_field_name (str): The name of identity dataset's field with
            values to assign.
        **kwargs: Arbitrary keyword arguments. See below

    Keyword Args:
        chunk_size (int): The number of features to process per loop.
        dataset_where_sql (str): The SQL where-clause for dataset subselection.
        identity_where_sql (str): The SQL where-clause for the identity
            dataset subselection.
        log_level (str): The level to log the function at.
        replacement_value: The value to replace identity field values with.
        tolerance (float): The tolerance for coincidence, in dataset's units.

    Returns:
        str: The path of the dataset updated.
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
    # Create a temporary copy of the overlay dataset.
    temp_overlay_path = dataset.copy(
        identity_dataset_path, helpers.unique_temp_dataset_path('overlay'),
        dataset_where_sql=kwargs.get('identity_where_sql'), log_level=None
        )
    # Avoid field name collisions with neutral holding field.
    temp_overlay_field_name = dataset.duplicate_field(
        temp_overlay_path, identity_field_name,
        new_field_name=helpers.unique_name(identity_field_name), log_level=None
        )
    attributes.update_by_function(
        temp_overlay_path, temp_overlay_field_name, function=(lambda x: x),
        field_as_first_arg=False, arg_field_names=[identity_field_name],
        log_level=None
        )
    for view_name in combo.view_chunks(
            dataset_path, kwargs.get('chunk_size', 4096),
            dataset_where_sql=kwargs.get('dataset_where_sql'), log_level=None
        ):
        temp_output_path = helpers.unique_temp_dataset_path('output')
        arcpy.analysis.Identity(
            in_features=view_name, identity_features=temp_overlay_path,
            out_feature_class=temp_output_path, join_attributes='all',
            cluster_tolerance=kwargs.get('tolerance'), relationship=False
            )
        # Push identity (or replacement) value from temp to update field.
        attributes.update_by_function(
            temp_output_path, field_name, update_function,
            field_as_first_arg=False, arg_field_names=[temp_overlay_field_name],
            log_level=None
            )
        # Replace original chunk features with identity features.
        features.delete(view_name, log_level=None)
        dataset.delete(view_name, log_level=None)
        features.insert_from_path(dataset_path, temp_output_path,
                                  log_level=None)
        dataset.delete(temp_output_path, log_level=None)
    dataset.delete(temp_overlay_path, log_level=None)
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
        dataset_path (str): The path of the dataset.
        field_name (str): The name of the dataset's field to assign to.
        overlay_dataset_path (str): The path of the overlay dataset.
        overlay_field_name (str): The name of overlay dataset's field with
            values to assign.
        **kwargs: Arbitrary keyword arguments. See below

    Keyword Args:
        chunk_size (int): The number of features to process per loop.
        dataset_where_sql (str): The SQL where-clause for dataset subselection.
        overlay_central_coincident (bool): The flag to overlay the centrally-
            coincident value.
        overlay_most_coincident (bool): The flag to overlay the most
            coincident value.
        overlay_where_sql (str): The SQL where-clause for the overlay dataset
            subselection.
        log_level (str): The level to log the function at.
        replacement_value: The value to replace overlay field values with.
        tolerance (float): The tolerance for coincidence, in dataset's units.

    Returns:
        str: The path of the dataset updated.
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
    if kwargs.get('tolerance') is not None:
        old_tolerance = arcpy.env.XYTolerance
        arcpy.env.XYTolerance = kwargs['tolerance']
    # Create temporary copy of overlay dataset.
    temp_overlay_path = dataset.copy(
        overlay_dataset_path, helpers.unique_temp_dataset_path('overlay'),
        dataset_where_sql=kwargs.get('overlay_where_sql'), log_level=None
        )
    # Avoid field name collisions with neutral holding field.
    temp_overlay_field_name = dataset.duplicate_field(
        temp_overlay_path, overlay_field_name,
        new_field_name=helpers.unique_name(overlay_field_name), log_level=None
        )
    attributes.update_by_function(
        temp_overlay_path, temp_overlay_field_name, function=(lambda x: x),
        field_as_first_arg=False, arg_field_names=[overlay_field_name],
        log_level=None
        )
    for view_name in combo.view_chunks(
            dataset_path, kwargs.get('chunk_size', 4096),
            dataset_where_sql=kwargs.get('dataset_where_sql'), log_level=None
        ):
        temp_output_path = helpers.unique_temp_dataset_path('output')
        arcpy.analysis.SpatialJoin(
            target_features=view_name, join_features=temp_overlay_path,
            out_feature_class=temp_output_path, **join_kwargs
            )
        attributes.update_by_function(
            temp_output_path, field_name, update_function,
            field_as_first_arg=False,
            arg_field_names=[temp_overlay_field_name], log_level=None
            )
        features.delete(view_name, log_level=None)
        dataset.delete(view_name, log_level=None)
        features.insert_from_path(dataset_path, temp_output_path,
                                  log_level=None)
        dataset.delete(temp_output_path, log_level=None)
    dataset.delete(temp_overlay_path, log_level=None)
    if kwargs['tolerance']:
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
        dataset_path (str): The path of the dataset.
        field_name (str): The name of the dataset's field to assign to.
        union_dataset_path (str): The path of the union dataset.
        union_field_name (str): The name of union dataset's field with values
            to assign.
        **kwargs: Arbitrary keyword arguments. See below

    Keyword Args:
        chunk_size (int): The number of features to process per loop.
        dataset_where_sql (str): The SQL where-clause for dataset subselection.
        union_where_sql (str): The SQL where-clause for the union dataset
            subselection.
        log_level (str): The level to log the function at.
        replacement_value: The value to replace overlay field values with.
        tolerance (float): The tolerance for coincidence, in dataset's units.

    Returns:
        str: The path of the dataset updated.
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
    # Create a temporary copy of the union dataset.
    temp_union_path = dataset.copy(
        union_dataset_path, helpers.unique_temp_dataset_path('union'),
        dataset_where_sql=kwargs.get('union_where_sql'), log_level=None
        )
    # Avoid field name collisions with neutral holding field.
    temp_union_field_name = dataset.duplicate_field(
        temp_union_path, union_field_name,
        new_field_name=helpers.unique_name(union_field_name), log_level=None
        )
    attributes.update_by_function(
        temp_union_path, temp_union_field_name, function=(lambda x: x),
        field_as_first_arg=False, arg_field_names=[union_field_name],
        log_level=None
        )
    for view_name in combo.view_chunks(
            dataset_path, kwargs.get('chunk_size', 4096),
            dataset_where_sql=kwargs.get('dataset_where_sql'), log_level=None
        ):
        temp_output_path = helpers.unique_temp_dataset_path('output')
        arcpy.analysis.Union(
            in_features=[view_name, temp_union_path],
            out_feature_class=temp_output_path, join_attributes='all',
            cluster_tolerance=kwargs.get('tolerance'), gaps=False
            )
        attributes.update_by_function(
            temp_output_path, field_name, update_function,
            field_as_first_arg=False, arg_field_names=[temp_union_field_name],
            log_level=None
            )
        features.delete(view_name, log_level=None)
        dataset.delete(view_name, log_level=None)
        features.insert_from_path(dataset_path, temp_output_path,
                                  log_level=None)
        dataset.delete(temp_output_path, log_level=None)
    dataset.delete(temp_union_path, log_level=None)
    LOG.log(log_level, "End: Union.")
    return dataset_path
