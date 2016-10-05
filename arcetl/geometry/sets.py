# -*- coding=utf-8 -*-
"""Geometric set operations."""
import logging

import arcpy

from arcetl import attributes, dataset, features
from arcetl.helpers import (CHUNK_WHERE_SQL_TEMPLATE, LOG_LEVEL_MAP,
                            unique_name, unique_temp_dataset_path)


LOG = logging.getLogger(__name__)


def identity_features(dataset_path, field_name, identity_dataset_path,
                      identity_field_name, **kwargs):
    """Assign unique identity value to features, splitting where necessary.

    replacement_value is a value that will substitute as the identity
    value.
    This method has a 'chunking' routine in order to avoid an
    unhelpful output error that occurs when the inputs are rather large.
    For some reason, the identity will 'succeed' with and empty output
    warning, but not create an output dataset. Running the identity against
    smaller sets of data generally avoids this conundrum.

    Args:
        dataset_path (str): Path of dataset.
        field_name (str): Name of field.
        identity_dataset_path (str): Path of identity-dataset.
        identity_field_name (str): Name of identity-field.
    Kwargs:
        tolerance (float): Tolerance for coincidence, in dataset's units.
        replacement_value: Value to replace present identity field value with.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        chunk_size (int): Number of features to process per loop iteration.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [
            ('chunk_size', 4096), ('dataset_where_sql', None),
            ('log_level', 'info'), ('replacement_value', None),
            ('tolerance', None)
        ]:
        kwargs.setdefault(*kwarg_default)
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(
        log_level, ("Start: Identity-overlay features in %s's field %s"
                    " using features in %s's field %s."),
        dataset_path, field_name, identity_dataset_path, identity_field_name
        )
    if kwargs['replacement_value'] is not None:
        update_function = (lambda x: kwargs['replacement_value'] if x else None)
    else:
        # Identity puts empty string when identity feature not present.
        # Fix to null (replacement value function does this inherently).
        update_function = (lambda x: None if x == '' else x)
    # Create a temporary copy of the overlay dataset.
    temp_overlay_path = dataset.copy(
        identity_dataset_path, unique_temp_dataset_path('overlay'),
        log_level=None
        )
    # Avoid field name collisions with neutral holding field.
    temp_overlay_field_name = dataset.duplicate_field(
        temp_overlay_path, identity_field_name,
        new_field_name=unique_name(identity_field_name), log_level=None
        )
    attributes.update_by_function(
        temp_overlay_path, temp_overlay_field_name, function=(lambda x: x),
        field_as_first_arg=False, arg_field_names=[identity_field_name],
        log_level=None
        )
    # Get an iterable of all object IDs in the dataset.
    # Sorting is important, allows views with ID range instead of list.
    oids = sorted(oid for oid, in attributes.as_iters(
        dataset_path, ['oid@'], dataset_where_sql=kwargs['dataset_where_sql']
        ))
    while oids:
        # Get subset OIDs & remove them from full set.
        chunk = oids[:kwargs['chunk_size']]
        oids = oids[kwargs['chunk_size']:]
        LOG.debug("Chunk: Feature OIDs %s to %s", chunk[0], chunk[-1])
        # ArcPy where clauses cannot use 'between'.
        chunk_sql = CHUNK_WHERE_SQL_TEMPLATE.format(
            field=dataset.metadata(dataset_path)['oid_field_name'],
            from_oid=chunk[0], to_oid=chunk[-1]
            )
        if kwargs['dataset_where_sql']:
            chunk_sql += " and ({})".format(kwargs['dataset_where_sql'])
        chunk_view_name = dataset.create_view(
            unique_name('view'), dataset_path,
            dataset_where_sql=chunk_sql, log_level=None
            )
        # Create temporary dataset with the identity values.
        temp_output_path = unique_temp_dataset_path('output')
        arcpy.analysis.Identity(
            in_features=chunk_view_name, identity_features=temp_overlay_path,
            out_feature_class=temp_output_path, join_attributes='all',
            cluster_tolerance=kwargs['tolerance'], relationship=False
            )
        # Push identity (or replacement) value from temp to update field.
        attributes.update_by_function(
            temp_output_path, field_name, update_function,
            field_as_first_arg=False,
            arg_field_names=[temp_overlay_field_name], log_level=None
            )
        # Replace original chunk features with identity features.
        features.delete(chunk_view_name, log_level=None)
        dataset.delete(chunk_view_name, log_level=None)
        features.insert_from_path(dataset_path, temp_output_path,
                                  log_level=None)
        dataset.delete(temp_output_path, log_level=None)
    dataset.delete(temp_overlay_path, log_level=None)
    LOG.log(log_level, "End: Identity.")
    return dataset_path


def overlay_features(dataset_path, field_name, overlay_dataset_path,
                     overlay_field_name, **kwargs):
    """Assign overlay value to features, splitting where necessary.

    Please note that only one overlay flag at a time can be used. If
    mutliple are set to True, the first one referenced in the code
    will be used. If no overlay flags are set, the operation will perform a
    basic intersection check, and the result will be at the whim of the
    geoprocessing environment's merge rule for the update field.
    replacement_value is a value that will substitute as the identity
    value.
    This method has a 'chunking' routine in order to avoid an
    unhelpful output error that occurs when the inputs are rather large.
    For some reason, the identity will 'succeed' with and empty output
    warning, but not create an output dataset. Running the identity against
    smaller sets of data generally avoids this conundrum.

    Args:
        dataset_path (str): Path of dataset.
        field_name (str): Name of field.
        overlay_dataset_path (str): Path of overlay-dataset.
        overlay_field_name (str): Name of overlay-field.
    Kwargs:
        overlay_most_coincident (bool): Flag indicating overlay using most
            coincident value.
        overlay_central_coincident (bool): Flag indicating overlay using
            centrally-coincident value.
        tolerance (float): Tolerance for coincidence, in dataset's units.
        replacement_value: Value to replace present overlay-field value with.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        chunk_size (int): Number of features to process per loop iteration.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [
            ('chunk_size', 4096), ('dataset_where_sql', None),
            ('log_level', 'info'), ('overlay_central_coincident', False),
            ('overlay_most_coincident', False), ('replacement_value', None),
            ('tolerance', None)
        ]:
        kwargs.setdefault(*kwarg_default)
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(
        log_level, ("Start: Overlay features in %s's field %s"
                    " using features in %s's field %s."),
        dataset_path, field_name, overlay_dataset_path, overlay_field_name
        )
    # Check flags & set details for spatial join call.
    if kwargs['overlay_most_coincident']:
        raise NotImplementedError(
            "overlay_most_coincident not yet implemented."
            )
    elif kwargs['overlay_central_coincident']:
        join_kwargs = {'join_operation': 'join_one_to_many',
                       'join_type': 'keep_all',
                       'match_option': 'have_their_center_in'}
    else:
        join_kwargs = {'join_operation': 'join_one_to_many',
                       'join_type': 'keep_all',
                       'match_option': 'intersect'}
    if kwargs['replacement_value'] is not None:
        update_function = (lambda x: kwargs['replacement_value'] if x else None)
    else:
        update_function = (lambda x: x)
    # Create temporary copy of overlay dataset.
    temp_overlay_path = dataset.copy(
        overlay_dataset_path, unique_temp_dataset_path('overlay'),
        log_level=None
        )
    # Avoid field name collisions with neutral holding field.
    temp_overlay_field_name = dataset.duplicate_field(
        temp_overlay_path, overlay_field_name,
        new_field_name=unique_name(overlay_field_name), log_level=None
        )
    attributes.update_by_function(
        temp_overlay_path, temp_overlay_field_name, function=(lambda x: x),
        field_as_first_arg=False, arg_field_names=[overlay_field_name],
        log_level=None
        )
    # Get an iterable of all object IDs in the dataset.
    # Sorting is important, allows views with ID range instead of list.
    oids = sorted(oid for (oid,) in attributes.as_iters(
        dataset_path, ['oid@'], dataset_where_sql=kwargs['dataset_where_sql']
        ))
    while oids:
        chunk = oids[:kwargs['chunk_size']]
        oids = oids[kwargs['chunk_size']:]
        LOG.debug("Chunk: Feature OIDs %s to %s", chunk[0], chunk[-1])
        # ArcPy where clauses cannot use 'between'.
        chunk_sql = CHUNK_WHERE_SQL_TEMPLATE.format(
            field=dataset.metadata(dataset_path)['oid_field_name'],
            from_oid=chunk[0], to_oid=chunk[-1]
            )
        if kwargs['dataset_where_sql']:
            chunk_sql += " and ({})".format(kwargs['dataset_where_sql'])
        chunk_view_name = dataset.create_view(
            unique_name('view'), dataset_path,
            dataset_where_sql=chunk_sql, log_level=None
            )
        # Create the temp output of the overlay.
        if kwargs['tolerance']:
            old_tolerance = arcpy.env.XYTolerance
            arcpy.env.XYTolerance = kwargs['tolerance']
        temp_output_path = unique_temp_dataset_path('output')
        arcpy.analysis.SpatialJoin(
            target_features=chunk_view_name, join_features=temp_overlay_path,
            out_feature_class=temp_output_path, **join_kwargs
            )
        if kwargs['tolerance']:
            arcpy.env.XYTolerance = old_tolerance
        # Push overlay (or replacement) value from temp to update field.
        attributes.update_by_function(
            temp_output_path, field_name, update_function,
            field_as_first_arg=False,
            arg_field_names=[temp_overlay_field_name], log_level=None
            )
        # Replace original chunk features with overlay features.
        features.delete(chunk_view_name, log_level=None)
        dataset.delete(chunk_view_name, log_level=None)
        features.insert_from_path(dataset_path, temp_output_path,
                                  log_level=None)
        dataset.delete(temp_output_path, log_level=None)
    dataset.delete(temp_overlay_path, log_level=None)
    LOG.log(log_level, "End: Overlay.")
    return dataset_path


def union_features(dataset_path, field_name, union_dataset_path,
                   union_field_name, **kwargs):
    """Assign unique union value to features, splitting where necessary.

    replacement_value is a value that will substitute as the union value.

    Args:
        dataset_path (str): Path of dataset.
        field_name (str): Name of field.
        union_dataset_path (str): Path of union-dataset.
        union_field_name (str): Name of union-field.
    Kwargs:
        tolerance (float): Tolerance for coincidence, in dataset's units.
        replacement_value: Value to replace present union-field value with.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        chunk_size (int): Number of features to process per loop iteration.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [
            ('chunk_size', 4096), ('dataset_where_sql', None),
            ('log_level', 'info'), ('replacement_value', None),
            ('tolerance', None)
        ]:
        kwargs.setdefault(kwarg_default)
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(
        log_level, ("Start: Union-overlay features in %s's field %s"
                    " using features in %s's field %s."),
        dataset_path, field_name, union_dataset_path, union_field_name
        )
    if kwargs['replacement_value'] is not None:
        update_function = (lambda x: kwargs['replacement_value'] if x else None)
    else:
        # Union puts empty string when identity feature not present.
        # Fix to null (replacement value function does this inherently).
        update_function = (lambda x: None if x == '' else x)
    # Create a temporary copy of the union dataset.
    temp_union_path = dataset.copy(
        union_dataset_path, unique_temp_dataset_path('union'),
        log_level=None
        )
    # Avoid field name collisions with neutral holding field.
    temp_union_field_name = dataset.duplicate_field(
        temp_union_path, union_field_name,
        new_field_name=unique_name(union_field_name), log_level=None
        )
    attributes.update_by_function(
        temp_union_path, temp_union_field_name, function=(lambda x: x),
        field_as_first_arg=False, arg_field_names=[union_field_name],
        log_level=None
        )
    # Sorting is important, allows views with ID range instead of list.
    oids = sorted(oid for (oid,) in attributes.as_iters(
        dataset_path, ['oid@'], dataset_where_sql=kwargs['dataset_where_sql']
        ))
    while oids:
        chunk = oids[:kwargs['chunk_size']]
        oids = oids[kwargs['chunk_size']:]
        LOG.debug("Chunk: Feature OIDs %s to %s", chunk[0], chunk[-1])
        # ArcPy where clauses cannot use 'between'.
        chunk_sql = CHUNK_WHERE_SQL_TEMPLATE.format(
            field=dataset.metadata(dataset_path)['oid_field_name'],
            from_oid=chunk[0], to_oid=chunk[-1]
            )
        if kwargs['dataset_where_sql']:
            chunk_sql += " and ({})".format(kwargs['dataset_where_sql'])
        chunk_view_name = dataset.create_view(
            unique_name('chunk_view'), dataset_path,
            dataset_where_sql=chunk_sql, log_level=None
            )
        # Create the temp output of the union.
        temp_output_path = unique_temp_dataset_path('output')
        arcpy.analysis.Union(
            in_features=[chunk_view_name, temp_union_path],
            out_feature_class=temp_output_path, join_attributes='all',
            cluster_tolerance=kwargs['tolerance'], gaps=False
            )
        # Push union (or replacement) value from temp to update field.
        attributes.update_by_function(
            temp_output_path, field_name, update_function,
            field_as_first_arg=False, arg_field_names=[temp_union_field_name],
            log_level=None
            )
        # Replace original chunk features with union features.
        features.delete(chunk_view_name, log_level=None)
        dataset.delete(chunk_view_name, log_level=None)
        features.insert_from_path(dataset_path, temp_output_path,
                                  log_level=None)
        dataset.delete(temp_output_path, log_level=None)
    dataset.delete(temp_union_path, log_level=None)
    LOG.log(log_level, "End: Union.")
    return dataset_path
