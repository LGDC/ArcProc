# -*- coding=utf-8 -*-
"""Geometric set operations."""
import logging

import arcpy

from .. import arcwrap, fields, helpers, values
from arcetl import attributes, features
from ..metadata import dataset_metadata, feature_count


CHUNK_WHERE_SQL_TEMPLATE = "{field} >= {from_oid} and {field} <= {to_oid}"
LOG = logging.getLogger(__name__)


# Features/attributes.

def clip_features(dataset_path, clip_dataset_path, **kwargs):
    """Clip feature geometry where overlapping clip-geometry.

    Args:
        dataset_path (str): Path of dataset.
        clip_dataset_path (str): Path of dataset defining clip area.
    Kwargs:
        tolerance (float): Tolerance for coincidence, in dataset's units.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        clip_where_sql (str): SQL where-clause for clip dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [
            ('clip_where_sql', None), ('dataset_where_sql', None),
            ('log_level', 'info'), ('tolerance', None)
        ]:
        kwargs.setdefault(*kwarg_default)
    log_level = helpers.LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Clip features in %s where overlapping %s.",
            dataset_path, clip_dataset_path)
    LOG.log(log_level, "%s features in dataset.", feature_count(dataset_path))
    dataset_view_name = arcwrap.create_dataset_view(
        helpers.unique_name('view'), dataset_path,
        dataset_where_sql=kwargs['dataset_where_sql']
        )
    clip_dataset_view_name = arcwrap.create_dataset_view(
        helpers.unique_name('view'), clip_dataset_path,
        dataset_where_sql=kwargs['clip_where_sql']
        )
    temp_output_path = helpers.unique_temp_dataset_path('output')
    arcpy.analysis.Clip(
        in_features=dataset_view_name, clip_features=clip_dataset_view_name,
        out_feature_class=temp_output_path,
        cluster_tolerance=kwargs['tolerance']
        )
    arcwrap.delete_dataset(clip_dataset_view_name)
    # Load back into the dataset.
    features.delete(dataset_view_name)
    arcwrap.delete_dataset(dataset_view_name)
    features.insert_from_path(dataset_path, temp_output_path)
    arcwrap.delete_dataset(temp_output_path)
    LOG.log(log_level, "%s features in dataset.", feature_count(dataset_path))
    LOG.log(log_level, "End: Clip.")
    return dataset_path


def dissolve_features(dataset_path, dissolve_field_names=None, **kwargs):
    """Merge features that share values in given fields.

    Args:
        dataset_path (str): Path of dataset.
        dissolve_field_names (iter): Iterable of field names to dissolve on.
    Kwargs:
        multipart (bool): Flag indicating if dissolve should create multipart
            features.
        unsplit_lines (bool): Flag indicating if dissolving lines should merge
            features when endpoints meet without a crossing feature.
        tolerance (float): Tolerance for coincidence, in dataset's units.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [
            ('dataset_where_sql', None), ('log_level', 'info'),
            ('multipart', True), ('tolerance', 0.001),
            ('unsplit_lines', False)
        ]:
        kwargs.setdefault(*kwarg_default)
    log_level = helpers.LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Dissolve features in %s on fields: %s.",
            dataset_path, dissolve_field_names)
    LOG.log(log_level, "%s features in dataset.", feature_count(dataset_path))
    if kwargs['tolerance']:
        old_tolerance = arcpy.env.XYTolerance
        arcpy.env.XYTolerance = kwargs['tolerance']
    dataset_view_name = arcwrap.create_dataset_view(
        helpers.unique_name('view'), dataset_path,
        dataset_where_sql=kwargs['dataset_where_sql'])
    temp_output_path = helpers.unique_temp_dataset_path('output')
    arcpy.management.Dissolve(
        in_features=dataset_view_name, out_feature_class=temp_output_path,
        dissolve_field=dissolve_field_names, multi_part=kwargs['multipart'],
        unsplit_lines=kwargs['unsplit_lines']
        )
    if kwargs['tolerance']:
        arcpy.env.XYTolerance = old_tolerance
    # Delete undissolved features that are now dissolved (in the temp).
    features.delete(dataset_view_name)
    arcwrap.delete_dataset(dataset_view_name)
    # Copy the dissolved features (in the temp) to the dataset.
    features.insert_from_path(dataset_path, temp_output_path)
    arcwrap.delete_dataset(temp_output_path)
    LOG.log(log_level, "%s features in dataset.", feature_count(dataset_path))
    LOG.log(log_level, "End: Dissolve.")
    return dataset_path


def erase_features(dataset_path, erase_dataset_path, **kwargs):
    """Erase feature geometry where overlaps erase dataset geometry.

    Args:
        dataset_path (str): Path of dataset.
        erase_dataset_path (str): Path of erase-dataset.
    Kwargs:
        tolerance (float): Tolerance for coincidence, in dataset's units.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        erase_where_sql (str): SQL where-clause for erase-dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [
            ('dataset_where_sql', None), ('erase_where_sql', None),
            ('log_level', 'info'), ('tolerance', None)
        ]:
        kwargs.setdefault(*kwarg_default)
    log_level = helpers.LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Erase features in %s where overlapping %s.",
            dataset_path, erase_dataset_path)
    LOG.log(log_level, "%s features in dataset.", feature_count(dataset_path))
    dataset_view_name = arcwrap.create_dataset_view(
        helpers.unique_name('view'), dataset_path,
        dataset_where_sql=kwargs['dataset_where_sql']
        )
    erase_dataset_view_name = arcwrap.create_dataset_view(
        helpers.unique_name('view'), erase_dataset_path,
        dataset_where_sql=kwargs['erase_where_sql']
        )
    temp_output_path = helpers.unique_temp_dataset_path('output')
    arcpy.analysis.Erase(
        in_features=dataset_view_name, erase_features=erase_dataset_view_name,
        out_feature_class=temp_output_path,
        cluster_tolerance=kwargs['tolerance']
        )
    arcwrap.delete_dataset(erase_dataset_view_name)
    # Load back into the dataset.
    features.delete(dataset_view_name)
    arcwrap.delete_dataset(dataset_view_name)
    features.insert_from_path(dataset_path, temp_output_path)
    arcwrap.delete_dataset(temp_output_path)
    LOG.log(log_level, "%s features in dataset.", feature_count(dataset_path))
    LOG.log(log_level, "End: Erase.")
    return dataset_path


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
    log_level = helpers.LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(
        log_level, ("Start: Identity-overlay features in %s's field %s"
                    " using features in %s's field %s."),
        dataset_path, field_name, identity_dataset_path, identity_field_name
        )
    LOG.log(log_level, "%s features in dataset.", feature_count(dataset_path))
    if kwargs['replacement_value'] is not None:
        update_function = (lambda x: kwargs['replacement_value'] if x else None)
    else:
        # Identity puts empty string when identity feature not present.
        # Fix to null (replacement value function does this inherently).
        update_function = (lambda x: None if x == '' else x)
    # Create a temporary copy of the overlay dataset.
    temp_overlay_path = arcwrap.copy_dataset(
        identity_dataset_path, helpers.unique_temp_dataset_path('overlay')
        )
    # Avoid field name collisions with neutral holding field.
    temp_overlay_field_name = fields.duplicate_field(
        temp_overlay_path, identity_field_name,
        new_field_name=helpers.unique_name(identity_field_name),
        duplicate_values=True, log_level=None
        )
    # Get an iterable of all object IDs in the dataset.
    # Sorting is important, allows views with ID range instead of list.
    oids = sorted(
        oid for oid, in values.features_as_iters(
            dataset_path, field_names=['oid@'],
            dataset_where_sql=kwargs['dataset_where_sql'])
        )
    while oids:
        # Get subset OIDs & remove them from full set.
        chunk = oids[:kwargs['chunk_size']]
        oids = oids[kwargs['chunk_size']:]
        LOG.debug("Chunk: Feature OIDs %s to %s", chunk[0], chunk[-1])
        # ArcPy where clauses cannot use 'between'.
        chunk_sql = CHUNK_WHERE_SQL_TEMPLATE.format(
            field=dataset_metadata(dataset_path)['oid_field_name'],
            from_oid=chunk[0], to_oid=chunk[-1]
            )
        if kwargs['dataset_where_sql']:
            chunk_sql += " and ({})".format(kwargs['dataset_where_sql'])
        chunk_view_name = arcwrap.create_dataset_view(
            helpers.unique_name('view'), dataset_path,
            dataset_where_sql=chunk_sql
            )
        # Create temporary dataset with the identity values.
        temp_output_path = helpers.unique_temp_dataset_path('output')
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
        features.delete(chunk_view_name)
        arcwrap.delete_dataset(chunk_view_name)
        features.insert_from_path(dataset_path, temp_output_path)
        arcwrap.delete_dataset(temp_output_path)
    arcwrap.delete_dataset(temp_overlay_path)
    LOG.log(log_level, "%s features in dataset.", feature_count(dataset_path))
    LOG.log(log_level, "End: Identity.")
    return dataset_path


def keep_features_by_location(dataset_path, location_dataset_path, **kwargs):
    """Keep features where geometry overlaps location feature geometry.

    Args:
        dataset_path (str): Path of dataset.
        location_dataset_path (str): Path of location-dataset.
    Kwargs:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        location_where_sql (str): SQL where-clause for location-dataset
            subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('dataset_where_sql', None),
                          ('location_where_sql', None), ('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    log_level = helpers.LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(
        log_level, "Start: Keep features in %s by locations overlapping %s.",
        dataset_path, location_dataset_path
        )
    LOG.log(log_level, "%s features in dataset.", feature_count(dataset_path))
    dataset_view_name = arcwrap.create_dataset_view(
        helpers.unique_name('view'), dataset_path,
        dataset_where_sql=kwargs['dataset_where_sql']
        )
    location_dataset_view_name = arcwrap.create_dataset_view(
        helpers.unique_name('view'), location_dataset_path,
        dataset_where_sql=kwargs['location_where_sql']
        )
    arcpy.management.SelectLayerByLocation(
        in_layer=dataset_view_name, overlap_type='intersect',
        select_features=location_dataset_view_name,
        selection_type='new_selection'
        )
    # Switch selection for non-overlapping features (to delete).
    arcpy.management.SelectLayerByLocation(in_layer=dataset_view_name,
                                           selection_type='switch_selection')
    arcwrap.delete_dataset(location_dataset_view_name)
    features.delete(dataset_view_name)
    arcwrap.delete_dataset(dataset_view_name)
    LOG.log(log_level, "%s features in dataset.", feature_count(dataset_path))
    LOG.log(log_level, "End: Keep.")
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
    log_level = helpers.LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(
        log_level, ("Start: Overlay features in %s's field %s"
                    " using features in %s's field %s."),
        dataset_path, field_name, overlay_dataset_path, overlay_field_name
        )
    LOG.log(log_level, "%s features in dataset.", feature_count(dataset_path))
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
    temp_overlay_path = arcwrap.copy_dataset(
        overlay_dataset_path, helpers.unique_temp_dataset_path('overlay')
        )
    # Avoid field name collisions with neutral holding field.
    temp_overlay_field_name = fields.duplicate_field(
        temp_overlay_path, overlay_field_name,
        new_field_name=helpers.unique_name(overlay_field_name),
        duplicate_values=True, log_level=None
        )
    # Get an iterable of all object IDs in the dataset.
    # Sorting is important, allows views with ID range instead of list.
    oids = sorted(
        oid for (oid,) in values.features_as_iters(
            dataset_path, ['oid@'],
            dataset_where_sql=kwargs['dataset_where_sql'])
        )
    while oids:
        chunk = oids[:kwargs['chunk_size']]
        oids = oids[kwargs['chunk_size']:]
        LOG.debug("Chunk: Feature OIDs %s to %s", chunk[0], chunk[-1])
        # ArcPy where clauses cannot use 'between'.
        chunk_sql = CHUNK_WHERE_SQL_TEMPLATE.format(
            field=dataset_metadata(dataset_path)['oid_field_name'],
            from_oid=chunk[0], to_oid=chunk[-1]
            )
        if kwargs['dataset_where_sql']:
            chunk_sql += " and ({})".format(kwargs['dataset_where_sql'])
        chunk_view_name = arcwrap.create_dataset_view(
            helpers.unique_name('view'), dataset_path,
            dataset_where_sql=chunk_sql
            )
        # Create the temp output of the overlay.
        if kwargs['tolerance']:
            old_tolerance = arcpy.env.XYTolerance
            arcpy.env.XYTolerance = kwargs['tolerance']
        temp_output_path = helpers.unique_temp_dataset_path('output')
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
        features.delete(chunk_view_name)
        arcwrap.delete_dataset(chunk_view_name)
        features.insert_from_path(dataset_path, temp_output_path)
        arcwrap.delete_dataset(temp_output_path)
    arcwrap.delete_dataset(temp_overlay_path)
    LOG.log(log_level, "%s features in dataset.", feature_count(dataset_path))
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
    log_level = helpers.LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(
        log_level, ("Start: Union-overlay features in %s's field %s"
                    " using features in %s's field %s."),
        dataset_path, field_name, union_dataset_path, union_field_name
        )
    LOG.log(log_level, "%s features in dataset.", feature_count(dataset_path))
    if kwargs['replacement_value'] is not None:
        update_function = (lambda x: kwargs['replacement_value'] if x else None)
    else:
        # Union puts empty string when identity feature not present.
        # Fix to null (replacement value function does this inherently).
        update_function = (lambda x: None if x == '' else x)
    # Create a temporary copy of the union dataset.
    temp_union_path = arcwrap.copy_dataset(
        union_dataset_path, helpers.unique_temp_dataset_path('union')
        )
    # Avoid field name collisions with neutral holding field.
    temp_union_field_name = fields.duplicate_field(
        temp_union_path, union_field_name,
        new_field_name=helpers.unique_name(union_field_name),
        duplicate_values=True, log_level=None
        )
    # Sorting is important, allows views with ID range instead of list.
    oids = sorted(
        oid for (oid,) in values.features_as_iters(
            dataset_path, ['oid@'],
            dataset_where_sql=kwargs['dataset_where_sql'])
        )
    while oids:
        chunk = oids[:kwargs['chunk_size']]
        oids = oids[kwargs['chunk_size']:]
        LOG.debug("Chunk: Feature OIDs %s to %s", chunk[0], chunk[-1])
        # ArcPy where clauses cannot use 'between'.
        chunk_sql = CHUNK_WHERE_SQL_TEMPLATE.format(
            field=dataset_metadata(dataset_path)['oid_field_name'],
            from_oid=chunk[0], to_oid=chunk[-1]
            )
        if kwargs['dataset_where_sql']:
            chunk_sql += " and ({})".format(kwargs['dataset_where_sql'])
        chunk_view_name = arcwrap.create_dataset_view(
            helpers.unique_name('chunk_view'), dataset_path,
            dataset_where_sql=chunk_sql
            )
        # Create the temp output of the union.
        temp_output_path = helpers.unique_temp_dataset_path('output')
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
        features.delete(chunk_view_name)
        arcwrap.delete_dataset(chunk_view_name)
        features.insert_from_path(dataset_path, temp_output_path)
        arcwrap.delete_dataset(temp_output_path)
    arcwrap.delete_dataset(temp_union_path)
    LOG.log(log_level, "%s features in dataset.", feature_count(dataset_path))
    LOG.log(log_level, "End: Union.")
    return dataset_path
