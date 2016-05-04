# -*- coding=utf-8 -*-
"""Geometric set operations."""
import logging

import arcpy

from .. import arcwrap, features, fields, helpers, properties


LOG = logging.getLogger(__name__)


# Features/attributes.

@helpers.log_function
def clip_features(dataset_path, clip_dataset_path, **kwargs):
    """Clip feature geometry where overlapping clip-geometry.

    Args:
        dataset_path (str): Path of dataset.
        clip_dataset_path (str): Path of dataset defining clip area.
    Kwargs:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        clip_where_sql (str): SQL where-clause for clip dataset subselection.
        tolerance (float): Tolerance level (in dataset's units) to clip at.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [
            ('clip_where_sql', None), ('dataset_where_sql', None),
            ('log_level', 'info'), ('tolerance', None)]:
        kwargs.setdefault(*kwarg_default)
    meta = {
        'description': "Clip {} where geometry overlaps {}.".format(
            dataset_path, clip_dataset_path),
        'dataset_view_name': arcwrap.create_dataset_view(
            helpers.unique_name('view'), dataset_path,
            dataset_where_sql=kwargs['dataset_where_sql']),
        'clip_dataset_view_name': arcwrap.create_dataset_view(
            helpers.unique_name('view'), clip_dataset_path,
            dataset_where_sql=kwargs['clip_where_sql']),
        'temp_output_path': helpers.unique_temp_dataset_path('output')}
    helpers.log_line('start', meta['description'], kwargs['log_level'])
    helpers.log_line('feature_count', features.feature_count(dataset_path),
                     kwargs['log_level'])
    try:
        arcpy.analysis.Clip(
            in_features=meta['dataset_view_name'],
            clip_features=meta['clip_dataset_view_name'],
            out_feature_class=meta['temp_output_path'],
            cluster_tolerance=kwargs['tolerance'])
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    arcwrap.delete_dataset(meta['clip_dataset_view_name'])
    # Load back into the dataset.
    arcwrap.delete_features(meta['dataset_view_name'])
    arcwrap.delete_dataset(meta['dataset_view_name'])
    features.insert_features_from_path(
        dataset_path, meta['temp_output_path'], log_level=None)
    arcwrap.delete_dataset(meta['temp_output_path'])
    helpers.log_line('feature_count', features.feature_count(dataset_path),
                     kwargs['log_level'])
    helpers.log_line('end', meta['description'], kwargs['log_level'])
    return dataset_path


@helpers.log_function
def dissolve_features(dataset_path, dissolve_field_names, **kwargs):
    """Merge features that share values in given fields.

    Args:
        dataset_path (str): Path of dataset.
        dissolve_field_names (iter): Iterable of field names to dissolve on.
    Kwargs:
        multipart (bool): Flag indicating if dissolve should create multipart
            features.
        unsplit_lines (bool): Flag indicating if dissolving lines should merge
            features when endpoints meet without a crossing feature.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('dataset_where_sql', None), ('log_level', 'info'),
                          ('multipart', True), ('unsplit_lines', False)]:
        kwargs.setdefault(*kwarg_default)
    meta = {
        'description': "Dissolve features in {} on {}.".format(
            dataset_path, dissolve_field_names),
        'dataset_view_name': arcwrap.create_dataset_view(
            helpers.unique_name('view'), dataset_path,
            dataset_where_sql=kwargs['dataset_where_sql']),
        'temp_output_path': helpers.unique_temp_dataset_path('output')}
    helpers.log_line('start', meta['description'], kwargs['log_level'])
    helpers.log_line('feature_count', features.feature_count(dataset_path),
                     kwargs['log_level'])
    # Set the environment tolerance, so we can be sure the in_memory
    # datasets respect it. 0.003280839895013 is the default for all
    # datasets in our geodatabases.
    arcpy.env.XYTolerance = 0.003280839895013
    try:
        arcpy.management.Dissolve(
            in_features=meta['dataset_view_name'],
            out_feature_class=meta['temp_output_path'],
            dissolve_field=dissolve_field_names,
            multi_part=kwargs['multipart'],
            unsplit_lines=kwargs['unsplit_lines'])
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    # Delete undissolved features that are now dissolved (in the temp).
    arcwrap.delete_features(meta['dataset_view_name'])
    arcwrap.delete_dataset(meta['dataset_view_name'])
    # Copy the dissolved features (in the temp) to the dataset.
    features.insert_features_from_path(
        dataset_path, meta['temp_output_path'], log_level=None)
    arcwrap.delete_dataset(meta['temp_output_path'])
    helpers.log_line('feature_count', features.feature_count(dataset_path),
                     kwargs['log_level'])
    helpers.log_line('end', meta['description'], kwargs['log_level'])
    return dataset_path


@helpers.log_function
def erase_features(dataset_path, erase_dataset_path, **kwargs):
    """Erase feature geometry where overlaps erase dataset geometry.

    Args:
        dataset_path (str): Path of dataset.
        erase_dataset_path (str): Path of erase-dataset.
    Kwargs:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        erase_where_sql (str): SQL where-clause for erase-dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('dataset_where_sql', None),
                          ('erase_where_sql', None), ('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    meta = {
        'description': "Erase {} where geometry overlaps {}.".format(
            dataset_path, erase_dataset_path),
        'dataset_view_name': arcwrap.create_dataset_view(
            helpers.unique_name('view'), dataset_path,
            dataset_where_sql=kwargs['dataset_where_sql']),
        'erase_dataset_view_name': arcwrap.create_dataset_view(
            helpers.unique_name('view'), erase_dataset_path,
            dataset_where_sql=kwargs['erase_where_sql']),
        'temp_output_path': helpers.unique_temp_dataset_path('output')}
    helpers.log_line('start', meta['description'], kwargs['log_level'])
    helpers.log_line('feature_count', features.feature_count(dataset_path),
                     kwargs['log_level'])
    try:
        arcpy.analysis.Erase(
            in_features=meta['dataset_view_name'],
            erase_features=meta['erase_dataset_view_name'],
            out_feature_class=meta['temp_output_path'])
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    arcwrap.delete_dataset(meta['erase_dataset_view_name'])
    # Load back into the dataset.
    arcwrap.delete_features(meta['dataset_view_name'])
    arcwrap.delete_dataset(meta['dataset_view_name'])
    features.insert_features_from_path(
        dataset_path, meta['temp_output_path'], log_level=None)
    arcwrap.delete_dataset(meta['temp_output_path'])
    helpers.log_line('feature_count', features.feature_count(dataset_path),
                     kwargs['log_level'])
    helpers.log_line('end', meta['description'], kwargs['log_level'])
    return dataset_path


@helpers.log_function
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
        replacement_value: Value to replace present identity field value with.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        chunk_size (int): Number of features to process per loop iteration.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('chunk_size', 4096), ('dataset_where_sql', None),
                          ('log_level', 'info'), ('replacement_value', None)]:
        kwargs.setdefault(*kwarg_default)
    meta = {
        'description': "Identity features with {}.{}.".format(
            identity_dataset_path, identity_field_name),
        'dataset': properties.dataset_metadata(dataset_path),
        'chunk_sql_template': "{field} >= {from_oid} and {field} <= {to_oid}",
        'update_function': (
            # Identity puts empty string when identity feature not present.
            # Fix to null (replacement value function does this inherently).
            (lambda x: kwargs['replacement_value'] if x else None)
            if kwargs['replacement_value'] is not None
            else (lambda x: None if x == '' else x)),
        'temp_overlay_path': helpers.unique_temp_dataset_path('overlay'),
        'temp_output_path': helpers.unique_temp_dataset_path('output')}
    helpers.log_line('start', meta['description'], kwargs['log_level'])
    helpers.log_line('feature_count', features.feature_count(dataset_path),
                     kwargs['log_level'])
    # Create a temporary copy of the overlay dataset.
    arcwrap.copy_dataset(identity_dataset_path, meta['temp_overlay_path'])
    # Avoid field name collisions with neutral holding field.
    meta['temp_overlay_field_name'] = fields.duplicate_field(
        meta['temp_overlay_path'], identity_field_name,
        new_field_name=helpers.unique_name(identity_field_name),
        duplicate_values=True, log_level=None)
    # Get an iterable of all object IDs in the dataset.
    # Sorting is important, allows views with ID range instead of list.
    oids = sorted(
        oid for oid, in values.features_as_iters(
            dataset_path, field_names=['oid@'],
            dataset_where_sql=kwargs['dataset_where_sql']))
    while oids:
        # Get subset OIDs & remove them from full set.
        chunk = oids[:kwargs['chunk_size']]
        oids = oids[kwargs['chunk_size']:]
        LOG.debug("Chunk: Feature OIDs %s to %s", chunk[0], chunk[-1])
        # ArcPy where clauses cannot use 'between'.
        meta['chunk_sql'] = meta['chunk_sql_template'].format(
            field=meta['dataset']['oid_field_name'],
            from_oid=chunk[0], to_oid=chunk[-1])
        if kwargs['dataset_where_sql']:
            meta['chunk_sql'] += " and ({})".format(
                kwargs['dataset_where_sql'])
        meta['chunk_view_name'] = arcwrap.create_dataset_view(
            helpers.unique_name('view'), dataset_path,
            dataset_where_sql=meta['chunk_sql'])
        # Create temporary dataset with the identity values.
        try:
            arcpy.analysis.Identity(
                in_features=meta['chunk_view_name'],
                identity_features=meta['temp_overlay_path'],
                out_feature_class=meta['temp_output_path'],
                join_attributes='all', relationship=False)
        except arcpy.ExecuteError:
            LOG.exception("ArcPy execution.")
            raise
        # Push identity (or replacement) value from temp to update field.
        fields.update_field_by_function(
            meta['temp_output_path'], field_name, meta['update_function'],
            field_as_first_arg=False,
            arg_field_names=[meta['temp_overlay_field_name']], log_level=None)
        # Replace original chunk features with identity features.
        arcwrap.delete_features(meta['chunk_view_name'])
        arcwrap.delete_dataset(meta['chunk_view_name'])
        features.insert_features_from_path(
            dataset_path, meta['temp_output_path'], log_level=None)
        arcwrap.delete_dataset(meta['temp_output_path'])
    arcwrap.delete_dataset(meta['temp_overlay_path'])
    helpers.log_line('feature_count', features.feature_count(dataset_path),
                     kwargs['log_level'])
    helpers.log_line('end', meta['description'], kwargs['log_level'])
    return dataset_path


@helpers.log_function
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
    meta = {
        'description': "Keep {} where geometry overlaps {}.".format(
            dataset_path, location_dataset_path),
        'dataset_view_name': arcwrap.create_dataset_view(
            helpers.unique_name('view'), dataset_path,
            dataset_where_sql=kwargs['dataset_where_sql']),
        'location_dataset_view_name': arcwrap.create_dataset_view(
            helpers.unique_name('view'), location_dataset_path,
            dataset_where_sql=kwargs['location_where_sql'])}
    helpers.log_line('start', meta['description'], kwargs['log_level'])
    helpers.log_line('feature_count', features.feature_count(dataset_path),
                     kwargs['log_level'])
    try:
        arcpy.management.SelectLayerByLocation(
            in_layer=meta['dataset_view_name'], overlap_type='intersect',
            select_features=meta['location_dataset_view_name'],
            selection_type='new_selection')
        # Switch selection for non-overlapping features (to delete).
        arcpy.management.SelectLayerByLocation(
            in_layer=meta['dataset_view_name'],
            selection_type='switch_selection')
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    arcwrap.delete_dataset(meta['location_dataset_view_name'])
    arcwrap.delete_features(meta['dataset_view_name'])
    arcwrap.delete_dataset(meta['dataset_view_name'])
    helpers.log_line('feature_count', features.feature_count(dataset_path),
                     kwargs['log_level'])
    helpers.log_line('end', meta['description'], kwargs['log_level'])
    return dataset_path


@helpers.log_function
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
            ('overlay_most_coincident', False), ('replacement_value', None)]:
        kwargs.setdefault(*kwarg_default)
    meta = {
        'description': "Overlay features with {}.{}.".format(
            overlay_dataset_path, overlay_field_name),
        'chunk_sql_template': "{field} >= {from_oid} and {field} <= {to_oid}",
        'update_function': (
            (lambda x: kwargs['replacement_value'] if x else None)
            if kwargs['replacement_value'] is not None else (lambda x: x)),
        'temp_overlay_path': helpers.unique_temp_dataset_path('overlay'),
        'temp_output_path': helpers.unique_temp_dataset_path('output')}
    helpers.log_line('start', meta['description'], kwargs['log_level'])
    helpers.log_line('feature_count', features.feature_count(dataset_path),
                     kwargs['log_level'])
    # Check flags & set details for spatial join call.
    if kwargs['overlay_most_coincident']:
        raise NotImplementedError(
            "overlay_most_coincident not yet implemented.")
    elif kwargs['overlay_central_coincident']:
        join_kwargs = {'join_operation': 'join_one_to_many',
                       'join_type': 'keep_all',
                       'match_option': 'have_their_center_in'}
    else:
        join_kwargs = {'join_operation': 'join_one_to_many',
                       'join_type': 'keep_all',
                       'match_option': 'intersect'}
    # Create temporary copy of overlay dataset.
    arcwrap.copy_dataset(overlay_dataset_path, meta['temp_overlay_path'])
    # Avoid field name collisions with neutral holding field.
    meta['temp_overlay_field_name'] = fields.duplicate_field(
        meta['temp_overlay_path'], overlay_field_name,
        new_field_name=helpers.unique_name(overlay_field_name),
        duplicate_values=True, log_level=None)
    # Get an iterable of all object IDs in the dataset.
    # Sorting is important, allows views with ID range instead of list.
    oids = sorted(
        oid for (oid,)
        in values.features_as_iters(
            dataset_path, ['oid@'],
            dataset_where_sql=kwargs['dataset_where_sql']))
    while oids:
        chunk = oids[:kwargs['chunk_size']]
        oids = oids[kwargs['chunk_size']:]
        LOG.debug("Chunk: Feature OIDs %s to %s", chunk[0], chunk[-1])
        # ArcPy where clauses cannot use 'between'.
        meta['chunk_sql'] = meta['chunk_sql_template'].format(
            field=properties.dataset_metadata(dataset_path)['oid_field_name'],
            from_oid=chunk[0], to_oid=chunk[-1])
        if kwargs['dataset_where_sql']:
            meta['chunk_sql'] += " and ({})".format(
                kwargs['dataset_where_sql'])
        meta['chunk_view_name'] = arcwrap.create_dataset_view(
            helpers.unique_name('view'), dataset_path,
            dataset_where_sql=meta['chunk_sql'])
        # Create the temp output of the overlay.
        try:
            arcpy.analysis.SpatialJoin(
                target_features=meta['chunk_view_name'],
                join_features=meta['temp_overlay_path'],
                out_feature_class=meta['temp_output_path'],
                **join_kwargs)
        except arcpy.ExecuteError:
            LOG.exception("ArcPy execution.")
            raise
        # Push overlay (or replacement) value from temp to update field.
        fields.update_field_by_function(
            meta['temp_output_path'], field_name, meta['update_function'],
            field_as_first_arg=False,
            arg_field_names=[meta['temp_overlay_field_name']], log_level=None)
        # Replace original chunk features with overlay features.
        arcwrap.delete_features(meta['chunk_view_name'])
        arcwrap.delete_dataset(meta['chunk_view_name'])
        features.insert_features_from_path(
            dataset_path, meta['temp_output_path'], log_level=None)
        arcwrap.delete_dataset(meta['temp_output_path'])
    arcwrap.delete_dataset(meta['temp_overlay_path'])
    helpers.log_line('feature_count', features.feature_count(dataset_path),
                     kwargs['log_level'])
    helpers.log_line('end', meta['description'], kwargs['log_level'])
    return dataset_path


@helpers.log_function
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
        replacement_value: Value to replace present union-field value with.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        chunk_size (int): Number of features to process per loop iteration.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('chunk_size', 4096), ('dataset_where_sql', None),
                          ('log_level', 'info'), ('replacement_value', None)]:
        kwargs.setdefault(kwarg_default)
    meta = {
        'description': "Union features with {}.{}.".format(
            union_dataset_path, union_field_name),
        'dataset': properties.dataset_metadata(dataset_path),
        'chunk_sql_template': "{field} >= {from_oid} and {field} <= {to_oid}",
        'update_function': (
            # Union puts empty string when union feature not present.
            # Fix to null (replacement value function does this inherently).
            (lambda x: kwargs['replacement_value'] if x else None)
            if kwargs['replacement_value'] is not None
            else (lambda x: None if x == '' else x)),
        'temp_overlay_path': helpers.unique_temp_dataset_path('overlay'),
        'temp_output_path': helpers.unique_temp_dataset_path('output')}
    helpers.log_line('start', meta['description'], kwargs['log_level'])
    helpers.log_line('feature_count', features.feature_count(dataset_path),
                     kwargs['log_level'])
    # Create a temporary copy of the overlay dataset.
    arcwrap.copy_dataset(union_dataset_path, meta['temp_overlay_path'])
    # Avoid field name collisions with neutral holding field.
    meta['temp_overlay_field_name'] = fields.duplicate_field(
        meta['temp_overlay_path'], union_field_name,
        new_field_name=helpers.unique_name(union_field_name),
        duplicate_values=True, log_level=None)
    # Sorting is important, allows views with ID range instead of list.
    oids = sorted(
        oid for (oid,)
        in values.features_as_iters(
            dataset_path, ['oid@'],
            dataset_where_sql=kwargs['dataset_where_sql']))
    while oids:
        chunk = oids[:kwargs['chunk_size']]
        oids = oids[kwargs['chunk_size']:]
        LOG.debug("Chunk: Feature OIDs %s to %s", chunk[0], chunk[-1])
        # ArcPy where clauses cannot use 'between'.
        meta['chunk_where_sql'] = meta['chunk_sql_template'].format(
            field=meta['dataset']['oid_field_name'],
            from_oid=chunk[0], to_oid=chunk[-1])
        if kwargs['dataset_where_sql']:
            meta['chunk_where_sql'] += " and ({})".format(
                kwargs['dataset_where_sql'])
        meta['chunk_view_name'] = arcwrap.create_dataset_view(
            helpers.unique_name('chunk_view'), dataset_path,
            dataset_where_sql=meta['chunk_where_sql'])
        # Create the temp output of the union.
        try:
            arcpy.analysis.Union(
                in_features=[
                    meta['chunk_view_name'], meta['temp_overlay_path']],
                out_feature_class=meta['temp_output_path'],
                join_attributes='all', gaps=False)
        except arcpy.ExecuteError:
            LOG.exception("ArcPy execution.")
            raise
        # Push union (or replacement) value from temp to update field.
        fields.update_field_by_function(
            meta['temp_output_path'], field_name, meta['update_function'],
            field_as_first_arg=False,
            arg_field_names=[meta['temp_overlay_field_name']], log_level=None)
        # Replace original chunk features with union features.
        arcwrap.delete_features(meta['chunk_view_name'])
        arcwrap.delete_dataset(meta['chunk_view_name'])
        features.insert_features_from_path(
            dataset_path, meta['temp_output_path'], log_level=None)
        arcwrap.delete_dataset(meta['temp_output_path'])
    arcwrap.delete_dataset(meta['temp_overlay_path'])
    helpers.log_line('feature_count', features.feature_count(dataset_path),
                     kwargs['log_level'])
    helpers.log_line('end', meta['description'], kwargs['log_level'])
    return dataset_path

