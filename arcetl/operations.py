# -*- coding=utf-8 -*-
"""Processing operation objects."""
import collections
import csv
import logging
import os
import tempfile

import arcpy

from . import arcobj, helpers, properties


LOG = logging.getLogger(__name__)


##TODO: TEMPORARY LOCAL COPIES TO AVOID CYCLICAL IMPORT##

@helpers.log_function
def add_field(dataset_path, field_name, field_type, **kwargs):
    """."""
    for kwarg_default in [
            ('field_is_nullable', True), ('field_is_required', False),
            ('field_length', 64), ('field_precision', None),
            ('field_scale', None), ('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    meta = {'description': "Add field {}.{}.".format(dataset_path, field_name)}
    helpers.log_line('start', meta['description'], kwargs['log_level'])
    try:
        arcpy.management.AddField(
            in_table=dataset_path, field_name=field_name,
            field_type=arcobj.FIELD_TYPE_AS_ARC.get(
                field_type.lower(), field_type),
            field_length=kwargs['field_length'],
            field_precision=kwargs['field_precision'],
            field_scale=kwargs['field_scale'],
            field_is_nullable=kwargs['field_is_nullable'],
            field_is_required=kwargs['field_is_required'])
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    helpers.log_line('end', meta['description'], kwargs['log_level'])
    return field_name

@helpers.log_function
def add_fields_from_metadata_list(dataset_path, metadata_list, **kwargs):
    """."""
    kwargs.setdefault('log_level', 'info')
    meta = {
        'description': "Add fields to {} from a metadata list.".format(
            dataset_path)}
    helpers.log_line('start', meta['description'], kwargs['log_level'])
    meta['field_keywords'] = ['name', 'type', 'length', 'precision', 'scale',
                              'is_nullable', 'is_required']
    for metadata in metadata_list:
        try:
            field_name = add_field(
                dataset_path, log_level=None,
                **{'field_{}'.format(kw): metadata[kw]
                   for kw in meta['field_keywords'] if kw in metadata})
        except arcpy.ExecuteError:
            LOG.exception("ArcPy execution.")
            raise
        helpers.log_line(
            'misc', "Added {}.".format(field_name), kwargs['log_level'])
    helpers.log_line('end', meta['description'], kwargs['log_level'])
    return [metadata['name'] for metadata in metadata_list]

@helpers.log_function
def delete_field(dataset_path, field_name, **kwargs):
    """."""
    kwargs.setdefault('log_level', 'info')
    meta = {'description': "Delete field {}.".format(field_name)}
    helpers.log_line('start', meta['description'], kwargs['log_level'])
    try:
        arcpy.management.DeleteField(
            in_table=dataset_path, drop_field=field_name)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    helpers.log_line('end', meta['description'], kwargs['log_level'])
    return field_name

@helpers.log_function
def duplicate_field(dataset_path, field_name, new_field_name, **kwargs):
    """."""
    for kwarg_default in [('dataset_where_sql', None),
                          ('duplicate_values', False), ('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    meta = {
        'description': "Duplicate {}.{} as {}.".format(
            dataset_path, field_name, new_field_name)}
    helpers.log_line('start', meta['description'], kwargs['log_level'])
    meta['field'] = properties.field_metadata(dataset_path, field_name)
    meta['field']['name'] = new_field_name
    # Cannot add OID-type field, so push to a long-type.
    if meta['field']['type'].lower() == 'oid':
        meta['field']['type'] = 'long'
    add_fields_from_metadata_list(
        dataset_path, [meta['field']], log_level=None)
    if kwargs['duplicate_values']:
        update_field_by_function(
            dataset_path, meta['field']['name'], function=(lambda x: x),
            field_as_first_arg=False, arg_field_names=[field_name],
            dataset_where_sql=kwargs['dataset_where_sql'], log_level=None)
    helpers.log_line('end', meta['description'], kwargs['log_level'])
    return new_field_name

def feature_count(dataset_path, **kwargs):
    """."""
    kwargs.setdefault('dataset_where_sql', None)
    #pylint: disable=no-member
    with arcpy.da.SearchCursor(
        #pylint: enable=no-member
        in_table=dataset_path, field_names=['oid@'],
        where_clause=kwargs['dataset_where_sql']) as cursor:
        return len([None for _ in cursor])

@helpers.log_function
def insert_features_from_path(dataset_path, insert_dataset_path,
                              field_names=None, **kwargs):
    """."""
    kwargs.setdefault('insert_where_sql', None)
    kwargs.setdefault('log_level', 'info')
    _description = "Insert features into {} from {}.".format(
        dataset_path, insert_dataset_path)
    helpers.log_line('start', _description, kwargs['log_level'])
    helpers.log_line(
        'feature_count', feature_count(dataset_path), kwargs['log_level'])
    meta = {'dataset': properties.dataset_metadata(dataset_path),
            'insert_dataset': properties.dataset_metadata(insert_dataset_path)}
    # Create field maps.
    # Added because ArcGIS Pro's no-test append is case-sensitive (verified
    # 1.0-1.1.1). BUG-000090970 - ArcGIS Pro 'No test' field mapping in
    # Append tool does not auto-map to the same field name if naming
    # convention differs.
    if field_names:
        _field_names = [name.lower() for name in field_names]
    else:
        _field_names = [field['name'].lower()
                        for field in meta['dataset']['fields']]
    insert_field_names = [
        field['name'].lower() for field in meta['insert_dataset']['fields']]
    # Append takes care of geometry & OIDs independent of the field maps.
    for field_name_type in ('geometry_field_name', 'oid_field_name'):
        if meta['dataset'].get(field_name_type):
            _field_names.remove(meta['dataset'][field_name_type].lower())
            insert_field_names.remove(
                meta['insert_dataset'][field_name_type].lower())
    field_maps = arcpy.FieldMappings()
    for field_name in _field_names:
        if field_name in insert_field_names:
            field_map = arcpy.FieldMap()
            field_map.addInputField(insert_dataset_path, field_name)
            field_maps.addFieldMap(field_map)
    insert_dataset_view_name = create_dataset_view(
        helpers.unique_name('insert_dataset_view'), insert_dataset_path,
        dataset_where_sql=kwargs['insert_where_sql'],
        # Insert view must be nonspatial to append to nonspatial table.
        force_nonspatial=(not meta['dataset']['is_spatial']), log_level=None)
    try:
        arcpy.management.Append(
            inputs=insert_dataset_view_name, target=dataset_path,
            schema_type='no_test', field_mapping=field_maps)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    delete_dataset(insert_dataset_view_name, log_level=None)
    helpers.log_line(
        'feature_count', feature_count(dataset_path), kwargs['log_level'])
    helpers.log_line('end', _description, kwargs['log_level'])
    return dataset_path

@helpers.log_function
def update_field_by_function(dataset_path, field_name, function, **kwargs):
    """."""
    for kwarg_default in [
            ('arg_field_names', []), ('dataset_where_sql', None),
            ('field_as_first_arg', True), ('kwarg_field_names', []),
            ('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    meta = {
        'description': "Update field {} using function {}.".format(
            field_name, function.__name__)}
    helpers.log_line('start', meta['description'], kwargs['log_level'])
    #pylint: disable=no-member
    with arcpy.da.UpdateCursor(
        #pylint: enable=no-member
        in_table=dataset_path,
        field_names=([field_name] + list(kwargs['arg_field_names'])
                     + list(kwargs['kwarg_field_names'])),
        where_clause=kwargs['dataset_where_sql']) as cursor:
        for row in cursor:
            args = row[1:(len(kwargs['arg_field_names']) + 1)]
            if kwargs['field_as_first_arg']:
                args.insert(0, row[0])
            _kwargs = dict(zip(kwargs['kwarg_field_names'],
                               row[(len(kwargs['arg_field_names']) + 1):]))
            new_value = function(*args, **_kwargs)
            if row[0] != new_value:
                cursor.updateRow([new_value] + list(row[1:]))
    helpers.log_line('end', meta['description'], kwargs['log_level'])
    return field_name

@helpers.log_function
def update_field_by_joined_value(dataset_path, field_name, join_dataset_path,
                                 join_field_name, on_field_pairs,
                                 **kwargs):
    """."""
    for kwarg_default in [('dataset_where_sql', None), ('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    meta = {
        'description':
            "Update field {} with joined values from {}.{}>.".format(
                field_name, join_dataset_path, join_field_name)}
    helpers.log_line('start', meta['description'], kwargs['log_level'])
    # Build join-reference.
    join_value_map = {
        tuple(row[1:]): row[0]
        for row in properties.field_values(
            join_dataset_path,
            field_names=[join_field_name] + [p[1] for p in on_field_pairs])}
    #pylint: disable=no-member
    with arcpy.da.UpdateCursor(
        #pylint: enable=no-member
        in_table=dataset_path,
        field_names=[field_name] + [p[0] for p in on_field_pairs],
        where_clause=kwargs.get('dataset_where_sql')) as cursor:
        for row in cursor:
            new_value = join_value_map.get(tuple(row[1:]))
            if row[0] != new_value:
                cursor.updateRow([new_value] + list(row[1:]))
    helpers.log_line('end', meta['description'], kwargs['log_level'])
    return field_name

##TODO: TEMPORARY LOCAL COPIES TO AVOID CYCLICAL IMPORT##


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
    for kwarg_default in [('clip_where_sql', None), ('dataset_where_sql', None),
                          ('log_level', 'info'), ('tolerance', None)]:
        kwargs.setdefault(*kwarg_default)
    _description = "Clip {} where geometry overlaps {}.".format(
        dataset_path, clip_dataset_path)
    helpers.log_line('start', _description, kwargs['log_level'])
    helpers.log_line(
        'feature_count', feature_count(dataset_path), kwargs['log_level'])
    dataset_view_name = create_dataset_view(
        helpers.unique_name('dataset_view'), dataset_path,
        dataset_where_sql=kwargs['dataset_where_sql'], log_level=None)
    clip_dataset_view_name = create_dataset_view(
        helpers.unique_name('clip_dataset_view'), clip_dataset_path,
        dataset_where_sql=kwargs['clip_where_sql'], log_level=None)
    temp_output_path = helpers.unique_temp_dataset_path('temp_output')
    try:
        arcpy.analysis.Clip(
            in_features=dataset_view_name,
            clip_features=clip_dataset_view_name,
            out_feature_class=temp_output_path,
            cluster_tolerance=kwargs['tolerance'])
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    delete_dataset(clip_dataset_view_name, log_level=None)
    # Load back into the dataset.
    delete_features(dataset_view_name, log_level=None)
    delete_dataset(dataset_view_name, log_level=None)
    insert_features_from_path(dataset_path, temp_output_path, log_level=None)
    delete_dataset(temp_output_path, log_level=None)
    helpers.log_line(
        'feature_count', feature_count(dataset_path), kwargs['log_level'])
    helpers.log_line('end', _description, kwargs['log_level'])
    return dataset_path


@helpers.log_function
def delete_features(dataset_path, **kwargs):
    """Delete select features.

    Args:
        dataset_path (str): Path of dataset.
    Kwargs:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    kwargs.setdefault('dataset_where_sql', None)
    kwargs.setdefault('log_level', 'info')
    _description = "Delete features from {}.".format(dataset_path)
    helpers.log_line('start', _description, kwargs['log_level'])
    helpers.log_line(
        'feature_count', feature_count(dataset_path), kwargs['log_level'])
    dataset_view_name = create_dataset_view(
        helpers.unique_name('dataset_view'), dataset_path,
        dataset_where_sql=kwargs['dataset_where_sql'], log_level=None)
    # Can use (faster) truncate when no sub-selection
    run_truncate = kwargs.get('dataset_where_sql') is None
    if run_truncate:
        try:
            arcpy.management.TruncateTable(in_table=dataset_view_name)
        except arcpy.ExecuteError:
            truncate_type_error_codes = (
                # "Only supports Geodatabase tables and feature classes."
                'ERROR 000187',
                # "Operation not supported on table {table name}."
                'ERROR 001260',
                # Operation not supported on a feature class in a controller
                # dataset.
                'ERROR 001395',
                )
            # Avoid arcpy.GetReturnCode(); error code position inconsistent.
            # Search messages for 'ERROR ######' instead.
            if any(code in arcpy.GetMessages()
                   for code in truncate_type_error_codes):
                LOG.debug("Truncate unsupported; will try deleting rows.")
                run_truncate = False
            else:
                LOG.exception("ArcPy execution.")
                raise
    if not run_truncate:
        try:
            arcpy.management.DeleteRows(in_rows=dataset_view_name)
        except:
            LOG.exception("ArcPy execution.")
            raise
    delete_dataset(dataset_view_name, log_level=None)
    helpers.log_line(
        'feature_count', feature_count(dataset_path), kwargs['log_level'])
    helpers.log_line('end', _description, kwargs['log_level'])
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
    _description = "Dissolve features in {} on {}.".format(
        dataset_path, dissolve_field_names)
    helpers.log_line('start', _description, kwargs['log_level'])
    helpers.log_line(
        'feature_count', feature_count(dataset_path), kwargs['log_level'])
    # Set the environment tolerance, so we can be sure the in_memory
    # datasets respect it. 0.003280839895013 is the default for all
    # datasets in our geodatabases.
    arcpy.env.XYTolerance = 0.003280839895013
    dataset_view_name = create_dataset_view(
        helpers.unique_name('dataset_view'), dataset_path,
        dataset_where_sql=kwargs['dataset_where_sql'], log_level=None)
    temp_output_path = helpers.unique_temp_dataset_path('temp_output')
    try:
        arcpy.management.Dissolve(
            in_features=dataset_view_name, out_feature_class=temp_output_path,
            dissolve_field=dissolve_field_names,
            multi_part=kwargs['multipart'],
            unsplit_lines=kwargs['unsplit_lines'])
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    # Delete undissolved features that are now dissolved (in the temp).
    delete_features(dataset_view_name, log_level=None)
    delete_dataset(dataset_view_name, log_level=None)
    # Copy the dissolved features (in the temp) to the dataset.
    insert_features_from_path(dataset_path, temp_output_path, log_level=None)
    delete_dataset(temp_output_path, log_level=None)
    helpers.log_line(
        'feature_count', feature_count(dataset_path), kwargs['log_level'])
    helpers.log_line('end', _description, kwargs['log_level'])
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
    _description = "Erase {} where geometry overlaps {}.".format(
        dataset_path, erase_dataset_path)
    helpers.log_line('start', _description, kwargs['log_level'])
    helpers.log_line(
        'feature_count', feature_count(dataset_path), kwargs['log_level'])
    dataset_view_name = create_dataset_view(
        helpers.unique_name('dataset_view'), dataset_path,
        dataset_where_sql=kwargs['dataset_where_sql'], log_level=None)
    erase_dataset_view_name = create_dataset_view(
        helpers.unique_name('erase_dataset_view'), erase_dataset_path,
        dataset_where_sql=kwargs['erase_where_sql'], log_level=None)
    temp_output_path = helpers.unique_temp_dataset_path('temp_output')
    try:
        arcpy.analysis.Erase(
            in_features=dataset_view_name,
            erase_features=erase_dataset_view_name,
            out_feature_class=temp_output_path)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    delete_dataset(erase_dataset_view_name, log_level=None)
    # Load back into the dataset.
    delete_features(dataset_view_name, log_level=None)
    delete_dataset(dataset_view_name, log_level=None)
    insert_features_from_path(dataset_path, temp_output_path, log_level=None)
    delete_dataset(temp_output_path, log_level=None)
    helpers.log_line(
        'feature_count', feature_count(dataset_path), kwargs['log_level'])
    helpers.log_line('end', _description, kwargs['log_level'])
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
    _description = "Keep {} where geometry overlaps {}.".format(
        dataset_path, location_dataset_path)
    helpers.log_line('start', _description, kwargs['log_level'])
    helpers.log_line(
        'feature_count', feature_count(dataset_path), kwargs['log_level'])
    dataset_view_name = create_dataset_view(
        helpers.unique_name('dataset_view'), dataset_path,
        dataset_where_sql=kwargs['dataset_where_sql'], log_level=None)
    location_dataset_view_name = create_dataset_view(
        helpers.unique_name('location_dataset_view'), location_dataset_path,
        dataset_where_sql=kwargs['location_where_sql'], log_level=None)
    try:
        arcpy.management.SelectLayerByLocation(
            in_layer=dataset_view_name, overlap_type='intersect',
            select_features=location_dataset_view_name,
            selection_type='new_selection')
        # Switch selection for non-overlapping features (to delete).
        arcpy.management.SelectLayerByLocation(
            in_layer=dataset_view_name, selection_type='switch_selection')
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    delete_dataset(location_dataset_view_name, log_level=None)
    delete_features(dataset_view_name, log_level=None)
    delete_dataset(dataset_view_name, log_level=None)
    helpers.log_line(
        'feature_count', feature_count(dataset_path), kwargs['log_level'])
    helpers.log_line('end', _description, kwargs['log_level'])
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
    _description = "Identity features with {}.{}.".format(
        identity_dataset_path, identity_field_name)
    helpers.log_line('start', _description, kwargs['log_level'])
    helpers.log_line(
        'feature_count', feature_count(dataset_path), kwargs['log_level'])
    # Create a temporary copy of the overlay dataset.
    temp_overlay_path = copy_dataset(
        identity_dataset_path,
        helpers.unique_temp_dataset_path('temp_overlay'), log_level=None)
    # Avoid field name collisions with neutral holding field.
    temp_overlay_field_name = duplicate_field(
        temp_overlay_path, identity_field_name,
        new_field_name=helpers.unique_name(identity_field_name),
        duplicate_values=True, log_level=None)
    # Get an iterable of all object IDs in the dataset.
    # Sorting is important, allows views with ID range instead of list.
    oids = sorted(
        oid for (oid,)
        in properties.field_values(
            dataset_path, ['oid@'],
            dataset_where_sql=kwargs['dataset_where_sql']))
    while oids:
        # Get subset OIDs & remove them from full set.
        chunk = oids[:kwargs['chunk_size']]
        oids = oids[kwargs['chunk_size']:]
        LOG.debug("Chunk: Feature OIDs %s to %s", chunk[0], chunk[-1])
        # ArcPy where clauses cannot use 'between'.
        chunk_where_sql = (
            "{field} >= {from_oid} and {field} <= {to_oid}".format(
                field=properties.dataset_metadata(
                    dataset_path)['oid_field_name'],
                from_oid=chunk[0], to_oid=chunk[-1]))
        if kwargs['dataset_where_sql']:
            chunk_where_sql += " and ({})".format(
                kwargs['dataset_where_sql'])
        chunk_view_name = create_dataset_view(
            helpers.unique_name('chunk_view'), dataset_path,
            dataset_where_sql=chunk_where_sql, log_level=None)
        # Create temporary dataset with the identity values.
        temp_output_path = helpers.unique_temp_dataset_path('temp_output')
        try:
            arcpy.analysis.Identity(
                in_features=chunk_view_name,
                identity_features=temp_overlay_path,
                out_feature_class=temp_output_path, join_attributes='all',
                relationship=False)
        except arcpy.ExecuteError:
            LOG.exception("ArcPy execution.")
            raise
        # Push identity (or replacement) value from temp to update field.
        # Apply replacement value if necessary.
        if kwargs['replacement_value'] is not None:
            update_field_by_function(
                temp_output_path, field_name,
                function=(
                    lambda x: kwargs['replacement_value'] if x else None),
                field_as_first_arg=False,
                arg_field_names=[temp_overlay_field_name], log_level=None)
        # Identity puts empty string when identity feature not present.
        # Fix to null (replacement value function does this inherently).
        else:
            update_field_by_function(
                temp_output_path, field_name,
                function=(lambda x: None if x == '' else x),
                field_as_first_arg=False,
                arg_field_names=[temp_overlay_field_name], log_level=None)
        # Replace original chunk features with identity features.
        delete_features(chunk_view_name, log_level=None)
        delete_dataset(chunk_view_name, log_level=None)
        insert_features_from_path(
            dataset_path, temp_output_path, log_level=None)
        delete_dataset(temp_output_path, log_level=None)
    delete_dataset(temp_overlay_path, log_level=None)
    helpers.log_line(
        'feature_count', feature_count(dataset_path), kwargs['log_level'])
    helpers.log_line('end', _description, kwargs['log_level'])
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
    _description = "Overlay features with {}.{}.".format(
        overlay_dataset_path, overlay_field_name)
    helpers.log_line('start', _description, kwargs['log_level'])
    helpers.log_line(
        'feature_count', feature_count(dataset_path), kwargs['log_level'])
    # Check flags & set details for spatial join call.
    if kwargs['overlay_most_coincident']:
        raise NotImplementedError(
            "overlay_most_coincident not yet implemented.")
    elif kwargs['overlay_central_coincident']:
        _join_kwargs = {'join_operation': 'join_one_to_many',
                        'join_type': 'keep_all',
                        'match_option': 'have_their_center_in'}
    else:
        _join_kwargs = {'join_operation': 'join_one_to_many',
                        'join_type': 'keep_all',
                        'match_option': 'intersect'}
    # Create temporary copy of overlay dataset.
    temp_overlay_path = copy_dataset(
        overlay_dataset_path, helpers.unique_temp_dataset_path('temp_overlay'),
        log_level=None)
    # Avoid field name collisions with neutral holding field.
    temp_overlay_field_name = dataset.duplicate_field(
        temp_overlay_path, overlay_field_name,
        new_field_name=helpers.unique_name(overlay_field_name),
        duplicate_values=True, log_level=None)
    # Get an iterable of all object IDs in the dataset.
    # Sorting is important, allows views with ID range instead of list.
    oids = sorted(
        oid for (oid,)
        in properties.field_values(
            dataset_path, ['oid@'],
            dataset_where_sql=kwargs['dataset_where_sql']))
    while oids:
        chunk = oids[:kwargs['chunk_size']]
        oids = oids[kwargs['chunk_size']:]
        LOG.debug("Chunk: Feature OIDs %s to %s", chunk[0], chunk[-1])
        # ArcPy where clauses cannot use 'between'.
        chunk_where_sql = (
            "{field} >= {from_oid} and {field} <= {to_oid}".format(
                field=properties.dataset_metadata(
                    dataset_path)['oid_field_name'],
                from_oid=chunk[0], to_oid=chunk[-1]))
        if kwargs['dataset_where_sql']:
            chunk_where_sql += " and ({})".format(
                kwargs['dataset_where_sql'])
        chunk_view_name = create_dataset_view(
            helpers.unique_name('chunk_view'), dataset_path,
            dataset_where_sql=chunk_where_sql, log_level=None)
        # Create the temp output of the overlay.
        temp_output_path = helpers.unique_temp_dataset_path('temp_output')
        try:
            arcpy.analysis.SpatialJoin(
                target_features=chunk_view_name,
                join_features=temp_overlay_path,
                out_feature_class=temp_output_path,
                **_join_kwargs)
        except arcpy.ExecuteError:
            LOG.exception("ArcPy execution.")
            raise
        # Push overlay (or replacement) value from temp to update field.
        # Apply replacement value if necessary.
        if kwargs['replacement_value'] is not None:
            update_field_by_function(
                temp_output_path, field_name,
                function=(
                    lambda x: kwargs['replacement_value'] if x else None),
                field_as_first_arg=False,
                arg_field_names=[temp_overlay_field_name], log_level=None)
        else:
            update_field_by_function(
                temp_output_path, field_name, function=(lambda x: x),
                field_as_first_arg=False,
                arg_field_names=[temp_overlay_field_name], log_level=None)
        # Replace original chunk features with overlay features.
        delete_features(chunk_view_name, log_level=None)
        delete_dataset(chunk_view_name, log_level=None)
        insert_features_from_path(
            dataset_path, temp_output_path, log_level=None)
        delete_dataset(temp_output_path, log_level=None)
    delete_dataset(temp_overlay_path, log_level=None)
    helpers.log_line(
        'feature_count', feature_count(dataset_path), kwargs['log_level'])
    helpers.log_line('end', _description, kwargs['log_level'])
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
    _description = "Union features with {}.{}.".format(
        union_dataset_path, union_field_name)
    helpers.log_line('start', _description, kwargs['log_level'])
    helpers.log_line(
        'feature_count', feature_count(dataset_path), kwargs['log_level'])
    # Create a temporary copy of the overlay dataset.
    temp_overlay_path = copy_dataset(
        union_dataset_path, helpers.unique_temp_dataset_path('temp_overlay'),
        log_level=None)
    # Avoid field name collisions with neutral holding field.
    temp_overlay_field_name = duplicate_field(
        temp_overlay_path, union_field_name,
        new_field_name=helpers.unique_name(union_field_name),
        duplicate_values=True, log_level=None)
    # Sorting is important, allows views with ID range instead of list.
    oids = sorted(
        oid for (oid,)
        in properties.field_values(
            dataset_path, ['oid@'],
            dataset_where_sql=kwargs['dataset_where_sql']))
    while oids:
        chunk = oids[:kwargs['chunk_size']]
        oids = oids[kwargs['chunk_size']:]
        LOG.debug("Chunk: Feature OIDs %s to %s", chunk[0], chunk[-1])
        # ArcPy where clauses cannot use 'between'.
        chunk_where_sql = (
            "{field} >= {from_oid} and {field} <= {to_oid}".format(
                field=properties.dataset_metadata(
                    dataset_path)['oid_field_name'],
                from_oid=chunk[0], to_oid=chunk[-1]))
        if kwargs['dataset_where_sql']:
            chunk_where_sql += " and ({})".format(
                kwargs['dataset_where_sql'])
        chunk_view_name = create_dataset_view(
            helpers.unique_name('chunk_view'), dataset_path,
            dataset_where_sql=chunk_where_sql, log_level=None)
        # Create the temp output of the union.
        temp_output_path = helpers.unique_temp_dataset_path('temp_output')
        try:
            arcpy.analysis.Union(
                in_features=[chunk_view_name, temp_overlay_path],
                out_feature_class=temp_output_path, join_attributes='all',
                gaps=False)
        except arcpy.ExecuteError:
            LOG.exception("ArcPy execution.")
            raise
        # Push union (or replacement) value from temp to update field.
        # Apply replacement value if necessary.
        if kwargs['replacement_value'] is not None:
            update_field_by_function(
                temp_output_path, field_name,
                function=(
                    lambda x: kwargs['replacement_value'] if x else None),
                field_as_first_arg=False,
                arg_field_names=[temp_overlay_field_name], log_level=None)
        # Union puts empty string when union feature not present.
        # Fix to null (replacement value function does this inherently).
        else:
            update_field_by_function(
                temp_output_path, field_name,
                function=(lambda x: None if x == '' else x),
                field_as_first_arg=False,
                arg_field_names=[temp_overlay_field_name], log_level=None)
        # Replace original chunk features with union features.
        delete_features(chunk_view_name, log_level=None)
        delete_dataset(chunk_view_name, log_level=None)
        insert_features_from_path(
            dataset_path, temp_output_path, log_level=None)
        delete_dataset(temp_output_path, log_level=None)
    delete_dataset(temp_overlay_path, log_level=None)
    helpers.log_line(
        'feature_count', feature_count(dataset_path), kwargs['log_level'])
    helpers.log_line('end', _description, kwargs['log_level'])
    return dataset_path


# Products.

@helpers.log_function
def convert_polygons_to_lines(dataset_path, output_path, **kwargs):
    """Convert geometry from polygons to lines.

    If topological is set to True, shared outlines will be a single,
    separate feature. Note that one cannot pass attributes to a
    topological transformation (as the values would not apply to all
    adjacent features).

    If an id field name is specified, the output dataset will identify the
    input features that defined the line feature with the name & values
    from the provided field. This option will be ignored if the output is
    non-topological lines, as the field will pass over with the rest of
    the attributes.

    Args:
        dataset_path (str): Path of dataset.
        output_path (str): Path of output dataset.
    Kwargs:
        topological (bool): Flag indicating if lines should be topological, or
            merge overlapping lines.
        id_field_name (str): Name of field to apply ID to lines from.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('topological', False), ('id_field_name', None),
                          ('dataset_where_sql', None), ('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    _description = "Convert polygon features in {} to lines.".format(
        dataset_path)
    helpers.log_line('start', _description, kwargs['log_level'])
    dataset_view_name = create_dataset_view(
        helpers.unique_name('dataset_view'), dataset_path,
        dataset_where_sql=kwargs['dataset_where_sql'], log_level=None)
    try:
        arcpy.management.PolygonToLine(
            in_features=dataset_view_name, out_feature_class=output_path,
            neighbor_option=kwargs['topological'])
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    delete_dataset(dataset_view_name)
    if kwargs['topological']:
        id_field_metadata = properties.field_metadata(
            dataset_path, kwargs['id_field_name'])
        oid_field_name = properties.dataset_metadata(
            dataset_path)['oid_field_name']
        for side in ('left', 'right'):
            side_oid_field_name = '{}_FID'.format(side.upper())
            if kwargs['id_field_name']:
                side_id_field_metadata = id_field_metadata.copy()
                side_id_field_metadata['name'] = '{}_{}'.format(
                    side, kwargs['id_field_name'])
                # Cannot create an OID-type field, so force to long.
                if side_id_field_metadata['type'].lower() == 'oid':
                    side_id_field_metadata['type'] = 'long'
                add_fields_from_metadata_list(
                    output_path, [side_id_field_metadata], log_level=None)
                update_field_by_joined_value(
                    dataset_path=output_path,
                    field_name=side_id_field_metadata['name'],
                    join_dataset_path=dataset_path,
                    join_field_name=kwargs['id_field_name'],
                    on_field_pairs=[(side_oid_field_name, oid_field_name)],
                    log_level=None)
            delete_field(
                output_path, side_oid_field_name, log_level=None)
    else:
        delete_field(output_path, 'ORIG_FID', log_level=None)
    helpers.log_line('end', _description, kwargs['log_level'])
    return output_path


@helpers.log_function
def convert_table_to_spatial_dataset(dataset_path, output_path, x_field_name,
                                     y_field_name, z_field_name=None,
                                     **kwargs):
    """Convert nonspatial coordinate table to a new spatial dataset.

    Args:
        dataset_path (str): Path of dataset.
        output_path (str): Path of output dataset.
    Kwargs:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        spatial_reference_id (int): EPSG code indicating the spatial reference
            the coordinates and output geometry are/will be in.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('dataset_where_sql', None), ('log_level', 'info'),
                          ('spatial_reference_id', 4326)]:
        kwargs.setdefault(*kwarg_default)
    kwargs['spatial_reference'] = (
        arcpy.SpatialReference(kwargs['spatial_reference_id'])
        if kwargs.get('spatial_reference_id') else None)
    _description = "Convert {} to spatial dataset.".format(dataset_path)
    helpers.log_line('start', _description, kwargs['log_level'])
    dataset_view_name = helpers.unique_name('dataset_view')
    try:
        arcpy.management.MakeXYEventLayer(
            table=dataset_path, out_layer=dataset_view_name,
            in_x_field=x_field_name, in_y_field=y_field_name,
            in_z_field=z_field_name,
            spatial_reference=kwargs['spatial_reference'])
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    copy_dataset(dataset_view_name, output_path,
                 dataset_where_sql=kwargs['dataset_where_sql'])
    delete_dataset(dataset_view_name)
    helpers.log_line('end', _description, kwargs['log_level'])
    return output_path


@helpers.log_function
def generate_facility_service_rings(dataset_path, output_path, network_path,
                                    cost_attribute, ring_width, max_distance,
                                    **kwargs):
    """Create facility service ring features using a network dataset.

    Args:
        dataset_path (str): Path of dataset.
        output_path (str): Path of output dataset.
        network_path (str): Path of network dataset.
        cost_attribute (str): Name of network cost attribute to use in
            analysis.
        ring_width (float): Distance a service ring represents in travel, in
            the dataset's units.
        max_distance (float): Distance in travel from the facility the outer
            ring will extend to, in the dataset's units.
    Kwargs:
        restriction_attributes (iter): Iterable of network restriction
            attribute names to use in analysis.
        travel_from_facility (bool): Flag indicating generating rings while
            travelling 'from' the facility. False indicate travelling 'to'.
        detailed_rings (bool): Flag indicating rings should be generated with
            high-detail.
        overlap_facilities (bool): Flag indicating whether different facility's
            rings can overlap.
        id_field_name (str): Name of facility ID field.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [
            ('dataset_where_sql', None), ('detailed_rings', False),
            ('id_field_name', None), ('log_level', 'info'),
            ('overlap_facilities', True), ('restriction_attributes', []),
            ('travel_from_facility', False)]:
        kwargs.setdefault(*kwarg_default)
    _description = "Generate service rings for facilities in {}".format(
        dataset_path)
    helpers.log_line('start', _description, kwargs['log_level'])
    helpers.toggle_arc_extension('Network', toggle_on=True)
    dataset_view_name = create_dataset_view(
        helpers.unique_name('dataset_view'), dataset_path,
        dataset_where_sql=kwargs['dataset_where_sql'], log_level=None)
    try:
        arcpy.na.MakeServiceAreaLayer(
            in_network_dataset=network_path,
            out_network_analysis_layer='service_area',
            impedance_attribute=cost_attribute,
            travel_from_to=('travel_from' if kwargs['travel_from_facility']
                            else 'travel_to'),
            default_break_values=' '.join(
                str(x) for x
                in range(ring_width, max_distance + 1, ring_width)),
            polygon_type=('detailed_polys' if kwargs['detailed_rings']
                          else 'simple_polys'),
            merge='no_merge' if kwargs['overlap_facilities'] else 'no_overlap',
            nesting_type='rings',
            UTurn_policy='allow_dead_ends_and_intersections_only',
            restriction_attribute_name=kwargs['restriction_attributes'],
            # The trim seems to override the non-overlapping part in
            # larger analyses.
            #polygon_trim=True, poly_trim_value=ring_width,
            hierarchy='no_hierarchy')
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    try:
        arcpy.na.AddLocations(
            in_network_analysis_layer="service_area", sub_layer="Facilities",
            in_table=dataset_view_name,
            field_mappings='Name {} #'.format(kwargs['id_field_name']),
            search_tolerance=max_distance, match_type='match_to_closest',
            append='clear', snap_to_position_along_network='no_snap',
            exclude_restricted_elements=True)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    delete_dataset(dataset_view_name, log_level=None)
    try:
        arcpy.na.Solve(in_network_analysis_layer="service_area",
                       ignore_invalids=True, terminate_on_solve_error=True)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    helpers.toggle_arc_extension('Network', toggle_off=True)
    copy_dataset('service_area/Polygons', output_path, log_level=None)
    delete_dataset('service_area', log_level=None)
    if kwargs['id_field_name']:
        id_field_metadata = properties.field_metadata(
            dataset_path, kwargs['id_field_name'])
        add_fields_from_metadata_list(
            output_path, [id_field_metadata], log_level=None)
        type_id_function_map = {
            'short': (lambda x: int(x.split(' : ')[0]) if x else None),
            'long': (lambda x: int(x.split(' : ')[0]) if x else None),
            'double': (lambda x: float(x.split(' : ')[0]) if x else None),
            'single': (lambda x: float(x.split(' : ')[0]) if x else None),
            'string': (lambda x: x.split(' : ')[0] if x else None),
            }
        update_field_by_function(
            output_path, kwargs['id_field_name'],
            function=type_id_function_map[id_field_metadata['type']],
            field_as_first_arg=False, arg_field_names=['Name'], log_level=None)
    helpers.log_line('end', _description, kwargs['log_level'])
    return output_path


@helpers.log_function
def planarize_features(dataset_path, output_path, **kwargs):
    """Convert feature geometry to lines - planarizing them.

    This method does not make topological linework. However it does carry
    all attributes with it, rather than just an ID attribute.

    Since this method breaks the new line geometry at intersections, it
    can be useful to break line geometry features at them.

    Args:
        dataset_path (str): Path of dataset.
        output_path (str): Path of output dataset.
    Kwargs:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    kwargs.setdefault('dataset_where_sql', None)
    kwargs.setdefault('log_level', 'info')
    _description = "Planarize features in {}.".format(dataset_path)
    helpers.log_line('start', _description, kwargs['log_level'])
    dataset_view_name = create_dataset_view(
        helpers.unique_name('dataset_view'), dataset_path,
        dataset_where_sql=kwargs['dataset_where_sql'], log_level=None)
    try:
        arcpy.management.FeatureToLine(
            in_features=dataset_view_name, out_feature_class=output_path,
            ##cluster_tolerance,
            attributes=True)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    delete_dataset(dataset_view_name, log_level=None)
    helpers.log_line('end', _description, kwargs['log_level'])
    return output_path


@helpers.log_function
def project(dataset_path, output_path, **kwargs):
    """Project dataset features to a new dataset.

    Not supplying a spatial reference ID defaults to unprojected WGS84.

    Args:
        dataset_path (str): Path of dataset.
        output_path (str): Path of output dataset.
    Kwargs:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        spatial_reference_id (int): EPSG code indicating the spatial reference
            output geometry will be in.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('dataset_where_sql', None), ('log_level', 'info'),
                          ('spatial_reference_id', 4326)]:
        kwargs.setdefault(*kwarg_default)
    kwargs['spatial_reference'] = (
        arcpy.SpatialReference(kwargs['spatial_reference_id'])
        if kwargs.get('spatial_reference_id') else None)
    _description = "Project {} to {}.".format(
        dataset_path, kwargs['spatial_reference'].name)
    helpers.log_line('start', _description, kwargs['log_level'])
    _dataset_metadata = properties.dataset_metadata(dataset_path)
    # Project tool cannot output to an in-memory workspace (will throw
    # error 000944). Not a bug. Esri's Project documentation (as of v10.4)
    # specifically states: "The in_memory workspace is not supported as a
    # location to write the output dataset."
    # To avoid all this ado, we'll create a clone dataset & copy features.
    create_dataset(
        output_path,
        [field for field in _dataset_metadata['fields']
         # Geometry & OID taken care of internally.
         if field['type'].lower() not in ('geometry ', 'oid')],
        geometry_type=_dataset_metadata['geometry_type'],
        spatial_reference_id=kwargs['spatial_reference_id'], log_level=None)
    copy_dataset(
        dataset_path, output_path,
        dataset_where_sql=kwargs['dataset_where_sql'], log_level=None)
    helpers.log_line('end', _description, kwargs['log_level'])
    return output_path


@helpers.log_function
def sort_features(dataset_path, output_path, sort_field_names, **kwargs):
    """Sort features into a new dataset.

    reversed_field_names are fields in sort_field_names that should have
    their sort order reversed.

    Args:
        dataset_path (str): Path of dataset.
        output_path (str): Path of output dataset.
        sort_field_names (iter): Iterable of field names to sort on, in order.
    Kwargs:
        reversed_field_names (iter): Iterable of field names (present in
            sort_field_names) to sort values in reverse-order.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('dataset_where_sql', None), ('log_level', 'info'),
                          ('reversed_field_names', [])]:
        kwargs.setdefault(*kwarg_default)
    _description = "Sort features in {} to {}.".format(
        dataset_path, output_path)
    helpers.log_line('start', _description, kwargs['log_level'])
    dataset_view_name = create_dataset_view(
        helpers.unique_name('dataset_view'), dataset_path,
        dataset_where_sql=kwargs['dataset_where_sql'], log_level=None)
    try:
        arcpy.management.Sort(
            in_dataset=dataset_view_name, out_dataset=output_path,
            sort_field=[
                (name, 'descending') if name in  kwargs['reversed_field_names']
                else (name, 'ascending') for name in sort_field_names],
            spatial_sort_method='UR')
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    delete_dataset(dataset_view_name, log_level=None)
    helpers.log_line('end', _description, kwargs['log_level'])
    return output_path


@helpers.log_function
def write_rows_to_csvfile(rows, output_path, field_names, **kwargs):
    """Write collected of rows to a CSV-file.

    The rows can be represented by either a dictionary or iterable.
    Args:
        rows (iter): Iterable of obejcts representing rows (iterables or
            dictionaries).
        output_path (str): Path of output dataset.
        field_names (iter): Iterable of field names, in the desired order.
    Kwargs:
        header (bool): Flag indicating whether to write a header to the output.
        file_mode (str): Code indicating the file mode for writing.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('file_mode', 'wb'), ('header', False),
                          ('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    _description = "Write rows iterable to CSV-file {}".format(output_path)
    helpers.log_line('start', _description, kwargs['log_level'])
    with open(output_path, kwargs['file_mode']) as csvfile:
        for index, row in enumerate(rows):
            if index == 0:
                if isinstance(row, dict):
                    writer = csv.DictWriter(csvfile, field_names)
                    if kwargs['header']:
                        writer.writeheader()
                elif isinstance(row, collections.Sequence):
                    writer = csv.writer(csvfile)
                    if kwargs['header']:
                        writer.writerow(field_names)
                else:
                    raise TypeError(
                        "Row objects must be dictionaries or sequences.")
            writer.writerow(row)
    helpers.log_line('end', _description, kwargs['log_level'])
    return output_path


@helpers.log_function
def xref_near_features(dataset_path, dataset_id_field_name,
                       xref_path, xref_id_field_name, **kwargs):
    """Generator for cross-referenced near feature-pairs.

    Yielded objects will include at least the dataset ID & XRef ID.
    Setting max_near_distance to NoneType will generate every possible
    feature cross-reference.
    Setting only_closest to True will generate a cross reference only with
    the closest feature.
    Setting any include_* to True will include that value in the generated
    tuple, in argument order.
    Distance values will match the linear unit of the main dataset.
    Angle values are in decimal degrees.

    Args:
        dataset_path (str): Path of dataset.
        dataset_id_field_name (str): Name of ID field.
        xref_path (str): Path of xref-dataset.
        xref_id_field_name (str): Name of xref ID field.
    Kwargs:
        max_near_distance (float): Maximum distance to search for near-
            features, in units of the dataset's spatial reference.
        only_closest (bool): Flag indicating only closest feature  will be
            cross-referenced.
        include_distance (bool): Flag to include distance in output.
        include_rank (bool): Flag to include rank in output.
        include_angle (bool): Flag to include angle in output.
        include_coordinates (bool): Flag to include coordinates in output.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
    Yields:
        tuple.
    """
    for kwarg_default in [
            ('dataset_where_sql', None), ('include_angle', False),
            ('include_coordinates', False), ('include_distance', False),
            ('include_rank', False), ('max_near_distance', None),
            ('only_closest', False)]:
        kwargs.setdefault(*kwarg_default)
    dataset_view_name = create_dataset_view(
        helpers.unique_name('dataset_view'), dataset_path,
        dataset_where_sql=kwargs['dataset_where_sql'], log_level=None)
    temp_near_path = helpers.unique_temp_dataset_path('temp_near')
    try:
        arcpy.analysis.GenerateNearTable(
            in_features=dataset_view_name, near_features=xref_path,
            out_table=temp_near_path,
            search_radius=kwargs['max_near_distance'],
            location=kwargs['include_coordinates'],
            angle=kwargs['include_angle'], closest=kwargs['only_closest'],
            method='geodesic')
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    near_field_names = ['in_fid', 'near_fid']
    for flag, field_name in [
            (kwargs['include_distance'], 'near_dist'),
            (kwargs['include_rank'], 'near_rank'),
            (kwargs['include_angle'], 'near_angle'),
            (kwargs['include_coordinates'], 'near_x'),
            (kwargs['include_coordinates'], 'near_y')]:
        if flag:
            near_field_names.append(field_name)
    dataset_oid_id_map = properties.oid_field_value_map(
        dataset_view_name, dataset_id_field_name)
    xref_oid_id_map = properties.oid_field_value_map(
        xref_path, xref_id_field_name)
    #pylint: disable=no-member
    with arcpy.da.SearchCursor(
        in_table=temp_near_path, field_names=near_field_names) as cursor:
    #pylint: enable=no-member
        for row in cursor:
            row_info = dict(zip(cursor.fields, row))
            result = [dataset_oid_id_map[row_info['in_fid']],
                      xref_oid_id_map[row_info['near_fid']]]
            if kwargs['include_distance']:
                result.append(row_info['near_dist'])
            if kwargs['include_rank']:
                result.append(row_info['near_rank'])
            if kwargs['include_angle']:
                result.append(row_info['near_angle'])
            if kwargs['include_coordinates']:
                result.append(row_info['near_x'])
                result.append(row_info['near_y'])
            yield tuple(result)
    delete_dataset(dataset_view_name)
    delete_dataset(temp_near_path)


# Workspace.

@helpers.log_function
def build_network(network_path, **kwargs):
    """Build network (dataset or geometric).

    Args:
        network_path (str): Path of network.
    Kwargs:
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    kwargs.setdefault('log_level', 'info')
    _description = "Build network {}.".format(network_path)
    helpers.log_line('start', _description, kwargs['log_level'])
    #pylint: disable=no-member
    network_type = arcpy.Describe(network_path).dataType
    #pylint: enable=no-member
    if network_type == 'GeometricNetwork':
        #pylint: disable=no-member
        _build = arcpy.management.RebuildGeometricNetwork
        #pylint: enable=no-member
        _build_kwargs = {'geometric_network': network_path,
                         # Geometric network build requires logfile output.
                         'out_log': os.path.join(tempfile.gettempdir(), 'log')}
    elif network_type == 'NetworkDataset':
        helpers.toggle_arc_extension('Network', toggle_on=True)
        _build = arcpy.na.BuildNetwork
        _build_kwargs = {'in_network_dataset': network_path}
    else:
        raise ValueError("{} not a valid network type.".format(network_path))
    try:
        _build(**_build_kwargs)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    helpers.toggle_arc_extension('Network', toggle_off=True)
    if 'out_log' in _build_kwargs:
        os.remove(_build_kwargs['out_log'])
    helpers.log_line('end', _description, kwargs['log_level'])
    return network_path


@helpers.log_function
def compress_geodatabase(geodatabase_path, **kwargs):
    """Compress geodatabase.

    Args:
        geodatabase_path (str): Path of geodatabase.
    Kwargs:
        disconnect_users (bool): Flag to disconnect users before compressing.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    kwargs.setdefault('disconnect_users', False)
    kwargs.setdefault('log_level', 'info')
    _description = "Compress {}.".format(geodatabase_path)
    helpers.log_line('start', _description, kwargs['log_level'])
    try:
        workspace_type = arcpy.Describe(geodatabase_path).workspaceType
    except AttributeError:
        raise ValueError(
            "{} not a valid geodatabase.".format(geodatabase_path))
    if workspace_type == 'LocalDatabase':
        _compress = arcpy.management.CompressFileGeodatabaseData
        _compress_kwargs = {'in_data': geodatabase_path}
        # Local databases cannot disconnect users (connections managed
        # by file access).
        kwargs['disconnect_users'] = False
    elif workspace_type == 'RemoteDatabase':
        _compress = arcpy.management.Compress
        _compress_kwargs = {'in_workspace': geodatabase_path}
    else:
        raise ValueError(
            "{} not a valid geodatabase type.".format(workspace_type))
    if kwargs['disconnect_users']:
        arcpy.AcceptConnections(sde_workspace=geodatabase_path,
                                accept_connections=False)
        arcpy.DisconnectUser(sde_workspace=geodatabase_path, users='all')
    try:
        _compress(**_compress_kwargs)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    if kwargs['disconnect_users']:
        arcpy.AcceptConnections(sde_workspace=geodatabase_path,
                                accept_connections=True)
    helpers.log_line('end', _description, kwargs['log_level'])
    return geodatabase_path


@helpers.log_function
def copy_dataset(dataset_path, output_path, **kwargs):
    """Copy features into a new dataset.

    Args:
        dataset_path (str): Path of dataset.
        output_path (str): Path of output dataset.
    Kwargs:
        schema_only (bool): Flag to copy only the schema, omitting the data.
        overwrite (bool): Flag to overwrite an existing dataset at the path.
        sort_field_names (iter): Iterable of field names to sort on, in order.
        sort_reversed_field_names (iter): Iterable of field names (present in
            sort_field_names) to sort values in reverse-order.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [
            ('dataset_where_sql', None), ('log_level', 'info'),
            ('overwrite', False), ('schema_only', False),
            ('sort_field_names', []), ('sort_reversed_field_names', [])]:
        kwargs.setdefault(*kwarg_default)
    _description = "Copy {} to {}.".format(dataset_path, output_path)
    helpers.log_line('start', _description, kwargs['log_level'])
    dataset_view_name = create_dataset_view(
        helpers.unique_name('dataset_view'), dataset_path,
        dataset_where_sql=(
            "0=1" if kwargs['schema_only'] else kwargs['dataset_where_sql']),
        log_level=None)
    _dataset_metadata = properties.dataset_metadata(dataset_path)
    if kwargs['sort_field_names']:
        _copy = arcpy.management.Sort
        ##, , sort_field, {spatial_sort_method}
        _copy_kwargs = {
            'in_dataset': dataset_view_name, 'out_dataset': output_path,
            'sort_field': [
                (name, 'descending')
                if name in kwargs['sort_reversed_field_names']
                else (name, 'ascending')
                for name in kwargs['sort_field_names']],
            'spatial_sort_method': 'UR'}
    elif _dataset_metadata['is_spatial']:
        _copy = arcpy.management.CopyFeatures
        _copy_kwargs = {'in_features': dataset_view_name,
                        'out_feature_class': output_path}
    elif _dataset_metadata['is_table']:
        _copy = arcpy.management.CopyRows
        _copy_kwargs = {'in_rows': dataset_view_name,
                        'out_table': output_path}
    else:
        raise ValueError("{} unsupported dataset type.".format(dataset_path))
    if kwargs['overwrite'] and properties.is_valid_dataset(output_path):
        delete_dataset(output_path, log_level=None)
    try:
        _copy(**_copy_kwargs)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    delete_dataset(dataset_view_name, log_level=None)
    helpers.log_line('end', _description, kwargs['log_level'])
    return output_path


@helpers.log_function
def create_dataset(dataset_path, field_metadata_list=None, **kwargs):
    """Create new dataset.

    Args:
        dataset_path (str): Path of dataset to create.
        field_metadata_list (iter): Iterable of field metadata dicts.
    Kwargs:
        geometry_type (str): Type of geometry, if a spatial dataset.
        spatial_reference_id (int): EPSG code for spatial reference, if a
            spatial dataset.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('geometry_type', None), ('log_level', 'info'),
                          ('spatial_reference_id', 4326)]:
        kwargs.setdefault(*kwarg_default)
    _description = "Create dataset {}.".format(dataset_path)
    helpers.log_line('start', _description, kwargs['log_level'])
    _create_kwargs = {'out_path': os.path.dirname(dataset_path),
                      'out_name': os.path.basename(dataset_path)}
    if kwargs['geometry_type']:
        _create = arcpy.management.CreateFeatureclass
        _create_kwargs['geometry_type'] = kwargs['geometry_type']
        # Default to EPSG 4326 (unprojected WGS 84).
        _create_kwargs['spatial_reference'] = arcpy.SpatialReference(
            kwargs['spatial_reference_id'])
    else:
        _create = arcpy.management.CreateTable
    try:
        _create(**_create_kwargs)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    if field_metadata_list:
        for _metadata in field_metadata_list:
            add_field(log_level=None, **_metadata)
    helpers.log_line('end', _description, kwargs['log_level'])
    return dataset_path


@helpers.log_function
def create_dataset_view(view_name, dataset_path, **kwargs):
    """Create new view of dataset.

    Args:
        view_name (str): Name of view to create.
        dataset_path (str): Path of dataset.
    Kwargs:
        force_nonspatial (bool): Flag ensure view is nonspatial.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('dataset_where_sql', None),
                          ('force_nonspatial', False), ('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    _description = "Create dataset view of {}.".format(dataset_path)
    helpers.log_line('start', _description, kwargs['log_level'])
    _dataset_metadata = properties.dataset_metadata(dataset_path)
    _create_kwargs = {'where_clause': kwargs['dataset_where_sql'],
                      'workspace': _dataset_metadata['workspace_path']}
    if _dataset_metadata['is_spatial'] and not kwargs['force_nonspatial']:
        _create = arcpy.management.MakeFeatureLayer
        _create_kwargs['in_features'] = dataset_path
        _create_kwargs['out_layer'] = view_name
    elif _dataset_metadata['is_table']:
        _create = arcpy.management.MakeTableView
        _create_kwargs['in_table'] = dataset_path
        _create_kwargs['out_view'] = view_name
    else:
        raise ValueError("{} unsupported dataset type.".format(dataset_path))
    try:
        _create(**_create_kwargs)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    helpers.log_line('end', _description, kwargs['log_level'])
    return view_name


@helpers.log_function
def create_file_geodatabase(geodatabase_path, **kwargs):
    """Create new file geodatabase.

    Args:
        geodatabase_path (str): Path of geodatabase to create.
    Kwargs:
        xml_workspace_path (str): Path of XML workspace document to define
            geodatabase with.
        include_xml_data (bool): Flag to include data from a provided XML
            workspace document, if it has any.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('include_xml_data', False), ('log_level', 'info'),
                          ('xml_workspace_path', None)]:
        kwargs.setdefault(*kwarg_default)
    _description = "Create file geodatabase at {}.".format(geodatabase_path)
    helpers.log_line('start', _description, kwargs['log_level'])
    if os.path.exists(geodatabase_path):
        LOG.warning("Geodatabase already exists.")
        return geodatabase_path
    try:
        arcpy.management.CreateFileGDB(
            out_folder_path=os.path.dirname(geodatabase_path),
            out_name=os.path.basename(geodatabase_path),
            out_version='current')
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    if kwargs['xml_workspace_path']:
        try:
            arcpy.management.ImportXMLWorkspaceDocument(
                target_geodatabase=geodatabase_path,
                in_file=kwargs['xml_workspace_path'],
                import_type=(
                    'data' if kwargs['include_xml_data'] else 'schema_only'),
                config_keyword='defaults')
        except arcpy.ExecuteError:
            LOG.exception("ArcPy execution.")
            raise
    helpers.log_line('end', _description, kwargs['log_level'])
    return geodatabase_path


@helpers.log_function
def create_geodatabase_xml_backup(geodatabase_path, output_path, **kwargs):
    """Create backup of geodatabase as XML workspace document.

    Args:
        geodatabase_path (str): Path of geodatabase.
        output_path (str): Path of output XML workspace document.
    Kwargs:
        include_data (bool): Flag to include data in backup.
        include_metadata (bool): Flag to include metadata in backup.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('include_data', False), ('include_metadata', True),
                          ('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    _description = "Create backup {} for {}.".format(
        geodatabase_path, output_path)
    helpers.log_line('start', _description, kwargs['log_level'])
    try:
        arcpy.management.ExportXMLWorkspaceDocument(
            in_data=geodatabase_path, out_file=output_path,
            export_type='data' if kwargs['include_data'] else 'schema_only',
            storage_type='binary', export_metadata=kwargs['include_metadata'])
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    helpers.log_line('end', _description, kwargs['log_level'])
    return output_path


@helpers.log_function
def delete_dataset(dataset_path, **kwargs):
    """Delete dataset.

    Args:
        dataset_path (str): Path of dataset.
    Kwargs:
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    kwargs.setdefault('log_level', 'info')
    _description = "Delete {}.".format(dataset_path)
    helpers.log_line('start', _description, kwargs['log_level'])
    try:
        arcpy.management.Delete(dataset_path)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    helpers.log_line('end', _description, kwargs['log_level'])
    return dataset_path


@helpers.log_function
def execute_sql_statement(statement, database_path, **kwargs):
    """Runs a SQL statement via SDE's SQL execution interface.

    Only works if path resolves to an actual SQL database.

    Args:
        dataset_path (str): Path of dataset.
    Kwargs:
        log_level (str): Level at which to log this function.
    Returns:
        str, tuple.
    """
    kwargs.setdefault('log_level', 'info')
    _description = "Execute SQL statement."
    helpers.log_line('start', _description, kwargs['log_level'])
    conn = arcpy.ArcSDESQLExecute(server=database_path)
    try:
        result = conn.execute(statement)
    except AttributeError:
        LOG.exception("Incorrect SQL syntax.")
        raise
    finally:
        del conn  # Yeah, what can you do?
    helpers.log_line('end', _description, kwargs['log_level'])
    return result


@helpers.log_function
def set_dataset_privileges(dataset_path, user_name, allow_view=None,
                           allow_edit=None, **kwargs):
    """Set privileges for dataset in enterprise geodatabase.

    For the allow-flags, True = grant; False = revoke; None = as is.

    Args:
        dataset_path (str): Path of dataset.
        allow_view (bool): Flag to allow or revoke view privileges.
        allow_edit (bool): Flag to allow or revoke edit privileges.
    Kwargs:
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    kwargs.setdefault('log_level', 'info')
    _description = "Set privileges for {} on {}.".format(
        user_name, dataset_path)
    helpers.log_line('start', _description, kwargs['log_level'])
    boolean_privilege_map = {True: 'grant', False: 'revoke', None: 'as_is'}
    try:
        arcpy.management.ChangePrivileges(
            in_dataset=dataset_path, user=user_name,
            View=boolean_privilege_map[allow_view],
            Edit=boolean_privilege_map[allow_edit])
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    helpers.log_line('end', _description, kwargs['log_level'])
    return dataset_path
