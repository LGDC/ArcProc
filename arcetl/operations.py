# -*- coding=utf-8 -*-
"""Processing operation objects."""
import collections
import csv
import datetime
import inspect
import logging
import os
import uuid

import arcpy

from .helpers import (
    log_function, log_line, unique_ids, unique_name, unique_temp_dataset_path,
    )
from .properties import (
    dataset_metadata, feature_count, field_metadata, field_values,
    is_valid_dataset, oid_field_value_map
    )

FIELD_TYPE_AS_ARC = {'string': 'text', 'integer': 'long'}
FIELD_TYPE_AS_PYTHON = {
    'double': float, 'single': float,
    'integer': int, 'long': int, 'short': int, 'smallinteger': int,
    'guid': uuid.UUID,
    'string': str, 'text': str,
    }
GEOMETRY_PROPERTY_AS_ARC = {
    'area': ['area'],
    'centroid': ['centroid'],
    'extent': ['extent'],
    'length': ['length'],
    'x-coordinate': ['X'], 'x': ['X'],
    'x-maximum': ['extent', 'XMax'], 'xmax': ['extent', 'XMax'],
    'x-minimum': ['extent', 'XMin'], 'xmin': ['extent', 'XMin'],
    'y-coordinate': ['Y'], 'y': ['Y'],
    'y-maximum': ['extent', 'YMax'], 'ymax': ['extent', 'YMax'],
    'y-minimum': ['extent', 'YMin'], 'ymin': ['extent', 'YMin'],
    'z-coordinate': ['Z'], 'z': ['Z'],
    'z-maximum': ['extent', 'ZMax'], 'zmax': ['extent', 'ZMax'],
    'z-minimum': ['extent', 'ZMin'], 'zmin': ['extent', 'ZMin'],
    }
LOG = logging.getLogger(__name__)


# Schema.

@log_function
def add_field(dataset_path, field_name, field_type, field_length=None,
              field_precision=None, field_scale=None, field_is_nullable=True,
              field_is_required=False, log_level='info'):
    """Add field to dataset."""
    _description = "Add field {}.{}.".format(dataset_path, field_name)
    log_line('start', _description, log_level)
    ##if field_type.lower() == 'text' and field_length is None:
    ##    field_length = 64
    _add_kwargs = {
        'in_table': dataset_path, 'field_name': field_name,
        'field_type': FIELD_TYPE_AS_ARC.get(
            field_type.lower(), field_type.lower()),
        'field_length': field_length,
        'field_precision': field_precision, 'field_scale': field_scale,
        'field_is_nullable': field_is_nullable,
        'field_is_required': field_is_required,
        #'field_alias': None, #'field_domain': None,
        }
    # Set length if needed.
    if all([_add_kwargs['field_type'] == 'text',
            _add_kwargs['field_length'] is None]):
        _add_kwargs['field_length'] = 64
    try:
        arcpy.management.AddField(**_add_kwargs)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    log_line('end', _description, log_level)
    return field_name


@log_function
def add_fields_from_metadata_list(dataset_path, metadata_list,
                                  log_level='info'):
    """Add fields to dataset from list of metadata dictionaries."""
    _description = "Add fields to {} from a metadata list.".format(
        dataset_path)
    log_line('start', _description, log_level)
    for _field_metadata in metadata_list:
        _add_kwargs = {'dataset_path': dataset_path, 'log_level': None}
        for attribute in ['name', 'type', 'length', 'precision', 'scale',
                          'is_nullable', 'is_required']:
            if attribute in _field_metadata:
                _add_kwargs['field_{}'.format(attribute)] = (
                    _field_metadata[attribute])
        try:
            field_name = add_field(**_add_kwargs)
        except arcpy.ExecuteError:
            LOG.exception("ArcPy execution.")
            raise
        log_line('misc', "Added {}.".format(field_name), log_level)
    log_line('end', _description, log_level)
    return [_field_metadata['name'] for _field_metadata in metadata_list]


@log_function
def add_index(dataset_path, field_names, index_name=None, is_unique=False,
              is_ascending=False, log_level='info'):
    """Add index to dataset fields."""
    _description = "Add index for {}.{}.".format(dataset_path, field_names)
    log_line('start', _description, log_level)
    index_field_types = {
        field['type'].lower()
        for field in dataset_metadata(dataset_path)['fields']
        if field['name'].lower() in (name.lower() for name in field_names)}
    if 'geometry' in index_field_types:
        if len(field_names) != 1:
            raise RuntimeError("Cannot create a composite spatial index.")
        _add = arcpy.management.AddSpatialIndex
        _add_kwargs = {'in_features': dataset_path}
    else:
        _add = arcpy.management.AddIndex
        _add_kwargs = {'in_table': dataset_path, 'fields': field_names,
                       'index_name': (index_name if index_name
                                      else '_'.join(['ndx'] + field_names)),
                       'unique': is_unique, 'ascending': is_ascending}
    try:
        _add(**_add_kwargs)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    log_line('end', _description, log_level)
    return dataset_path


@log_function
def delete_field(dataset_path, field_name, log_level='info'):
    """Delete field from dataset."""
    _description = "Delete field {}.".format(field_name)
    log_line('start', _description, log_level)
    try:
        arcpy.management.DeleteField(in_table=dataset_path,
                                     drop_field=field_name)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    log_line('end', _description, log_level)
    return field_name


@log_function
def duplicate_field(dataset_path, field_name, new_field_name,
                    duplicate_values=False, dataset_where_sql=None,
                    log_level='info'):
    """Create new field as a duplicate of another."""
    _description = "Duplicate {}.{} as {}.".format(
        dataset_path, field_name, new_field_name)
    log_line('start', _description, log_level)
    _field_metadata = field_metadata(dataset_path, field_name)
    _field_metadata['name'] = new_field_name
    # Cannot add OID-type field, so push to a long-type.
    if _field_metadata['type'].lower() == 'oid':
        _field_metadata['type'] = 'long'
    add_fields_from_metadata_list(dataset_path, [_field_metadata],
                                  log_level=None)
    if duplicate_values:
        update_field_by_function(
            dataset_path, _field_metadata['name'],
            function=(lambda x: x), field_as_first_arg=False,
            arg_field_names=[field_name], dataset_where_sql=dataset_where_sql,
            log_level=None)
    log_line('end', _description, log_level)
    return new_field_name


@log_function
def join_field(dataset_path, join_dataset_path, join_field_name,
               on_field_name, on_join_field_name, log_level='info'):
    """Add field and its values from join-dataset."""
    _description = "Join field {} from {}.".format(
        join_field_name, join_dataset_path)
    log_line('start', _description, log_level)
    try:
        arcpy.management.JoinField(
            in_data=dataset_path, in_field=on_field_name,
            join_table=join_dataset_path, join_field=on_join_field_name,
            fields=join_field_name)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    log_line('end', _description, log_level)
    return join_field_name


@log_function
def rename_field(dataset_path, field_name, new_field_name, log_level='info'):
    """Rename field."""
    _description = "Rename field {}.{} to {}.".format(
        dataset_path, field_name, new_field_name)
    log_line('start', _description, log_level)
    try:
        arcpy.management.AlterField(in_table=dataset_path, field=field_name,
                                    new_field_name=new_field_name)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    log_line('end', _description, log_level)
    return new_field_name


# Features/attributes.

@log_function
def adjust_features_for_shapefile(dataset_path,
                                  datetime_null_replacement=datetime.date.min,
                                  integer_null_replacement=0,
                                  numeric_null_replacement=0.0,
                                  string_null_replacement='',
                                  log_level='info'):
    """Adjust features to meet shapefile requirements.

    Adjustments currently made:
    * Convert datetime values to date or time based on
    preserve_time_not_date flag.
    * Convert nulls to replacement value.
    """
    _description = "Adjust features in {} for shapefile output.".format(
        dataset_path)
    log_line('start', _description, log_level)
    log_line('feature_count', feature_count(dataset_path), log_level)
    type_function_map = {
        #'blob',  # Not a valid shapefile type.
        # Shapefiles can only store dates, not times.
        'date': lambda x: datetime_null_replacement if x is None else x.date(),
        'double': lambda x: numeric_null_replacement if x is None else x,
        #'geometry',  # Passed-through: Shapefile loader handles this.
        #'guid': Not valid shapefile type.
        'integer': lambda x: integer_null_replacement if x is None else x,
        #'oid',  # Passed-through: Shapefile loader handles this.
        #'raster',  # Not a valid shapefile type.
        'single': lambda x: numeric_null_replacement if x is None else x,
        'smallinteger': lambda x: integer_null_replacement if x is None else x,
        'string': lambda x: string_null_replacement if x is None else x,
        }
    for field in dataset_metadata(dataset_path)['fields']:
        if field['type'].lower() in type_function_map:
            update_field_by_function(
                dataset_path, field['name'],
                function=type_function_map[field['type'].lower()],
                log_level=None)
    log_line('feature_count', feature_count(dataset_path), log_level)
    log_line('end', _description, log_level)
    return dataset_path


@log_function
def clip_features(dataset_path, clip_dataset_path, dataset_where_sql=None,
                  clip_where_sql=None, log_level='info'):
    """Clip feature geometry where overlapping clip-geometry."""
    _description = "Clip {} where geometry overlaps {}.".format(
        dataset_path, clip_dataset_path)
    log_line('start', _description, log_level)
    log_line('feature_count', feature_count(dataset_path), log_level)
    dataset_view_name = create_dataset_view(
        unique_name('dataset_view'), dataset_path,
        dataset_where_sql, log_level=None)
    clip_dataset_view_name = create_dataset_view(
        unique_name('clip_dataset_view'), clip_dataset_path,
        clip_where_sql, log_level=None)
    temp_output_path = unique_temp_dataset_path('temp_output')
    try:
        arcpy.analysis.Clip(
            in_features=dataset_view_name,
            clip_features=clip_dataset_view_name,
            out_feature_class=temp_output_path)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    delete_dataset(clip_dataset_view_name, log_level=None)
    # Load back into the dataset.
    delete_features(dataset_view_name, log_level=None)
    delete_dataset(dataset_view_name, log_level=None)
    insert_features_from_path(dataset_path, temp_output_path, log_level=None)
    delete_dataset(temp_output_path, log_level=None)
    log_line('feature_count', feature_count(dataset_path), log_level)
    log_line('end', _description, log_level)
    return dataset_path


@log_function
def delete_features(dataset_path, dataset_where_sql=None, log_level='info'):
    """Delete select features."""
    _description = "Delete features from {} where {}.".format(
        dataset_path, dataset_where_sql)
    log_line('start', _description, log_level)
    log_line('feature_count', feature_count(dataset_path), log_level)
    dataset_view_name = create_dataset_view(
        unique_name('dataset_view'), dataset_path,
        dataset_where_sql, log_level=None)
    _dataset_metadata = dataset_metadata(dataset_path)
    # Can use (faster) truncate when:
    # (1) Database-type; (2) not in-memory; (3) no sub-selection.
    if all([dataset_where_sql is None,
            _dataset_metadata['data_type'] in ['FeatureClass', 'Table'],
            _dataset_metadata['workspace_path'] != 'in_memory']):
        _delete = arcpy.management.TruncateTable
        _delete_kwargs = {'in_table': dataset_view_name}
    elif _dataset_metadata['is_spatial']:
        _delete = arcpy.management.DeleteFeatures
        _delete_kwargs = {'in_features': dataset_view_name}
    elif _dataset_metadata['is_table']:
        _delete = arcpy.management.DeleteRows
        _delete_kwargs = {'in_rows': dataset_view_name}
    else:
        raise ValueError("{} unsupported dataset type.".format(dataset_path))
    _delete(**_delete_kwargs)
    delete_dataset(dataset_view_name, log_level=None)
    log_line('feature_count', feature_count(dataset_path), log_level)
    log_line('end', _description, log_level)
    return dataset_path


@log_function
def dissolve_features(dataset_path, dissolve_field_names, multipart=True,
                      unsplit_lines=False, dataset_where_sql=None,
                      log_level='info'):
    """Merge features that share values in given fields."""
    _description = "Dissolve features in {} on {}.".format(
        dataset_path, dissolve_field_names)
    log_line('start', _description, log_level)
    log_line('feature_count', feature_count(dataset_path), log_level)
    # Set the environment tolerance, so we can be sure the in_memory
    # datasets respect it. 0.003280839895013 is the default for all
    # datasets in our geodatabases.
    arcpy.env.XYTolerance = 0.003280839895013
    dataset_view_name = create_dataset_view(
        unique_name('dataset_view'), dataset_path, dataset_where_sql,
        log_level=None)
    temp_output_path = unique_temp_dataset_path('temp_output')
    try:
        arcpy.management.Dissolve(
            in_features=dataset_view_name, out_feature_class=temp_output_path,
            dissolve_field=dissolve_field_names, multi_part=multipart,
            unsplit_lines=unsplit_lines)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    # Delete undissolved features that are now dissolved (in the temp).
    delete_features(dataset_view_name, log_level=None)
    delete_dataset(dataset_view_name, log_level=None)
    # Copy the dissolved features (in the temp) to the dataset.
    insert_features_from_path(dataset_path, temp_output_path, log_level=None)
    delete_dataset(temp_output_path, log_level=None)
    log_line('feature_count', feature_count(dataset_path), log_level)
    log_line('end', _description, log_level)
    return dataset_path


@log_function
def erase_features(dataset_path, erase_dataset_path,
                   dataset_where_sql=None, erase_where_sql=None,
                   log_level='info'):
    """Erase feature geometry where overlaps erase dataset geometry."""
    _description = "Erase {} where geometry overlaps {}.".format(
        dataset_path, erase_dataset_path)
    log_line('start', _description, log_level)
    log_line('feature_count', feature_count(dataset_path), log_level)
    dataset_view_name = create_dataset_view(
        unique_name('dataset_view'), dataset_path,
        dataset_where_sql, log_level=None)
    erase_dataset_view_name = create_dataset_view(
        unique_name('erase_dataset_view'), erase_dataset_path,
        erase_where_sql, log_level=None)
    temp_output_path = unique_temp_dataset_path('temp_output')
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
    log_line('feature_count', feature_count(dataset_path), log_level)
    log_line('end', _description, log_level)
    return dataset_path


@log_function
def keep_features_by_location(dataset_path, location_dataset_path,
                              dataset_where_sql=None, location_where_sql=None,
                              log_level='info'):
    """Keep features where geometry overlaps location feature geometry."""
    _description = "Keep {} where geometry overlaps {}.".format(
        dataset_path, location_dataset_path)
    log_line('start', _description, log_level)
    log_line('feature_count', feature_count(dataset_path), log_level)
    dataset_view_name = create_dataset_view(
        unique_name('dataset_view'), dataset_path,
        dataset_where_sql,
        log_level=None)
    location_dataset_view_name = create_dataset_view(
        unique_name('location_dataset_view'), location_dataset_path,
        location_where_sql, log_level=None)
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
    log_line('feature_count', feature_count(dataset_path), log_level)
    log_line('end', _description, log_level)
    return dataset_path


@log_function
def identity_features(dataset_path, field_name,
                      identity_dataset_path, identity_field_name,
                      replacement_value=None, dataset_where_sql=None,
                      chunk_size=4096, log_level='info'):
    """Assign unique identity value to features, splitting where necessary.

    replacement_value is a value that will substitute as the identity
    value.
    This method has a 'chunking' routine in order to avoid an
    unhelpful output error that occurs when the inputs are rather large.
    For some reason, the identity will 'succeed' with and empty output
    warning, but not create an output dataset. Running the identity against
    smaller sets of data generally avoids this conundrum.
    """
    _description = "Identity features with {}.{}.".format(
        identity_dataset_path, identity_field_name)
    log_line('start', _description, log_level)
    log_line('feature_count', feature_count(dataset_path), log_level)
    # Create a temporary copy of the overlay dataset.
    temp_overlay_path = copy_dataset(
        identity_dataset_path, unique_temp_dataset_path('temp_overlay'),
        log_level=None)
    # Avoid field name collisions with neutral holding field.
    temp_overlay_field_name = duplicate_field(
        temp_overlay_path, identity_field_name,
        new_field_name=unique_name(identity_field_name),
        duplicate_values=True, log_level=None)
    # Get an iterable of all object IDs in the dataset.
    # Sorting is important, allows views with ID range instead of list.
    oids = sorted(oid for (oid,)
                  in field_values(dataset_path, ['oid@'], dataset_where_sql))
    while oids:
        # Get subset OIDs & remove them from full set.
        chunk = oids[:chunk_size]
        oids = oids[chunk_size:]
        LOG.debug("Chunk: Feature OIDs %s to %s", chunk[0], chunk[-1])
        # ArcPy where clauses cannot use 'between'.
        chunk_where_clause = (
            "{field} >= {from_oid} and {field} <= {to_oid}".format(
                field=dataset_metadata(dataset_path)['oid_field_name'],
                from_oid=chunk[0], to_oid=chunk[-1]))
        if dataset_where_sql:
            chunk_where_clause += " and ({})".format(dataset_where_sql)
        chunk_view_name = create_dataset_view(
            unique_name('chunk_view'), dataset_path,
            chunk_where_clause, log_level=None)
        # Create temporary dataset with the identity values.
        temp_output_path = unique_temp_dataset_path('temp_output')
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
        if replacement_value is not None:
            update_field_by_function(
                temp_output_path, field_name,
                function=(lambda x: replacement_value if x else None),
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
        insert_features_from_path(dataset_path, temp_output_path,
                                  log_level=None)
        delete_dataset(temp_output_path, log_level=None)
    delete_dataset(temp_overlay_path, log_level=None)
    log_line('feature_count', feature_count(dataset_path), log_level)
    log_line('end', _description, log_level)
    return dataset_path


@log_function
def insert_features_from_iterables(dataset_path, insert_dataset_iterables,
                                   field_names, log_level='info'):
    """Insert features from a collection of iterables."""
    _description = "Insert features into {} from iterables.".format(
        dataset_path)
    log_line('start', _description, log_level)
    log_line('feature_count', feature_count(dataset_path), log_level)
    # Create generator if insert_dataset_iterables is a generator function.
    if inspect.isgeneratorfunction(insert_dataset_iterables):
        insert_dataset_iterables = insert_dataset_iterables()
    with arcpy.da.InsertCursor(dataset_path, field_names) as cursor:
        for row in insert_dataset_iterables:
            cursor.insertRow(row)
    log_line('feature_count', feature_count(dataset_path), log_level)
    log_line('end', _description, log_level)
    return dataset_path


@log_function
def insert_features_from_path(dataset_path, insert_dataset_path,
                              field_names=None, insert_where_sql=None,
                              log_level='info'):
    """Insert features from a dataset referred to by a system path."""
    _description = "Insert features into {} from {}.".format(
        dataset_path, insert_dataset_path)
    log_line('start', _description, log_level)
    log_line('feature_count', feature_count(dataset_path), log_level)
    # Create field maps.
    # Added because ArcGIS Pro's no-test append is case-sensitive (verified
    # 1.0-1.1.1). BUG-000090970 - ArcGIS Pro 'No test' field mapping in
    # Append tool does not auto-map to the same field name if naming
    # convention differs.
    _dataset_metadata = dataset_metadata(dataset_path)
    if field_names:
        _field_names = [name.lower() for name in field_names]
    else:
        _field_names = [field['name'].lower()
                        for field in _dataset_metadata['fields']]
    insert_dataset_metadata = dataset_metadata(insert_dataset_path)
    insert_field_names = [field['name'].lower() for field
                          in insert_dataset_metadata['fields']]
    # Append takes care of geometry & OIDs independent of the field maps.
    for field_name_type in ('geometry_field_name', 'oid_field_name'):
        if _dataset_metadata.get(field_name_type):
            _field_names.remove(
                _dataset_metadata[field_name_type].lower())
            insert_field_names.remove(
                insert_dataset_metadata[field_name_type].lower())
    field_maps = arcpy.FieldMappings()
    for field_name in _field_names:
        if field_name in insert_field_names:
            field_map = arcpy.FieldMap()
            field_map.addInputField(insert_dataset_path, field_name)
            field_maps.addFieldMap(field_map)
    insert_dataset_view_name = create_dataset_view(
        unique_name('insert_dataset_view'), insert_dataset_path,
        insert_where_sql,
        # Insert view must be nonspatial to append to nonspatial table.
        force_nonspatial=(not _dataset_metadata['is_spatial']),
        log_level=None)
    try:
        arcpy.management.Append(
            inputs=insert_dataset_view_name, target=dataset_path,
            schema_type='no_test', field_mapping=field_maps)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    delete_dataset(insert_dataset_view_name, log_level=None)
    log_line('feature_count', feature_count(dataset_path), log_level)
    log_line('end', _description, log_level)
    return dataset_path


@log_function
def overlay_features(dataset_path, field_name, overlay_dataset_path,
                     overlay_field_name, replacement_value=None,
                     overlay_most_coincident=False,
                     overlay_central_coincident=False,
                     dataset_where_sql=None, chunk_size=4096,
                     log_level='info'):
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
    """
    _description = "Overlay features with {}.{}.".format(
        overlay_dataset_path, overlay_field_name)
    log_line('start', _description, log_level)
    log_line('feature_count', feature_count(dataset_path), log_level)
    # Check flags & set details for spatial join call.
    if overlay_most_coincident:
        raise NotImplementedError(
            "overlay_most_coincident not yet implemented.")
    elif overlay_central_coincident:
        _join_kwargs = {'join_operation': 'join_one_to_many',
                        'join_type': 'keep_all',
                        'match_option': 'have_their_center_in'}
    else:
        _join_kwargs = {'join_operation': 'join_one_to_many',
                        'join_type': 'keep_all',
                        'match_option': 'intersect'}
    # Create temporary copy of overlay dataset.
    temp_overlay_path = copy_dataset(
        overlay_dataset_path, unique_temp_dataset_path('temp_overlay'),
        log_level=None)
    # Avoid field name collisions with neutral holding field.
    temp_overlay_field_name = duplicate_field(
        temp_overlay_path, overlay_field_name,
        new_field_name=unique_name(overlay_field_name),
        duplicate_values=True, log_level=None)
    # Get an iterable of all object IDs in the dataset.
    # Sorting is important, allows views with ID range instead of list.
    oids = sorted(oid for (oid,)
                  in field_values(dataset_path, ['oid@'], dataset_where_sql))
    while oids:
        chunk = oids[:chunk_size]
        oids = oids[chunk_size:]
        LOG.debug("Chunk: Feature OIDs %s to %s", chunk[0], chunk[-1])
        # ArcPy where clauses cannot use 'between'.
        chunk_where_clause = (
            "{field} >= {from_oid} and {field} <= {to_oid}".format(
                field=dataset_metadata(dataset_path)['oid_field_name'],
                from_oid=chunk[0], to_oid=chunk[-1]))
        if dataset_where_sql:
            chunk_where_clause += " and ({})".format(dataset_where_sql)
        chunk_view_name = create_dataset_view(
            unique_name('chunk_view'), dataset_path,
            chunk_where_clause, log_level=None)
        # Create the temp output of the overlay.
        temp_output_path = unique_temp_dataset_path('temp_output')
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
        if replacement_value is not None:
            update_field_by_function(
                temp_output_path, field_name,
                function=(lambda x: replacement_value if x else None),
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
    log_line('feature_count', feature_count(dataset_path), log_level)
    log_line('end', _description, log_level)
    return dataset_path


@log_function
def union_features(dataset_path, field_name, union_dataset_path,
                   union_field_name, replacement_value=None,
                   dataset_where_sql=None, chunk_size=4096,
                   log_level='info'):
    """Assign unique union value to features, splitting where necessary.

    replacement_value is a value that will substitute as the union value.
    """
    _description = "Union features with {}.{}.".format(
        union_dataset_path, union_field_name)
    log_line('start', _description, log_level)
    log_line('feature_count', feature_count(dataset_path), log_level)
    # Create a temporary copy of the overlay dataset.
    temp_overlay_path = copy_dataset(
        union_dataset_path, unique_temp_dataset_path('temp_overlay'),
        log_level=None)
    # Avoid field name collisions with neutral holding field.
    temp_overlay_field_name = duplicate_field(
        temp_overlay_path, union_field_name,
        new_field_name=unique_name(union_field_name),
        duplicate_values=True, log_level=None)
    # Sorting is important, allows views with ID range instead of list.
    oids = sorted(oid for (oid,)
                  in field_values(dataset_path, ['oid@'], dataset_where_sql))
    while oids:
        chunk = oids[:chunk_size]
        oids = oids[chunk_size:]
        LOG.debug("Chunk: Feature OIDs %s to %s", chunk[0], chunk[-1])
        # ArcPy where clauses cannot use 'between'.
        chunk_where_clause = (
            "{field} >= {from_oid} and {field} <= {to_oid}".format(
                field=dataset_metadata(dataset_path)['oid_field_name'],
                from_oid=chunk[0], to_oid=chunk[-1]))
        if dataset_where_sql:
            chunk_where_clause += " and ({})".format(dataset_where_sql)
        chunk_view_name = create_dataset_view(
            unique_name('chunk_view'), dataset_path,
            chunk_where_clause, log_level=None)
        # Create the temp output of the union.
        temp_output_path = unique_temp_dataset_path('temp_output')
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
        if replacement_value is not None:
            update_field_by_function(
                temp_output_path, field_name,
                function=(lambda x: replacement_value if x else None),
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
        insert_features_from_path(dataset_path, temp_output_path,
                                  log_level=None)
        delete_dataset(temp_output_path, log_level=None)
    delete_dataset(temp_overlay_path, log_level=None)
    log_line('feature_count', feature_count(dataset_path), log_level)
    log_line('end', _description, log_level)
    return dataset_path


@log_function
def update_field_by_coded_value_domain(dataset_path, field_name,
                                       code_field_name, domain_name,
                                       domain_workspace_path,
                                       dataset_where_sql=None,
                                       log_level='info'):
    """Update field values using a coded-values domain."""
    _description = "Update field {} using domain {} referenced in {}.".format(
        field_name, domain_name, code_field_name)
    log_line('start', _description, log_level)
    code_description_map = next(
        domain for domain in arcpy.da.ListDomains(domain_workspace_path)
        if domain.name.lower() == domain_name.lower()).codedValues
    update_field_by_function(
        dataset_path, field_name, function=code_description_map.get,
        field_as_first_arg=False, arg_field_names=[code_field_name],
        dataset_where_sql=dataset_where_sql, log_level=None)
    log_line('end', _description, log_level)
    return field_name


@log_function
def update_field_by_constructor_method(dataset_path, field_name, constructor,
                                       method_name, field_as_first_arg=True,
                                       arg_field_names=None,
                                       kwarg_field_names=None,
                                       dataset_where_sql=None,
                                       log_level='info'):
    """Update field values by passing them to a constructed object method.

    wraps update_field_by_function.
    """
    _description = (
        "Update field {} using method {} from the object constructed by {}"
        ).format(field_name, method_name, constructor.__name__)
    log_line('start', _description, log_level)
    function = getattr(constructor(), method_name)
    update_field_by_function(
        dataset_path, field_name, function, field_as_first_arg,
        arg_field_names, kwarg_field_names, dataset_where_sql, log_level=None)
    log_line('end', _description, log_level)
    return field_name


@log_function
def update_field_by_expression(dataset_path, field_name, expression,
                               dataset_where_sql=None, log_level='info'):
    """Update field values using a (single) code-expression."""
    _description = "Update field {} using the expression <{}>.".format(
        field_name, expression)
    log_line('start', _description, log_level)
    dataset_view_name = create_dataset_view(
        unique_name('dataset_view'), dataset_path, dataset_where_sql,
        log_level=None)
    try:
        arcpy.management.CalculateField(
            in_table=dataset_view_name, field=field_name,
            expression=expression, expression_type='python_9.3')
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    delete_dataset(dataset_view_name, log_level=None)
    log_line('end', _description, log_level)
    return field_name


@log_function
def update_field_by_feature_matching(dataset_path, field_name,
                                     identifier_field_names,
                                     update_value_type, flag_value=None,
                                     sort_field_names=None,
                                     dataset_where_sql=None, log_level='info'):
    """Update field values by aggregating info about matching features."""
    ##valid_update_value_types = ['flag-value', 'match-count', 'sort-order']
    ##if sort_field_names is None:
    ##    sort_field_names = []
    raise NotImplementedError


@log_function
def update_field_by_function(dataset_path, field_name, function,
                             field_as_first_arg=True, arg_field_names=None,
                             kwarg_field_names=None, dataset_where_sql=None,
                             log_level='info'):
    """Update field values by passing them to a function.

    field_as_first_arg flag indicates that the function will consume the
    field's value as the first argument.
    arg_field_names indicate fields whose values will be positional
    arguments passed to the function.
    kwarg_field_names indicate fields who values will be passed as keyword
    arguments (field name as key).
    """
    _description = "Update field {} using function {}.".format(
        field_name, function.__name__)
    log_line('start', _description, log_level)
    if arg_field_names is None:
        arg_field_names = []
    if kwarg_field_names is None:
        kwarg_field_names = []
    with arcpy.da.UpdateCursor(
        dataset_path,
        field_names=(
            [field_name] + list(arg_field_names) + list(kwarg_field_names)),
        where_clause=dataset_where_sql) as cursor:
        for row in cursor:
            args = row[1:len(arg_field_names) + 1]
            if field_as_first_arg:
                args.insert(0, row[0])
            kwargs = dict(zip(kwarg_field_names,
                              row[len(arg_field_names) + 1:]))
            new_value = function(*args, **kwargs)
            if row[0] != new_value:
                cursor.updateRow([new_value] + list(row[1:]))
    log_line('end', _description, log_level)
    return field_name


@log_function
def update_field_by_geometry(dataset_path, field_name,
                             geometry_property_cascade, update_units=None,
                             dataset_where_sql=None, spatial_reference_id=None,
                             log_level='info'):
    """Update field values by cascading through a geometry's attributes.

    If the spatial reference ID is not specified, the spatial reference of
    the dataset is used.
    """
    _description = (
        "Update field {} values using geometry property cascade {}."
        ).format(field_name, geometry_property_cascade)
    log_line('start', _description, log_level)
    if update_units:
        raise NotImplementedError("update_units not yet implemented.")
    with arcpy.da.UpdateCursor(
        in_table=dataset_path, field_names=[field_name, 'shape@'],
        where_clause=dataset_where_sql,
        spatial_reference=(arcpy.SpatialReference(spatial_reference_id)
                           if spatial_reference_id else None)) as cursor:
        for field_value, geometry in cursor:
            if geometry is None:
                new_value = None
            else:
                new_value = geometry
                # Cascade down the geometry properties.
                for _property in geometry_property_cascade:
                    ##_property = _property.lower()
                    for _sub_property in GEOMETRY_PROPERTY_AS_ARC.get(
                            _property.lower(), [_property]):
                        new_value = getattr(new_value, _sub_property)
            if new_value != field_value:
                cursor.updateRow((new_value, geometry))
    log_line('end', _description, log_level)
    return field_name


@log_function
def update_field_by_joined_value(dataset_path, field_name, join_dataset_path,
                                 join_field_name, on_field_pairs,
                                 dataset_where_sql=None, log_level='info'):
    """Update field values by referencing a joinable field."""
    _description = "Update field {} with joined values from {}.{}>.".format(
        field_name, join_dataset_path, join_field_name)
    log_line('start', _description, log_level)
    # Build join-reference.
    with arcpy.da.SearchCursor(
        in_table=join_dataset_path,
        field_names=[join_field_name] + [pair[1] for pair in on_field_pairs]
        ) as cursor:
        join_value_map = {tuple(row[1:]): row[0] for row in cursor}
    with arcpy.da.UpdateCursor(
        in_table=dataset_path,
        field_names=[field_name] + [pair[0] for pair in on_field_pairs],
        where_clause=dataset_where_sql) as cursor:
        for row in cursor:
            new_value = join_value_map.get(tuple(row[1:]))
            if row[0] != new_value:
                cursor.updateRow([new_value] + list(row[1:]))
    log_line('end', _description, log_level)
    return field_name


@log_function
def update_field_by_near_feature(dataset_path, field_name, near_dataset_path,
                                 near_field_name, replacement_value=None,
                                 distance_field_name=None,
                                 angle_field_name=None,
                                 x_coordinate_field_name=None,
                                 y_coordinate_field_name=None,
                                 max_search_distance=None, near_rank=1,
                                 dataset_where_sql=None, log_level='info'):
    """Update field by finding near-feature value.

    One can optionally update ancillary fields with analysis properties by
    indicating the following fields: distance_field_name, angle_field_name,
    x_coordinate_field_name, y_coordinate_field_name.
    """
    _description = "Update field {} using near-values {}.{}.".format(
        field_name, near_dataset_path, near_field_name)
    log_line('start', _description, log_level)
    # Create a temporary copy of near dataset.
    temp_near_path = copy_dataset(
        near_dataset_path, unique_temp_dataset_path('temp_near'),
        log_level=None)
    # Avoid field name collisions with neutral holding field.
    temp_near_field_name = duplicate_field(
        temp_near_path, near_field_name,
        new_field_name=unique_name(near_field_name),
        duplicate_values=True, log_level=None)
    # Create the temp output of the near features.
    dataset_view_name = create_dataset_view(
        unique_name('dataset_view'), dataset_path, dataset_where_sql,
        log_level=None)
    temp_output_path = unique_temp_dataset_path('temp_output')
    try:
        arcpy.analysis.GenerateNearTable(
            in_features=dataset_view_name, near_features=temp_near_path,
            out_table=temp_output_path, search_radius=max_search_distance,
            location=any([x_coordinate_field_name, y_coordinate_field_name]),
            angle=any([angle_field_name]),
            closest='all', closest_count=near_rank,
            # Would prefer geodesic, but that forces XY values to lon-lat.
            method='planar')
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    delete_dataset(dataset_view_name, log_level=None)
    # Remove near rows not matching chosen rank.
    delete_features(
        temp_output_path,
        dataset_where_sql="near_rank <> {}".format(near_rank),
        log_level=None)
    # Join near values to the near output.
    join_field(
        temp_output_path, join_dataset_path=temp_near_path,
        join_field_name=temp_near_field_name, on_field_name='near_fid',
        on_join_field_name=dataset_metadata(temp_near_path)['oid_field_name'],
        log_level=None)
    delete_dataset(temp_near_path, log_level=None)
    # Push overlay (or replacement) value from temp to update field.
    # Apply replacement value if necessary.
    if replacement_value is not None:
        update_field_by_function(
            temp_output_path, field_name,
            function=(lambda x: replacement_value if x else None),
            field_as_first_arg=False, arg_field_names=[temp_near_field_name],
            log_level=None)
    else:
        update_field_by_function(
            temp_output_path, field_name, function=(lambda x: x),
            field_as_first_arg=False, arg_field_names=[temp_near_field_name],
            log_level=None)
    # Update values in original dataset.
    dataset_oid_field_name = dataset_metadata(dataset_path)['oid_field_name']
    update_field_by_joined_value(
        dataset_path, field_name,
        join_dataset_path=temp_output_path, join_field_name=field_name,
        on_field_pairs=[(dataset_oid_field_name, 'in_fid')],
        dataset_where_sql=dataset_where_sql, log_level=None)
    # Update ancillary near property fields.
    if distance_field_name:
        update_field_by_joined_value(
            dataset_path, distance_field_name,
            join_dataset_path=temp_output_path, join_field_name='near_dist',
            on_field_pairs=[(dataset_oid_field_name, 'in_fid')],
            dataset_where_sql=dataset_where_sql, log_level=None)
    if angle_field_name:
        update_field_by_joined_value(
            dataset_path, angle_field_name,
            join_dataset_path=temp_output_path, join_field_name='near_angle',
            on_field_pairs=[(dataset_oid_field_name, 'in_fid')],
            dataset_where_sql=dataset_where_sql, log_level=None)
    if x_coordinate_field_name:
        update_field_by_joined_value(
            dataset_path, x_coordinate_field_name,
            join_dataset_path=temp_output_path, join_field_name='near_x',
            on_field_pairs=[(dataset_oid_field_name, 'in_fid')],
            dataset_where_sql=dataset_where_sql, log_level=None)
    if y_coordinate_field_name:
        update_field_by_joined_value(
            dataset_path, y_coordinate_field_name,
            join_dataset_path=temp_output_path, join_field_name='near_y',
            on_field_pairs=[(dataset_oid_field_name, 'in_fid')],
            dataset_where_sql=dataset_where_sql, log_level=None)
    delete_dataset(temp_output_path, log_level=None)
    log_line('end', _description, log_level)
    return field_name


@log_function
def update_field_by_overlay(dataset_path, field_name,
                            overlay_dataset_path, overlay_field_name,
                            replacement_value=None,
                            overlay_most_coincident=False,
                            overlay_central_coincident=False,
                            dataset_where_sql=None, log_level='info'):
    """Update field by finding overlay feature value.

    Since only one value will be selected in the overlay, operations with
    multiple overlaying features will respect the geoprocessing
    environment's merge rule. This rule generally defaults to the 'first'
    feature's value.

    Please note that only one overlay flag at a time can be used (e.g.
    overlay_most_coincident, overlay_central_coincident). If mutliple are
    set to True, the first one referenced in the code will be used. If no
    overlay flags are set, the operation will perform a basic intersection
    check, and the result will be at the whim of the geoprocessing
    environment's merge rule for the update field.
    """
    _description = "Update field {} using overlay values {}.{}.".format(
        field_name, overlay_dataset_path, overlay_field_name)
    log_line('start', _description, log_level)
    # Check flags & set details for spatial join call.
    if overlay_most_coincident:
        raise NotImplementedError(
            "overlay_most_coincident not yet implemented.")
    elif overlay_central_coincident:
        _join_kwargs = {'join_operation': 'join_one_to_many',
                        'join_type': 'keep_all',
                        'match_option': 'have_their_center_in'}
    else:
        _join_kwargs = {'join_operation': 'join_one_to_many',
                        'join_type': 'keep_all',
                        'match_option': 'intersect'}
    # Create temporary copy of overlay dataset.
    temp_overlay_path = copy_dataset(
        overlay_dataset_path, unique_temp_dataset_path('temp_overlay'),
        log_level=None)
    # Avoid field name collisions with neutral holding field.
    temp_overlay_field_name = duplicate_field(
        temp_overlay_path, overlay_field_name,
        new_field_name=unique_name(overlay_field_name),
        duplicate_values=True, log_level=None)
    # Create temp output of the overlay.
    dataset_view_name = create_dataset_view(
        unique_name('dataset_view'), dataset_path, dataset_where_sql,
        log_level=None)
    temp_output_path = unique_temp_dataset_path('temp_output')
    try:
        arcpy.analysis.SpatialJoin(
            target_features=dataset_view_name, join_features=temp_overlay_path,
            out_feature_class=temp_output_path, **_join_kwargs)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    delete_dataset(dataset_view_name, log_level=None)
    delete_dataset(temp_overlay_path, log_level=None)
    # Push overlay (or replacement) value from temp to update field.
    # Apply replacement value if necessary.
    if replacement_value is not None:
        update_field_by_function(
            temp_output_path, field_name,
            function=(lambda x: replacement_value if x else None),
            field_as_first_arg=False,
            arg_field_names=[temp_overlay_field_name], log_level=None)
    else:
        update_field_by_function(
            temp_output_path, field_name, function=(lambda x: x),
            field_as_first_arg=False,
            arg_field_names=[temp_overlay_field_name], log_level=None)
    # Update values in original dataset.
    update_field_by_joined_value(
        dataset_path, field_name,
        join_dataset_path=temp_output_path, join_field_name=field_name,
        on_field_pairs=[(dataset_metadata(dataset_path)['oid_field_name'],
                         'target_fid')],
        dataset_where_sql=dataset_where_sql, log_level=None)
    delete_dataset(temp_output_path, log_level=None)
    log_line('end', _description, log_level)
    return field_name


@log_function
def update_field_by_unique_id(dataset_path, field_name, dataset_where_sql=None,
                              log_level='info'):
    """Update field values by assigning a unique ID."""
    _description = "Update field {} using unique IDs.".format(field_name)
    log_line('start', _description, log_level)
    _field_metadata = field_metadata(dataset_path, field_name)
    unique_id_pool = unique_ids(
        data_type=FIELD_TYPE_AS_PYTHON[_field_metadata['type']],
        string_length=_field_metadata.get('length', 16))
    with arcpy.da.UpdateCursor(in_table=dataset_path, field_names=[field_name],
                               where_clause=dataset_where_sql) as cursor:
        for row in cursor:
            cursor.updateRow([next(unique_id_pool)])
    log_line('end', _description, log_level)
    return field_name


@log_function
def update_fields_by_geometry_node_ids(dataset_path, from_id_field_name,
                                       to_id_field_name, log_level='info'):
    """Update fields with node IDs based on feature geometry.

    Method assumes the IDs are the same field type.
    """
    _description = (
        "Update node ID fields {} & {} based on feature geometry."
        ).format(from_id_field_name, to_id_field_name)
    log_line('start', _description, log_level)
    used_ids = set(tuple(field_values(dataset_path, [from_id_field_name]))
                   + tuple(field_values(dataset_path, [to_id_field_name])))
    _field_metadata = field_metadata(dataset_path, from_id_field_name)
    # Generator for open node IDs.
    open_node_ids = (
        _id for _id
        in unique_ids(FIELD_TYPE_AS_PYTHON[_field_metadata['type']],
                      _field_metadata['length'])
        if _id not in used_ids)
    # Build node XY mapping.
    with arcpy.da.SearchCursor(
        in_table=dataset_path,
        field_names=['oid@', from_id_field_name, to_id_field_name, 'shape@']
        ) as cursor:
        node_xy_map = {}
        # {node_xy: {'node_id': {id}, 'f_oids': set(), 't_oids': set()},}
        for oid, fnode_id, tnode_id, geometry in cursor:
            fnode_xy = (geometry.firstPoint.X, geometry.firstPoint.Y)
            tnode_xy = (geometry.lastPoint.X, geometry.lastPoint.Y)
            # Add the XY if not yet present.
            for node_id, node_xy, oid_set_key in [
                    (fnode_id, fnode_xy, 'f_oids'),
                    (tnode_id, tnode_xy, 't_oids')]:
                if node_xy not in node_xy_map:
                    # Add XY with the node ID.
                    node_xy_map[node_xy] = {'node_id': None,
                                            'f_oids': set(), 't_oids': set()}
                # Choose lowest non-missing ID to perpetuate at the XY.
                try:
                    node_xy_map[node_xy]['node_id'] = min(
                        x for x in [node_xy_map[node_xy]['node_id'], node_id]
                        if x is not None)
                # ValueError means there's no ID already on there.
                except ValueError:
                    node_xy_map[node_xy]['node_id'] = next(open_node_ids)
                # Add the link ID to the corresponding link set.
                node_xy_map[node_xy][oid_set_key].add(oid)
    # Pivot node_xy_map into a node ID map.
    node_id_map = {}
    # {node_id: {'node_xy': tuple(), 'feature_count': int()},}
    for new_xy in node_xy_map.keys():
        new_node_id = node_xy_map[new_xy]['node_id']
        new_feature_count = len(
            node_xy_map[new_xy]['f_oids'].union(
                node_xy_map[new_xy]['t_oids']))
        # If ID already present in node_id_map, re-ID one of the nodes.
        if new_node_id in node_id_map:
            next_open_node_id = next(open_node_ids)
            old_node_id = new_node_id
            old_xy = node_id_map[old_node_id]['node_xy']
            old_feature_count = node_id_map[old_node_id]['feature_count']
            # If new node has more links, re-ID old node.
            if new_feature_count > old_feature_count:
                node_xy_map[old_xy]['node_id'] = next_open_node_id
                node_id_map[next_open_node_id] = node_id_map.pop(old_node_id)
            # Re-ID new node if old node has more links (or tequal counts).
            else:
                node_xy_map[new_xy]['node_id'] = next_open_node_id
                new_node_id = next_open_node_id
        # Now add the new node.
        node_id_map[new_node_id] = {'node_xy': new_xy,
                                    'feature_count': new_feature_count}
    # Build a feature-node mapping from node_xy_map.
    feature_nodes = {}
    # {feature_oid: {'fnode': {id}, 'tnode': {id}},}
    for node_xy in node_xy_map:
        node_id = node_xy_map[node_xy]['node_id']
        # If feature object ID is missing in feature_nodes: add.
        for feature_oid in node_xy_map[node_xy]['f_oids'].union(
                node_xy_map[node_xy]['t_oids']):
            if feature_oid not in feature_nodes:
                feature_nodes[feature_oid] = {}
        for feature_oid in node_xy_map[node_xy]['f_oids']:
            feature_nodes[feature_oid]['fnode'] = node_id
        for feature_oid in node_xy_map[node_xy]['t_oids']:
            feature_nodes[feature_oid]['tnode'] = node_id
    # Push changes to features.
    with arcpy.da.UpdateCursor(
        in_table=dataset_path,
        field_names=['oid@', from_id_field_name, to_id_field_name]) as cursor:
        for oid, old_fnode_id, old_tnode_id in cursor:
            new_fnode_id = feature_nodes[oid]['fnode']
            new_tnode_id = feature_nodes[oid]['tnode']
            if any([old_fnode_id != new_fnode_id,
                    old_tnode_id != new_tnode_id]):
                cursor.updateRow([oid, new_fnode_id, new_tnode_id])
    log_line('end', _description, log_level)
    return from_id_field_name, to_id_field_name


# Products.

@log_function
def convert_polygons_to_lines(dataset_path, output_path, topological=False,
                              id_field_name=None, dataset_where_sql=None,
                              log_level='info'):
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
    """
    _description = "Convert polygon features in {} to lines.".format(
        dataset_path)
    log_line('start', _description, log_level)
    dataset_view_name = create_dataset_view(
        unique_name('dataset_view'), dataset_path, dataset_where_sql,
        log_level=None)
    try:
        arcpy.management.PolygonToLine(
            in_features=dataset_view_name, out_feature_class=output_path,
            neighbor_option=topological)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    delete_dataset(dataset_view_name)
    if topological:
        id_field_metadata = field_metadata(dataset_path, id_field_name)
        oid_field_name = dataset_metadata(dataset_path)['oid_field_name']
        for side in ('left', 'right'):
            side_oid_field_name = '{}_FID'.format(side.upper())
            if id_field_name:
                side_id_field_metadata = id_field_metadata.copy()
                side_id_field_metadata['name'] = '{}_{}'.format(
                    side, id_field_name)
                # Cannot create an OID-type field, so force to long.
                if side_id_field_metadata['type'].lower() == 'oid':
                    side_id_field_metadata['type'] = 'long'
                add_fields_from_metadata_list(
                    output_path, [side_id_field_metadata], log_level=None)
                update_field_by_joined_value(
                    dataset_path=output_path,
                    field_name=side_id_field_metadata['name'],
                    join_dataset_path=dataset_path,
                    join_field_name=id_field_name,
                    on_field_pairs=[(side_oid_field_name, oid_field_name)],
                    log_level=None)
            delete_field(output_path, side_oid_field_name, log_level=None)
    else:
        delete_field(output_path, 'ORIG_FID', log_level=None)
    log_line('end', _description, log_level)
    return output_path


@log_function
def convert_table_to_spatial_dataset(dataset_path, output_path, x_field_name,
                                     y_field_name, z_field_name=None,
                                     dataset_where_sql=None,
                                     spatial_reference_id=4326,
                                     log_level='info'):
    """Convert nonspatial coordinate table to a new spatial dataset."""
    _description = "Convert {} to spatial dataset.".format(dataset_path)
    log_line('start', _description, log_level)
    dataset_view_name = unique_name('dataset_view')
    try:
        arcpy.management.MakeXYEventLayer(
            table=dataset_path, out_layer=dataset_view_name,
            in_x_field=x_field_name, in_y_field=y_field_name,
            in_z_field=z_field_name,
            spatial_reference=arcpy.SpatialReference(spatial_reference_id))
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    copy_dataset(dataset_view_name, output_path, dataset_where_sql)
    delete_dataset(dataset_view_name)
    log_line('end', _description, log_level)
    return output_path


@log_function
def generate_facility_service_rings(dataset_path, output_path, network_path,
                                    cost_attribute, ring_width, max_distance,
                                    restriction_attributes=None,
                                    travel_from_facility=False,
                                    detailed_rings=False,
                                    overlap_facilities=True,
                                    id_field_name=None, dataset_where_sql=None,
                                    log_level='info'):
    """Create facility service ring features using a network dataset."""
    _description = "Generate service rings for facilities in {}".format(
        dataset_path)
    log_line('start', _description, log_level)
    # Get Network Analyst license.
    if arcpy.CheckOutExtension('Network') != 'CheckedOut':
        raise RuntimeError("Unable to check out Network Analyst license.")
    dataset_view_name = create_dataset_view(
        unique_name('dataset_view'), dataset_path, dataset_where_sql,
        log_level=None)
    try:
        arcpy.na.MakeServiceAreaLayer(
            in_network_dataset=network_path,
            out_network_analysis_layer='service_area',
            impedance_attribute=cost_attribute,
            travel_from_to=(
                'travel_from' if travel_from_facility else 'travel_to'),
            default_break_values=' '.join(
                str(x) for x
                in range(ring_width, max_distance + 1, ring_width)),
            polygon_type=(
                'detailed_polys' if detailed_rings else 'simple_polys'),
            merge='no_merge' if overlap_facilities else 'no_overlap',
            nesting_type='rings',
            UTurn_policy='allow_dead_ends_and_intersections_only',
            restriction_attribute_name=(
                restriction_attributes if restriction_attributes else []),
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
            field_mappings='Name {} #'.format(id_field_name),
            search_tolerance=max_distance, match_type='match_to_closest',
            append='clear', snap_to_position_along_network='no_snap',
            exclude_restricted_elements=True)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    try:
        arcpy.na.Solve(in_network_analysis_layer="service_area",
                       ignore_invalids=True, terminate_on_solve_error=True)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    copy_dataset('service_area/Polygons', output_path, log_level=None)
    id_field_metadata = field_metadata(dataset_path, id_field_name)
    add_fields_from_metadata_list(output_path, [id_field_metadata],
                                  log_level=None)
    type_id_function_map = {
        'short': (lambda x: int(x.split(' : ')[0]) if x else None),
        'long': (lambda x: int(x.split(' : ')[0]) if x else None),
        'double': (lambda x: float(x.split(' : ')[0]) if x else None),
        'single': (lambda x: float(x.split(' : ')[0]) if x else None),
        'string': (lambda x: x.split(' : ')[0] if x else None),
        }
    delete_dataset('service_area')
    update_field_by_function(
        output_path, id_field_name,
        function=type_id_function_map[id_field_metadata['type']],
        field_as_first_arg=False, arg_field_names=['Name'], log_level=None)
    delete_dataset(dataset_view_name, log_level=None)
    log_line('end', _description, log_level)
    return output_path


@log_function
def planarize_features(dataset_path, output_path, dataset_where_sql=None,
                       log_level='info'):
    """Convert feature geometry to lines - planarizing them.

    This method does not make topological linework. However it does carry
    all attributes with it, rather than just an ID attribute.

    Since this method breaks the new line geometry at intersections, it
    can be useful to break line geometry features at them.
    """
    _description = "Planarize features in {}.".format(dataset_path)
    log_line('start', _description, log_level)
    dataset_view_name = create_dataset_view(
        unique_name('dataset_view'), dataset_path, dataset_where_sql,
        log_level=None)
    try:
        arcpy.management.FeatureToLine(
            in_features=dataset_view_name, out_feature_class=output_path,
            ##cluster_tolerance,
            attributes=True)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    log_line('end', _description, log_level)
    return output_path


@log_function
def project(dataset_path, output_path, dataset_where_sql=None,
            spatial_reference_id=4326, log_level='info'):
    """Project dataset features to a new dataset."""
    _description = "Project {} to {}.".format(
        dataset_path, arcpy.SpatialReference(spatial_reference_id).name)
    log_line('start', _description, log_level)
    _dataset_metadata = dataset_metadata(dataset_path)
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
        _dataset_metadata['geometry_type'], spatial_reference_id,
        log_level=None)
    copy_dataset(dataset_path, output_path, dataset_where_sql, log_level=None)
    log_line('end', _description, log_level)
    return output_path


@log_function
def write_rows_to_csvfile(rows, output_path, field_names, header=False,
                          file_mode='wb', log_level='info'):
    """Write collected of rows to a CSV-file.

    The rows can be represented by either a dictionary or iterable.
    """
    _description = "Write rows iterable to CSV-file {}".format(output_path)
    log_line('start', _description, log_level)
    with open(output_path, file_mode) as csvfile:
        for index, row in enumerate(rows):
            if index == 0:
                if isinstance(row, dict):
                    writer = csv.DictWriter(csvfile, field_names)
                    if header:
                        writer.writeheader()
                elif isinstance(row, collections.Sequence):
                    writer = csv.writer(csvfile)
                    if header:
                        writer.writerow(field_names)
                else:
                    raise TypeError(
                        "Row objects must be dictionaries or sequences.")
            writer.writerow(row)
    log_line('end', _description, log_level)
    return output_path


@log_function
def xref_near_features(dataset_path, dataset_id_field_name,
                       xref_path, xref_id_field_name, max_near_distance=None,
                       only_closest=False, include_distance=False,
                       include_rank=False, include_angle=False,
                       include_x_coordinate=False, include_y_coordinate=False,
                       dataset_where_sql=None):
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
    """
    dataset_view_name = create_dataset_view(
        unique_name('dataset_view'), dataset_path, dataset_where_sql,
        log_level=None)
    temp_near_path = unique_temp_dataset_path('temp_near')
    try:
        arcpy.analysis.GenerateNearTable(
            in_features=dataset_view_name, near_features=xref_path,
            out_table=temp_near_path, search_radius=max_near_distance,
            location=any([include_x_coordinate, include_y_coordinate]),
            angle=include_angle, closest=only_closest,
            method='geodesic')
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    near_field_names = ['in_fid', 'near_fid']
    for flag, field_name in [
            (include_distance, 'near_dist'),
            (include_rank, 'near_rank'),
            (include_angle, 'near_angle'),
            (include_x_coordinate, 'near_x'),
            (include_y_coordinate, 'near_y')]:
        if flag:
            near_field_names.append(field_name)
    dataset_oid_id_map = oid_field_value_map(dataset_view_name,
                                             dataset_id_field_name)
    xref_oid_id_map = oid_field_value_map(xref_path, xref_id_field_name)
    with arcpy.da.SearchCursor(in_table=temp_near_path,
                               field_names=near_field_names) as cursor:
        for row in cursor:
            row_info = dict(zip(cursor.fields, row))
            result = [dataset_oid_id_map[row_info['in_fid']],
                      xref_oid_id_map[row_info['near_fid']]]
            if include_distance:
                result.append(row_info['near_dist'])
            if include_rank:
                result.append(row_info['near_rank'])
            if include_angle:
                result.append(row_info['near_angle'])
            if include_x_coordinate:
                result.append(row_info['near_x'])
            if include_y_coordinate:
                result.append(row_info['near_y'])
            yield tuple(result)
    delete_dataset(dataset_view_name)
    delete_dataset(temp_near_path)


# Workspace.

@log_function
def compress_geodatabase(geodatabase_path, disconnect_users=False,
                         log_level='info'):
    """Compress geodatabase."""
    _description = "Compress {}.".format(geodatabase_path)
    log_line('start', _description, log_level)
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
        disconnect_users = False
    elif workspace_type == 'RemoteDatabase':
        _compress = arcpy.management.Compress
        _compress_kwargs = {'in_workspace': geodatabase_path}
    else:
        raise ValueError(
            "{} not a valid geodatabase type.".format(workspace_type))
    if disconnect_users:
        arcpy.AcceptConnections(sde_workspace=geodatabase_path,
                                accept_connections=False)
        arcpy.DisconnectUser(sde_workspace=geodatabase_path, users='all')
    try:
        _compress(**_compress_kwargs)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    if disconnect_users:
        arcpy.AcceptConnections(sde_workspace=geodatabase_path,
                                accept_connections=True)
    log_line('end', _description, log_level)
    return geodatabase_path


@log_function
def copy_dataset(dataset_path, output_path, dataset_where_sql=None,
                 schema_only=False, overwrite=False, log_level='info'):
    """Copy dataset."""
    _description = "Copy {} to {}.".format(dataset_path, output_path)
    log_line('start', _description, log_level)
    dataset_view_name = create_dataset_view(
        unique_name('dataset_view'), dataset_path,
        dataset_where_sql="0=1" if schema_only else dataset_where_sql,
        log_level=None)
    _dataset_metadata = dataset_metadata(dataset_path)
    if _dataset_metadata['is_spatial']:
        _copy = arcpy.management.CopyFeatures
        _copy_kwargs = {'in_features': dataset_view_name,
                        'out_feature_class': output_path}
    elif _dataset_metadata['is_table']:
        _copy = arcpy.management.CopyRows
        _copy_kwargs = {'in_rows': dataset_view_name,
                        'out_table': output_path}
    else:
        raise ValueError("{} unsupported dataset type.".format(dataset_path))
    if overwrite and is_valid_dataset(output_path):
        delete_dataset(output_path, log_level=None)
    try:
        _copy(**_copy_kwargs)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    delete_dataset(dataset_view_name, log_level=None)
    log_line('end', _description, log_level)
    return output_path


@log_function
def create_dataset(dataset_path, field_metadata_list=None, geometry_type=None,
                   spatial_reference_id=None, log_level='info'):
    """Create new dataset."""
    _description = "Create dataset {}.".format(dataset_path)
    log_line('start', _description, log_level)
    _create_kwargs = {'out_path': os.path.dirname(dataset_path),
                      'out_name': os.path.basename(dataset_path)}
    if geometry_type:
        _create = arcpy.management.CreateFeatureclass
        _create_kwargs['geometry_type'] = geometry_type
        # Default to EPSG 4326 (unprojected WGS 84).
        _create_kwargs['spatial_reference'] = arcpy.SpatialReference(
            spatial_reference_id if spatial_reference_id else 4326)
    else:
        _create = arcpy.management.CreateTable
    try:
        _create(**_create_kwargs)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    if field_metadata_list:
        for _field_metadata in field_metadata_list:
            _add_kwargs = _field_metadata.copy()
            add_field(log_level=None, **_add_kwargs)
    log_line('end', _description, log_level)
    return dataset_path


@log_function
def create_dataset_view(view_name, dataset_path, dataset_where_sql=None,
                        force_nonspatial=False, log_level='info'):
    """Create new view of dataset."""
    _description = "Create dataset view of {}.".format(dataset_path)
    log_line('start', _description, log_level)
    _dataset_metadata = dataset_metadata(dataset_path)
    _create_kwargs = {'where_clause': dataset_where_sql,
                      'workspace': _dataset_metadata['workspace_path']}
    if _dataset_metadata['is_spatial'] and not force_nonspatial:
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
    log_line('end', _description, log_level)
    return view_name


@log_function
def create_file_geodatabase(geodatabase_path, xml_workspace_path=None,
                            include_xml_data=False, log_level='info'):
    """Create new file geodatabase."""
    _description = "Create file geodatabase at {}.".format(geodatabase_path)
    log_line('start', _description, log_level)
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
    if xml_workspace_path:
        try:
            arcpy.management.ImportXMLWorkspaceDocument(
                target_geodatabase=geodatabase_path,
                in_file=xml_workspace_path,
                import_type='data' if include_xml_data else 'schema_only',
                config_keyword='defaults')
        except arcpy.ExecuteError:
            LOG.exception("ArcPy execution.")
            raise
    log_line('end', _description, log_level)
    return geodatabase_path


@log_function
def create_geodatabase_xml_backup(geodatabase_path, output_path,
                                  include_data=False, include_metadata=True,
                                  log_level='info'):
    """Create backup of geodatabase as XML workspace document."""
    _description = "Create backup {} for {}.".format(
        geodatabase_path, output_path)
    log_line('start', _description, log_level)
    try:
        arcpy.management.ExportXMLWorkspaceDocument(
            in_data=geodatabase_path, out_file=output_path,
            export_type='data' if include_data else 'schema_only',
            storage_type='binary', export_metadata=include_metadata)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    log_line('end', _description, log_level)
    return output_path


@log_function
def delete_dataset(dataset_path, log_level='info'):
    """Delete dataset."""
    _description = "Delete {}.".format(dataset_path)
    log_line('start', _description, log_level)
    try:
        arcpy.management.Delete(dataset_path)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    log_line('end', _description, log_level)
    return dataset_path


@log_function
def execute_sql_statement(statement, path_to_database, log_level='info'):
    """Runs a SQL statement via SDE's SQL execution interface.

    This only works if path resolves to an actual SQL database.
    """
    _description = "Execute SQL statement."
    log_line('start', _description, log_level)
    conn = arcpy.ArcSDESQLExecute(server=path_to_database)
    try:
        result = conn.execute(statement)
    except AttributeError:
        LOG.exception("Incorrect SQL syntax.")
        raise
    finally:
        del conn  # Yeah, what can you do?
    log_line('end', _description, log_level)
    return result


@log_function
def set_dataset_privileges(dataset_path, user_name, allow_view=None,
                           allow_edit=None, log_level='info'):
    """Set privileges for dataset in enterprise geodatabase."""
    _description = "Set privileges for {} on {}.".format(
        user_name, dataset_path)
    log_line('start', _description, log_level)
    boolean_priviledge_map = {True: 'grant', False: 'revoke', None: 'as_is'}
    try:
        arcpy.management.ChangePrivileges(
            in_dataset=dataset_path, user=user_name,
            View=boolean_priviledge_map[allow_view],
            Edit=boolean_priviledge_map[allow_edit])
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    log_line('end', _description, log_level)
    return dataset_path
