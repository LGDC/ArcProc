# -*- coding=utf-8 -*-
"""Module objects for attribute manipulation."""
import logging

import arcpy

from . import arcobj, helpers, operations, properties


LOG = logging.getLogger(__name__)


@helpers.log_function
def update_field_by_domain_code(dataset_path, field_name, code_field_name,
                                domain_name, domain_workspace_path, **kwargs):
    """Update field values using a coded-values domain.

    Args:
        dataset_path (str): Path of dataset.
        field_name (str): Name of field.
        code_field_name (str): Name of field with related domain code.
        domain_name (str): Name of domain.
        domain_workspace_path (str) Path of workspace domain is in.
    Kwargs:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('dataset_where_sql', None), ('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    meta = {
        'description':
            "Update field {} using domain {} referenced in {}.".format(
                field_name, domain_name, code_field_name)}
    helpers.log_line('start', meta['description'], kwargs['log_level'])
    ##TODO: After creating workspace submodule, create domain_metadata function.
    ##TODO: Then apply here.
    meta['domain'] = arcobj.domain_as_metadata(next(
        #pylint: disable=no-member
        domain for domain in arcpy.da.ListDomains(domain_workspace_path)
        #pylint: enable=no-member
        if domain.name.lower() == domain_name.lower()))
    update_field_by_function(
        dataset_path, field_name,
        function=meta['domain']['code_description_map'].get,
        field_as_first_arg=False, arg_field_names=[code_field_name],
        dataset_where_sql=kwargs['dataset_where_sql'], log_level=None)
    helpers.log_line('end', meta['description'], kwargs['log_level'])
    return field_name


@helpers.log_function
def update_field_by_expression(dataset_path, field_name, expression, **kwargs):
    """Update field values using a (single) code-expression.

    Args:
        dataset_path (str): Path of dataset.
        field_name (str): Name of field.
        expression (str): Python string expression to evaluate values from.
    Kwargs:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('dataset_where_sql', None), ('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    meta = {
        'description': "Update field {} using the expression <{}>.".format(
            field_name, expression)}
    helpers.log_line('start', meta['description'], kwargs['log_level'])
    meta['dataset_view_name'] = operations.create_dataset_view(
        helpers.unique_name('dataset_view'), dataset_path,
        dataset_where_sql=kwargs['dataset_where_sql'], log_level=None)
    try:
        arcpy.management.CalculateField(
            in_table=meta['dataset_view_name'], field=field_name,
            expression=expression, expression_type='python_9.3')
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    operations.delete_dataset(meta['dataset_view_name'], log_level=None)
    helpers.log_line('end', meta['description'], kwargs['log_level'])
    return field_name


@helpers.log_function
def update_field_by_feature_match(dataset_path, field_name,
                                  identifier_field_names, update_type,
                                  **kwargs):
    """Update field values by aggregating info about matching features.

    Valid update_type codes:
        flag-value: Apply the flag_value argument value to matched features.
        match-count: Apply the count of matched features.
        sort-order: Apply the position of the feature sorted with matches.

    Args:
        dataset_path (str): Path of dataset.
        field_name (str): Name of field.
        identifier_field_names (iter): Iterable of field names used to identify
            a feature.
        update_type (str): Code indicating how what values to apply to
            matched features.
    Kwargs:
        flag_value: Value to apply to matched features. Only used when
            update_type='flag-value'.
        sort_field_names (iter): Iterable of field names used to sort matched
            features. Only used when update_type='sort-order'.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('dataset_where_sql', None), ('flag_value', None),
                          ('log_level', 'info'), ('sort_field_names', None)]:
        kwargs.setdefault(*kwarg_default)
    meta = {
        'description': "Update field {} using feature-matching {}.".format(
            field_name, update_type)}
    helpers.log_line('start', meta['description'], kwargs['log_level'])
    ##valid_update_value_types = ['flag-value', 'match-count', 'sort-order']
    raise NotImplementedError
    ##helpers.log_line('end', meta['description'], kwargs['log_level'])
    ##return field_name


@helpers.log_function
def update_field_by_function(dataset_path, field_name, function, **kwargs):
    """Update field values by passing them to a function.

    field_as_first_arg flag indicates that the function will consume the
    field's value as the first argument.
    arg_field_names indicate fields whose values will be positional
    arguments passed to the function.
    kwarg_field_names indicate fields who values will be passed as keyword
    arguments (field name as key).

    Args:
        dataset_path (str): Path of dataset.
        field_name (str): Name of field.
        function (types.FunctionType): Function to get values from.
    Kwargs:
        field_as_first_arg (bool): Flag indicating the field value will be the
            first argument for the method.
        arg_field_names (iter): Iterable of field names whose values will be
            the method arguments (not including the primary field).
        kwarg_field_names (iter): Iterable of field names whose names & values
            will be the method keyword arguments.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
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
def update_field_by_geometry(dataset_path, field_name,
                             geometry_property_cascade, **kwargs):
    """Update field values by cascading through the geometry properties.

    If the spatial reference ID is not specified, the spatial reference of
    the dataset is used.

    Args:
        dataset_path (str): Path of dataset.
        field_name (str): Name of field.
        geometry_property_cascade (iter): Iterable of geometry properties, in
            object-access order.
    Kwargs:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        spatial_reference_id (int): EPSG code indicating the spatial reference
            to transform the geometry to for property representation.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('dataset_where_sql', None), ('log_level', 'info'),
                          ('spatial_reference_id', None)]:
        kwargs.setdefault(*kwarg_default)
    meta = {
        'description':
            "Update field {} using a geometry properties {}.".format(
                field_name, geometry_property_cascade)}
    helpers.log_line('start', meta['description'], kwargs['log_level'])
    meta['spatial_reference'] = (
        arcpy.SpatialReference(kwargs['spatial_reference_id'])
        if kwargs.get('spatial_reference_id') else None)
    property_as_cascade = {
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
    #pylint: disable=no-member
    with arcpy.da.UpdateCursor(
        #pylint: enable=no-member
        in_table=dataset_path, field_names=[field_name, 'shape@'],
        where_clause=kwargs.get('dataset_where_sql'),
        spatial_reference=meta['spatial_reference']) as cursor:
        for field_value, geometry in cursor:
            if geometry is None:
                new_value = None
            else:
                new_value = geometry
                # Cascade down the geometry properties.
                for _property in geometry_property_cascade:
                    ##_property = _property.lower()
                    for sub_property in property_as_cascade.get(
                            _property.lower(), [_property]):
                        new_value = getattr(new_value, sub_property)
            if new_value != field_value:
                cursor.updateRow((new_value, geometry))
    helpers.log_line('end', meta['description'], kwargs['log_level'])
    return field_name


@helpers.log_function
def update_field_by_instance_method(dataset_path, field_name, instance_class,
                                    method_name, **kwargs):
    """Update field values by passing them to a instanced class method.

    wraps update_field_by_function.

    Args:
        dataset_path (str): Path of dataset.
        field_name (str): Name of field.
        instance_class (type): Class that will be instanced.
        method_name (str): Name of method to get values from.
    Kwargs:
        field_as_first_arg (bool): Flag indicating the field value will be the
            first argument for the method.
        arg_field_names (iter): Iterable of field names whose values will be
            the method arguments (not including the primary field).
        kwarg_field_names (iter): Iterable of field names whose names & values
            will be the method keyword arguments.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [
            ('arg_field_names', []), ('dataset_where_sql', None),
            ('field_as_first_arg', True), ('kwarg_field_names', []),
            ('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    meta = {
        'description': "Update field {} using instance method {}().{}.".format(
            field_name, instance_class.__name__, method_name)}
    helpers.log_line('start', meta['description'], kwargs['log_level'])
    update_field_by_function(
        dataset_path, field_name,
        function=getattr(instance_class(), method_name),
        field_as_first_arg=kwargs['field_as_first_arg'],
        arg_field_names=kwargs['arg_field_names'],
        kwarg_field_names=kwargs['kwarg_field_names'],
        dataset_where_sql=kwargs['dataset_where_sql'], log_level=None)
    helpers.log_line('end', meta['description'], kwargs['log_level'])
    return field_name


@helpers.log_function
def update_field_by_joined_value(dataset_path, field_name, join_dataset_path,
                                 join_field_name, on_field_pairs,
                                 **kwargs):
    """Update field values by referencing a joinable field.

    Args:
        dataset_path (str): Path of dataset.
        field_name (str): Name of field.
        join_dataset_path (str): Path of join-dataset.
        join_field_name (str): Name of join-field.
        on_field_pairs (iter): Iterable of field name pairs to determine join.
    Kwargs:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
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


@helpers.log_function
def update_field_by_near_feature(dataset_path, field_name, near_dataset_path,
                                 near_field_name, **kwargs):
    """Update field by finding near-feature value.

    One can optionally update ancillary fields with analysis properties by
    indicating the following fields: distance_field_name, angle_field_name,
    x_coordinate_field_name, y_coordinate_field_name.

    Args:
        dataset_path (str): Path of dataset.
        field_name (str): Name of field.
        near_dataset_path (str): Path of near-dataset.
        near_field_name (str): Name of near-field.
    Kwargs:
        replacement_value: Value to replace present near-field value with.
        distance_field_name (str): Name of field to record distance.
        angle_field_name (str): Name of field to record angle.
        x_coordinate_field_name (str): Name of field to record x-coordinate.
        y_coordinate_field_name (str): Name of field to record y-coordinate.
        max_search_distance (float): Maximum distance to search for near-
            features.
        near_rank (int): Rank of near-feature to get field value from.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [
            ('angle_field_name', None), ('dataset_where_sql', None),
            ('distance_field_name', None), ('log_level', 'info'),
            ('max_search_distance', None), ('near_rank', 1),
            ('replacement_value', None), ('x_coordinate_field_name', None),
            ('y_coordinate_field_name', None)]:
        kwargs.setdefault(*kwarg_default)
    meta = {
        'description': "Update field {} using near-values {}.{}.".format(
            field_name, near_dataset_path, near_field_name)}
    helpers.log_line('start', meta['description'], kwargs['log_level'])
    meta['dataset'] = properties.dataset_metadata(dataset_path)
    # Create a temporary copy of near dataset.
    meta['temp_near_dataset'] = properties.dataset_metadata(
        operations.copy_dataset(
            near_dataset_path, helpers.unique_temp_dataset_path('temp_near'),
            log_level=None))
    # Avoid field name collisions with neutral holding field.
    meta['temp_near_field_name'] = operations.duplicate_field(
        meta['temp_near_dataset']['path'], near_field_name,
        new_field_name=helpers.unique_name(near_field_name),
        duplicate_values=True, log_level=None)
    # Create the temp output of the near features.
    meta['dataset_view_name'] = operations.create_dataset_view(
        helpers.unique_name('dataset_view'), dataset_path,
        dataset_where_sql=kwargs['dataset_where_sql'], log_level=None)
    meta['temp_output_path'] = helpers.unique_temp_dataset_path('temp_out')
    try:
        arcpy.analysis.GenerateNearTable(
            in_features=meta['dataset_view_name'],
            near_features=meta['temp_near_dataset']['path'],
            out_table=meta['temp_output_path'],
            search_radius=kwargs['max_search_distance'],
            location=any([kwargs['x_coordinate_field_name'],
                          kwargs['y_coordinate_field_name']]),
            angle=any([kwargs['angle_field_name']]),
            closest='all', closest_count=kwargs['near_rank'],
            # Would prefer geodesic, but that forces XY values to lon-lat.
            method='planar')
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    operations.delete_dataset(meta['dataset_view_name'], log_level=None)
    # Remove near rows not matching chosen rank.
    operations.delete_features(
        dataset_path=meta['temp_output_path'],
        dataset_where_sql="near_rank <> {}".format(kwargs['near_rank']),
        log_level=None)
    # Join ID values to the near output & rename facility_geofeature_id.
    operations.join_field(
        dataset_path=meta['temp_output_path'],
        join_dataset_path=meta['temp_near_dataset']['path'],
        join_field_name=meta['temp_near_field_name'], on_field_name='near_fid',
        on_join_field_name=meta['temp_near_dataset']['oid_field_name'],
        log_level=None)
    operations.delete_dataset(
        meta['temp_near_dataset']['path'], log_level=None)
    # Add update field to output.
    operations.add_fields_from_metadata_list(
        dataset_path=meta['temp_output_path'],
        metadata_list=[properties.field_metadata(dataset_path, field_name)],
        log_level=None)
    # Push overlay (or replacement) value from temp to update field.
    if kwargs['replacement_value'] is not None:
        update_function = lambda x: kwargs['replacement_value'] if x else None
    else:
        update_function = lambda x: x
    update_field_by_function(
        meta['temp_output_path'], field_name, function=update_function,
        field_as_first_arg=False,
        arg_field_names=[meta['temp_near_field_name']], log_level=None)
    # Update values in original dataset.
    field_join_map = {field_name: field_name}
    for keyword, join_name in [
            ('angle_field_name', 'near_angle'),
            ('distance_field_name', 'near_dist'),
            ('x_coordinate_field_name', 'near_x'),
            ('y_coordinate_field_name', 'near_y')]:
        if kwargs[keyword]:
            field_join_map[kwargs[keyword]] = join_name
    for _name, join_name in field_join_map.items():
        update_field_by_joined_value(
            dataset_path, field_name=_name, join_field_name=join_name,
            join_dataset_path=meta['temp_output_path'],
            on_field_pairs=[(meta['dataset']['oid_field_name'], 'in_fid')],
            dataset_where_sql=kwargs['dataset_where_sql'], log_level=None)
    operations.delete_dataset(meta['temp_output_path'], log_level=None)
    helpers.log_line('end', meta['description'], kwargs['log_level'])
    return field_name


@helpers.log_function
def update_field_by_overlay(dataset_path, field_name, overlay_dataset_path,
                            overlay_field_name, **kwargs):
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
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [
            ('overlay_most_coincident', False),
            ('overlay_central_coincident', False), ('replacement_value', None),
            ('dataset_where_sql', None), ('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    meta = {
        'description': "Update field {} using overlay values {}.{}.".format(
            field_name, overlay_dataset_path, overlay_field_name)}
    helpers.log_line('start', meta['description'], kwargs['log_level'])
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
    meta['dataset'] = properties.dataset_metadata(dataset_path)
    # Create temporary copy of overlay dataset.
    meta['temp_overlay_path'] = operations.copy_dataset(
        dataset_path=overlay_dataset_path,
        output_path=helpers.unique_temp_dataset_path('temp_overlay'),
        log_level=None)
    # Avoid field name collisions with neutral holding field.
    meta['temp_overlay_field_name'] = operations.duplicate_field(
        meta['temp_overlay_path'], overlay_field_name,
        new_field_name=helpers.unique_name(overlay_field_name),
        duplicate_values=True, log_level=None)
    # Create temp output of the overlay.
    meta['dataset_view_name'] = operations.create_dataset_view(
        helpers.unique_name('dataset_view'), dataset_path,
        dataset_where_sql=kwargs['dataset_where_sql'], log_level=None)
    meta['temp_output_path'] = helpers.unique_temp_dataset_path('temp_output')
    try:
        arcpy.analysis.SpatialJoin(
            target_features=meta['dataset_view_name'],
            join_features=meta['temp_overlay_path'],
            out_feature_class=meta['temp_output_path'], **_join_kwargs)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    operations.delete_dataset(meta['dataset_view_name'], log_level=None)
    operations.delete_dataset(meta['temp_overlay_path'], log_level=None)
    # Push overlay (or replacement) value from temp to update field.
    if kwargs['replacement_value'] is not None:
        update_function = lambda x: kwargs['replacement_value'] if x else None
    else:
        update_function = lambda x: x
    update_field_by_function(
        meta['temp_output_path'], field_name, function=update_function,
        field_as_first_arg=False,
        arg_field_names=[meta['temp_overlay_field_name']], log_level=None)
    # Update values in original dataset.
    update_field_by_joined_value(
        dataset_path, field_name,
        join_dataset_path=meta['temp_output_path'], join_field_name=field_name,
        on_field_pairs=[(meta['dataset']['oid_field_name'], 'target_fid')],
        dataset_where_sql=kwargs['dataset_where_sql'], log_level=None)
    operations.delete_dataset(meta['temp_output_path'], log_level=None)
    helpers.log_line('end', meta['description'], kwargs['log_level'])
    return field_name


@helpers.log_function
def update_field_by_unique_id(dataset_path, field_name, **kwargs):
    """Update field values by assigning a unique ID.

    Args:
        dataset_path (str): Path of dataset.
        field_name (str): Name of field.
    Kwargs:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    kwargs.setdefault('dataset_where_sql', None)
    kwargs.setdefault('log_level', 'info')
    _description = "Update field {} using unique IDs.".format(field_name)
    helpers.log_line('start', _description, kwargs['log_level'])
    meta = {'field': properties.field_metadata(dataset_path, field_name)}
    unique_id_pool = helpers.unique_ids(
        data_type=arcobj.FIELD_TYPE_AS_PYTHON[meta['field']['type']],
        string_length=meta['field'].get('length', 16))
    #pylint: disable=no-member
    with arcpy.da.UpdateCursor(
        #pylint: enable=no-member
        in_table=dataset_path, field_names=[field_name],
        where_clause=kwargs['dataset_where_sql']) as cursor:
        for _ in cursor:
            cursor.updateRow([next(unique_id_pool)])
    helpers.log_line('end', _description, kwargs['log_level'])
    return field_name


##TODO: Rename update_geometry_node_id_fields.
##TODO: Reduce branches & local vars with sub-functions.
@helpers.log_function
def update_fields_by_geometry_node_ids(dataset_path, from_id_field_name,
                                       to_id_field_name, **kwargs):
    """Update fields with node IDs based on feature geometry.

    Method assumes the IDs are the same field type.

    Args:
        dataset_path (str): Path of dataset.
        from_id_field_name (str): Name of from-ID field.
        to_id_field_name (str): Name of to-ID field.
    Kwargs:
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    kwargs.setdefault('log_level', 'info')
    meta = {
        'description':
            "Update node ID fields {} & {} using feature geometry.".format(
                from_id_field_name, to_id_field_name)}
    helpers.log_line('start', meta['description'], kwargs['log_level'])
    meta['field'] = properties.field_metadata(dataset_path, from_id_field_name)
    used_ids = set(
        tuple(properties.field_values(dataset_path, [from_id_field_name]))
        + tuple(properties.field_values(dataset_path, [to_id_field_name])))
    # Generator for open node IDs.
    open_node_ids = (
        i for i in helpers.unique_ids(
            arcobj.FIELD_TYPE_AS_PYTHON[meta['field']['type']],
            meta['field']['length'])
        if i not in used_ids)
    # Build node XY mapping.
    oid_fid_tid_geoms = properties.field_values(
        dataset_path,
        field_names=['oid@', from_id_field_name, to_id_field_name, 'shape@'])
    node_xy_map = {}
    # {node_xy: {'node_id': {id}, 'f_oids': set(), 't_oids': set()}}
    for oid, fid, tid, geom in oid_fid_tid_geoms:
        fnode_xy = (geom.firstPoint.X, geom.firstPoint.Y)
        tnode_xy = (geom.lastPoint.X, geom.lastPoint.Y)
        # Add the XY if not yet present.
        for node_id, node_xy, oid_set_key in [
                (fid, fnode_xy, 'f_oids'), (tid, tnode_xy, 't_oids')]:
            if node_xy not in node_xy_map:
                # Add XY with the node ID.
                node_xy_map[node_xy] = {
                    'node_id': None, 'f_oids': set(), 't_oids': set()}
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
    # {node_id: {'node_xy': tuple(), 'feature_count': int()}}
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
        node_id_map[new_node_id] = {
            'node_xy': new_xy, 'feature_count': new_feature_count}
    # Build a feature-node mapping from node_xy_map.
    feature_nodes = {}
    # {feature_oid: {'fnode': {id}, 'tnode': {id}}}
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
    #pylint: disable=no-member
    with arcpy.da.UpdateCursor(
        #pylint: enable=no-member
        in_table=dataset_path,
        field_names=['oid@', from_id_field_name, to_id_field_name]) as cursor:
        for oid, old_fnode_id, old_tnode_id in cursor:
            new_fnode_id = feature_nodes[oid]['fnode']
            new_tnode_id = feature_nodes[oid]['tnode']
            if any([old_fnode_id != new_fnode_id,
                    old_tnode_id != new_tnode_id]):
                cursor.updateRow([oid, new_fnode_id, new_tnode_id])
    helpers.log_line('end', meta['description'], kwargs['log_level'])
    return (from_id_field_name, to_id_field_name)

