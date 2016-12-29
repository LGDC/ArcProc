"""Attribute operations."""
from collections import defaultdict
import copy
import functools
import logging

import arcpy

from arcetl import arcobj
from arcetl import dataset
from arcetl import helpers

try:
    basestring
except NameError:
    basestring = (str, bytes)  # pylint: disable=redefined-builtin,invalid-name


LOG = logging.getLogger(__name__)


def _updated_node_coord_info_map(node_coord_info_map, force_to_type=None):
    """Return updated node coordinate info map with unique node IDs."""
    def feature_count(ids):
        """Count features on in the ID part of the coordinate info map."""
        return len(ids['from'].union(ids['to']))
    coord_info_map = copy.deepcopy(node_coord_info_map)
    used_ids = {info['node_id'] for info in coord_info_map.values()
                if info['node_id'] is not None}
    data_type = next(iter(used_ids)) if force_to_type is None else force_to_type
    string_length = (max(len(i) for i in used_ids)
                     if isinstance(data_type, basestring) else None)
    unused_ids = (i for i in helpers.unique_ids(data_type, string_length)
                  if i not in used_ids)
    id_coord_map = {}
    for coord in coord_info_map:
        node_id = coord_info_map[coord]['node_id']
        count = feature_count(coord_info_map[coord]['ids'])
        # Assign IDs where missing.
        if coord_info_map[coord]['node_id'] is None:
            node_id = next(unused_ids)
            coord_info_map[coord]['node_id'] = node_id
        # If ID duplicate, re-ID node with least features.
        elif node_id in id_coord_map:
            other_coord = id_coord_map[node_id]
            other_count = feature_count(coord_info_map[other_coord]['ids'])
            if count > other_count:
                other_node_id = next(unused_ids)
                coord_info_map[other_coord]['node_id'] = other_node_id
                id_coord_map[other_node_id] = id_coord_map.pop(node_id)
            else:
                node_id = next(unused_ids)
                coord_info_map[coord]['node_id'] = node_id
        id_coord_map[node_id] = coord
    return coord_info_map


def _feature_id_node_map(node_coord_info_map,
                         from_end_key='from', to_end_key='to'):
    """Return feature ID/node IDs map."""
    end_key_map = {'from': from_end_key, 'to': to_end_key}
    feature_id_node_map = {
        # <feature_id>: {'from': <id>, 'to': <id>}
        }
    for coord in node_coord_info_map:
        node_id = node_coord_info_map[coord]['node_id']
        for end in ['from', 'to']:
            for feature_id in node_coord_info_map[coord]['ids'][end]:
                if feature_id not in feature_id_node_map:
                    feature_id_node_map[feature_id] = {}
                feature_id_node_map[feature_id][end_key_map[end]] = node_id
    return feature_id_node_map


def as_dicts(dataset_path, field_names=None, **kwargs):
    """Generator for dictionaries of feature attributes.

    Use ArcPy cursor token names for object IDs and geometry objects/properties.

    Args:
        dataset_path (str): Path of dataset.
        field_names (iter): Iterable of field names.
    Kwargs:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        spatial_reference_id (int): EPSG code indicating the spatial reference
            output geometry will be in.
    Yields:
        dict.
    """
    for kwarg_default in [('dataset_where_sql', None),
                          ('spatial_reference_id', None)]:
        kwargs.setdefault(*kwarg_default)
    sref = arcobj.spatial_reference(kwargs['spatial_reference_id'])
    with arcpy.da.SearchCursor(
        in_table=dataset_path, field_names=field_names if field_names else '*',
        where_clause=kwargs['dataset_where_sql'], spatial_reference=sref
        ) as cursor:
        for feature in cursor:
            yield dict(zip(cursor.fields, feature))


def as_iters(dataset_path, field_names=None, **kwargs):
    """Generator for iterables of feature attributes.

    Use ArcPy cursor token names for object IDs and geometry objects/properties.

    Args:
        dataset_path (str): Path of dataset.
        field_names (iter): Iterable of field names.
    Kwargs:
        iter_type (object): Python iterable type to yield. Default is tuple.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        spatial_reference_id (int): EPSG code indicating the spatial reference
            output geometry will be in.
    Yields:
        iter.
    """
    for kwarg_default in [('dataset_where_sql', None), ('iter_type', tuple),
                          ('spatial_reference_id', None)]:
        kwargs.setdefault(*kwarg_default)
    sref = arcobj.spatial_reference(kwargs['spatial_reference_id'])
    with arcpy.da.SearchCursor(
        in_table=dataset_path, field_names=field_names if field_names else '*',
        where_clause=kwargs['dataset_where_sql'], spatial_reference=sref
        ) as cursor:
        for feature in cursor:
            yield kwargs['iter_type'](feature)


def id_map(dataset_path, field_names, id_field_names=('oid@',), **kwargs):
    """Return dictionary mapping of field attribute for each feature ID.

    There is no guarantee that the ID field(s) are unique.
    Use ArcPy cursor token names for object IDs and geometry objects/properties.

    Args:
        dataset_path (str): Path of dataset.
        id_field_names (iterm, str): Name(s) of id field(s).
            Defaults to feature object ID.
        field_names (iter, str): Name(s) of field.
    Kwargs:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        spatial_reference_id (int): EPSG code indicating the spatial reference
            output geometry will be in.
    Returns:
        dict.
    """
    for kwarg_default in [('dataset_where_sql', None),
                          ('spatial_reference_id', None)]:
        kwargs.setdefault(*kwarg_default)
    if isinstance(field_names, basestring):
        field_names = (field_names,)
    else:
        field_names = tuple(field_names)
    if isinstance(id_field_names, basestring):
        id_field_names = (id_field_names,)
    else:
        id_field_names = tuple(id_field_names)
    sref = arcobj.spatial_reference(kwargs['spatial_reference_id'])
    with arcpy.da.SearchCursor(
        dataset_path, field_names=id_field_names + field_names,
        where_clause=kwargs['dataset_where_sql'], spatial_reference=sref
        ) as cursor:
        result = {}
        for row in cursor:
            map_id = row[:len(id_field_names)]
            map_value = row[len(id_field_names):]
            if len(id_field_names) == 1:
                map_id = map_id[0]
            if len(field_names) == 1:
                map_value = map_value[0]
            result[map_id] = map_value
    return result


def id_node_map(dataset_path, from_id_field_name, to_id_field_name,
                update_nodes=False, **kwargs):
    """Return dictionary mapping of field node IDs for each feature ID.

    From & to IDs must be the same attribute type.

    Args:
        dataset_path (str): Path of dataset.
        from_id_field_name (str): Name of from-ID field.
        to_id_field_name (str): Name of to-ID field.
        update_nodes (bool): Flag indicating whether to update the nodes
            based on the feature geometries.
    Kwargs:
        id_field_name (str): Name of ID field. Defaults to feature OID.
        field_names_as_keys (bool): Flag indicating use of dataset's node ID
            field names as the ID field names in the map.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
    Returns:
        dict.    """
    for kwarg_default in [
            ('dataset_where_sql', None), ('id_field_name', 'oid@'),
            ('field_names_as_keys', False)
        ]:
        kwargs.setdefault(*kwarg_default)
    field_meta = {
        'from': arcobj.field_metadata(dataset_path, from_id_field_name),
        'to': arcobj.field_metadata(dataset_path, to_id_field_name),
        }
    if field_meta['from']['type'] != field_meta['to']['type']:
        raise ValueError("Fields %s & %s must be of same type.")
    field_names = (kwargs['id_field_name'],
                   from_id_field_name, to_id_field_name)
    if update_nodes:
        field_names = field_names + ('shape@',)
    coord_info_map = {
        # <coord>: {'node_id': <id>, 'ids': {'from': set(), 'to': set()}}
        }
    for feature in as_dicts(dataset_path, field_names, **kwargs):
        for end, node_id_key, geom_attr_name in (
                ('from', from_id_field_name, 'firstPoint'),
                ('to', to_id_field_name, 'lastPoint'),
            ):
            geom = getattr(feature['shape@'], geom_attr_name)
            coord = (geom.X, geom.Y)
            if coord not in coord_info_map:
                coord_info_map[coord] = {'node_id': feature[node_id_key],
                                         'ids': defaultdict(set)}
            # Use lowest ID at coordinate.
            if coord_info_map[coord]['node_id'] is None:
                coord_info_map[coord]['node_id'] = feature[node_id_key]
            elif feature[node_id_key] is not None:
                coord_info_map[coord]['node_id'] = min(
                    coord_info_map[coord]['node_id'], feature[node_id_key]
                    )
            # Add feature ID to end-ID set.
            coord_info_map[coord]['ids'][end].add(
                feature[kwargs['id_field_name']]
                )
    coord_info_map = _updated_node_coord_info_map(
        coord_info_map,
        force_to_type=arcobj.python_type(field_meta['from']['type'])
        )
    if kwargs['field_names_as_keys']:
        map_kwargs = {'from_end_key': from_id_field_name,
                      'to_end_key': to_id_field_name}
    else:
        map_kwargs = {}
    return _feature_id_node_map(coord_info_map, **map_kwargs)


def update_by_domain_code(dataset_path, field_name, code_field_name,
                          domain_name, domain_workspace_path, **kwargs):
    """Update attribute values using a coded-values domain.

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
    log_level = helpers.log_level(kwargs['log_level'])
    LOG.log(
        log_level, ("Start: Update attributes in %s on %s"
                    " by domain code in %s, using domain %s in %s."),
        field_name, dataset_path, code_field_name,
        domain_name, domain_workspace_path
        )
    domain_meta = arcobj.domain_metadata(domain_name, domain_workspace_path)
    update_by_function(
        dataset_path, field_name,
        function=domain_meta['code_description_map'].get,
        field_as_first_arg=False, arg_field_names=[code_field_name],
        dataset_where_sql=kwargs['dataset_where_sql'], log_level=None
        )
    LOG.log(log_level, "End: Update.")
    return field_name


def update_by_expression(dataset_path, field_name, expression, **kwargs):
    """Update attribute values using a (single) code-expression.

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
    log_level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(log_level,
            "Start: Update attributes in %s on %s by expression: ```%s```.",
            field_name, dataset_path, expression)
    with arcobj.DatasetView(dataset_path,
                            kwargs.get('dataset_where_sql')) as dataset_view:
        arcpy.management.CalculateField(in_table=dataset_view.name,
                                        field=field_name, expression=expression,
                                        expression_type='python_9.3')
    LOG.log(log_level, "End: Update.")
    return field_name


def update_by_feature_match(dataset_path, field_name, identifier_field_names,
                            update_type, **kwargs):
    """Update attribute values by aggregating info about matching features.

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
    log_level = helpers.log_level(kwargs['log_level'])
    LOG.log(log_level, ("Start: Update attributes in %s on %s"
                        " by feature-matching %s on identifiers (%s)."),
            field_name, dataset_path, update_type, identifier_field_names)
    valid_update_value_types = ('flag-value', 'match-count', 'sort-order')
    raise NotImplementedError
    LOG.log(log_level, "End: Update.")
    return field_name


def update_by_function(dataset_path, field_name, function, **kwargs):
    """Update attribute values by passing them to a function.

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
            ('log_level', 'info')
        ]:
        kwargs.setdefault(*kwarg_default)
    log_level = helpers.log_level(kwargs['log_level'])
    LOG.log(log_level, "Start: Update attributes in %s on %s by function %s.",
            field_name, dataset_path, function)
    with arcpy.da.UpdateCursor(
        in_table=dataset_path,
        field_names=((field_name,) + tuple(kwargs['arg_field_names'])
                     + tuple(kwargs['kwarg_field_names'])),
        where_clause=kwargs['dataset_where_sql']
        ) as cursor:
        for row in cursor:
            function_args = row[1:(len(kwargs['arg_field_names']) + 1)]
            if kwargs['field_as_first_arg']:
                function_args.insert(0, row[0])
            function_kwargs = dict(zip(
                kwargs['kwarg_field_names'],
                row[(len(kwargs['arg_field_names']) + 1):]
                ))
            new_value = function(*function_args, **function_kwargs)
            if row[0] != new_value:
                cursor.updateRow([new_value] + list(row[1:]))
    LOG.log(log_level, "End: Update.")
    return field_name


def update_by_function_map(dataset_path, field_name, function, key_field_name,
                           **kwargs):
    """Update attribute values by finding them in a function-created mapping.

    Wraps update_by_function.

    Args:
        dataset_path (str): Path of dataset.
        field_name (str): Name of field.
        function (object): Function executed to create map.
        key_field_name (str): Name of field whose values will be the map's key.
    Kwargs:
        default_value (object): Name of method to get values from.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('dataset_where_sql', None), ('default_value', None),
                          ('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    log_level = helpers.log_level(kwargs['log_level'])
    LOG.log(log_level, ("Start: Update attributes in %s on %s"
                        " by function %s mapping with key in %s."),
            field_name, dataset_path, function, key_field_name)
    function_map = function()
    if kwargs['default_value']:
        get_map = functools.partial(function_map.get, kwargs['default_value'])
    else:
        get_map = functools.partial(function_map.get)
    update_by_function(
        dataset_path, field_name, function=get_map,
        field_as_first_arg=False, arg_field_names=[key_field_name],
        dataset_where_sql=kwargs['dataset_where_sql'], log_level=None,
        )
    LOG.log(log_level, "End: Update.")
    return field_name


def update_by_geometry(dataset_path, field_name, geometry_property_cascade,
                       **kwargs):
    """Update attribute values by cascading through the geometry properties.

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
    log_level = helpers.log_level(kwargs['log_level'])
    LOG.log(log_level, ("Start: Update attributes in %s on %s"
                        " by geometry properties %s."),
            field_name, dataset_path, geometry_property_cascade)
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
    sref = arcobj.spatial_reference(kwargs['spatial_reference_id'])
    with arcpy.da.UpdateCursor(
        in_table=dataset_path, field_names=[field_name, 'shape@'],
        where_clause=kwargs.get('dataset_where_sql'), spatial_reference=sref
        ) as cursor:
        for field_value, geometry in cursor:
            if geometry is None:
                new_value = None
            else:
                new_value = geometry
                # Cascade down the geometry properties.
                for _property in geometry_property_cascade:
                    for sub_property in property_as_cascade.get(
                            _property.lower(), [_property]):
                        new_value = getattr(new_value, sub_property)
            if new_value != field_value:
                cursor.updateRow((new_value, geometry))
    LOG.log(log_level, "End: Update.")
    return field_name


def update_by_joined_value(dataset_path, field_name, join_dataset_path,
                           join_field_name, on_field_pairs, **kwargs):
    """Update attribute values by referencing a joinable field.

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
    log_level = helpers.log_level(kwargs['log_level'])
    LOG.log(log_level, ("Start: Update attributes in %s on %s"
                        " by joined values in %s on %s."),
            field_name, dataset_path, join_field_name, join_dataset_path)
    # Build join-reference.
    join_value_map = {
        tuple(feature[1:]): feature[0]
        for feature in as_iters(join_dataset_path,
                                field_names=[join_field_name]
                                + [p[1] for p in on_field_pairs])
        }
    with arcpy.da.UpdateCursor(
        in_table=dataset_path,
        field_names=[field_name] + [p[0] for p in on_field_pairs],
        where_clause=kwargs['dataset_where_sql']
        ) as cursor:
        for row in cursor:
            new_value = join_value_map.get(tuple(row[1:]))
            if row[0] != new_value:
                cursor.updateRow([new_value] + list(row[1:]))
    LOG.log(log_level, "End: Update.")
    return field_name


def update_by_overlay(dataset_path, field_name, overlay_dataset_path,
                      overlay_field_name, **kwargs):
    """Update attribute values by finding overlay feature value.

    Since only one value will be selected in the overlay, operations with
    multiple overlaying features will respect the geoprocessing
    environment's merge rule. This rule generally defaults to the 'first'
    feature's value.

    Please note that only one overlay flag at a time can be used (e.g.
    overlay_most_coincident, overlay_central_coincident). If multiple are
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
        tolerance (float): Tolerance for coincidence, in dataset's units.
        replacement_value: Value to replace present overlay-field value with.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        overlay_where_sql (str): SQL where-clause for overlay dataset
            subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [
            ('dataset_where_sql', None), ('log_level', 'info'),
            ('overlay_most_coincident', False),
            ('overlay_central_coincident', False), ('overlay_where_sql', None),
            ('replacement_value', None), ('tolerance', None)
        ]:
        kwargs.setdefault(*kwarg_default)
    log_level = helpers.log_level(kwargs['log_level'])
    LOG.log(log_level, ("Start: Update attributes in %s on %s"
                        " by overlay values in %s on %s."),
            field_name, dataset_path, overlay_field_name, overlay_dataset_path)
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
    dataset_view = arcobj.DatasetView(dataset_path,
                                      kwargs.get('dataset_where_sql'))
    # Create temporary copy of overlay dataset.
    temp_overlay = arcobj.TempDatasetCopy(overlay_dataset_path,
                                          kwargs.get('overlay_where_sql'))
    with dataset_view, temp_overlay:
        # Avoid field name collisions with neutral holding field.
        temp_overlay_field_name = dataset.duplicate_field(
            temp_overlay.path, overlay_field_name,
            new_field_name=helpers.unique_name(overlay_field_name),
            log_level=None
            )
        update_by_function(temp_overlay.path, temp_overlay_field_name,
                           function=(lambda x: x), field_as_first_arg=False,
                           arg_field_names=(overlay_field_name,),
                           log_level=None)
        # Create temp output of the overlay.
        if kwargs['tolerance']:
            old_tolerance = arcpy.env.XYTolerance
            arcpy.env.XYTolerance = kwargs['tolerance']
        temp_output_path = helpers.unique_temp_dataset_path('output')
        arcpy.analysis.SpatialJoin(target_features=dataset_view.name,
                                   join_features=temp_overlay.path,
                                   out_feature_class=temp_output_path,
                                   **join_kwargs)
        if kwargs['tolerance']:
            arcpy.env.XYTolerance = old_tolerance
    # Push overlay (or replacement) value from temp to update field.
    if kwargs['replacement_value'] is not None:
        function = (lambda x: kwargs['replacement_value'] if x else None)
    else:
        function = (lambda x: x)
    update_by_function(temp_output_path, field_name, function,
                       field_as_first_arg=False,
                       arg_field_names=(temp_overlay_field_name,),
                       log_level=None)
    # Update values in original dataset.
    oid_field_name = arcobj.dataset_metadata(dataset_path)['oid_field_name']
    update_by_joined_value(
        dataset_path, field_name,
        join_dataset_path=temp_output_path, join_field_name=field_name,
        on_field_pairs=((oid_field_name, 'target_fid'),),
        dataset_where_sql=kwargs.get('dataset_where_sql'), log_level=None
        )
    dataset.delete(temp_output_path, log_level=None)
    LOG.log(log_level, "End: Update.")
    return field_name


def update_by_unique_id(dataset_path, field_name, **kwargs):
    """Update attribute values by assigning a unique ID.

    Args:
        dataset_path (str): Path of dataset.
        field_name (str): Name of field.
    Kwargs:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('dataset_where_sql', None), ('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    log_level = helpers.log_level(kwargs['log_level'])
    LOG.log(log_level,
            "Start: Update attributes in %s on %s by assigning unique IDs.",
            field_name, dataset_path)
    field_meta = arcobj.field_metadata(dataset_path, field_name)
    unique_id_pool = helpers.unique_ids(
        data_type=arcobj.python_type(field_meta['type']),
        string_length=field_meta.get('length', 16)
        )
    with arcpy.da.UpdateCursor(dataset_path, (field_name,),
                               kwargs['dataset_where_sql']) as cursor:
        for _ in cursor:
            cursor.updateRow([next(unique_id_pool)])
    LOG.log(log_level, "End: Update.")
    return field_name


def update_by_value(dataset_path, field_name, value, **kwargs):
    """Update attribute values by assigning a given value.

    Args:
        dataset_path (str): Path of dataset.
        field_name (str): Name of field.
        value (types.FunctionType): Value to assign.
    Kwargs:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('dataset_where_sql', None), ('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    log_level = helpers.log_level(kwargs['log_level'])
    LOG.log(log_level, "Start: Update attributes in %s on %s by given value.",
            field_name, dataset_path)
    with arcpy.da.UpdateCursor(dataset_path, (field_name,),
                               kwargs['dataset_where_sql']) as cursor:
        for row in cursor:
            if row[0] != value:
                cursor.updateRow([value])
    LOG.log(log_level, "End: Update.")
    return field_name
