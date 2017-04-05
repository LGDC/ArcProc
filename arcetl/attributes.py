"""Attribute operations."""
import collections
import copy
import functools
import logging

import six

import arcpy

from arcetl import arcobj
from arcetl import dataset
from arcetl import helpers


LOG = logging.getLogger(__name__)


def as_dicts(dataset_path, field_names=None, **kwargs):
    """Generator for dictionaries of feature attributes.

    Use ArcPy cursor token names for object IDs and geometry objects/properties.

    Args:
        dataset_path (str): Path of the dataset.
        field_names (iter): Collection of field names. Names will be the keys
            in the dictionary mapping to their values.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        spatial_reference_id (int): EPSG code indicating the spatial reference
            output geometry will be in.

    Yields:
        dict: Mapping of feature attribute field names to values.
    """
    sref = arcobj.spatial_reference(kwargs.get('spatial_reference_id'))
    with arcpy.da.SearchCursor(
        in_table=dataset_path, field_names=field_names if field_names else '*',
        where_clause=kwargs.get('dataset_where_sql'), spatial_reference=sref
        ) as cursor:
        for feature in cursor:
            yield dict(zip(cursor.fields, feature))


def as_iters(dataset_path, field_names=None, **kwargs):
    """Generator for iterables of feature attributes.

    Use ArcPy cursor token names for object IDs and geometry objects/properties.

    Args:
        dataset_path (str): Path of the dataset.
        field_names (iter): Collection of field names. The order of the names
            in the collection will determine where its value will fall in the
            yielded item.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        iter_type: Iterable type to yield. Defaults to tuple.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        spatial_reference_id (int): EPSG code indicating the spatial reference
            output geometry will be in.

    Yields:
        iter: Collection of attribute values.
    """
    sref = arcobj.spatial_reference(kwargs.get('spatial_reference_id'))
    with arcpy.da.SearchCursor(
        in_table=dataset_path, field_names=field_names if field_names else '*',
        where_clause=kwargs.get('dataset_where_sql'), spatial_reference=sref
        ) as cursor:
        for feature in cursor:
            yield kwargs.get('iter_type', tuple)(feature)


def coordinate_node_info_map(dataset_path, from_id_field_name,
                             to_id_field_name, update_nodes=False, **kwargs):
    """Return dictionary mapping of coordinates to node-info dictionaries.

    Note:
        From & to IDs must be the same attribute type.

    Args:
        dataset_path (str): Path of the dataset.
        from_id_field_name (str): Name of the from-ID field.
        to_id_field_name (str): Name of the to-ID field.
        update_nodes (bool): Flag to indicate whether to update the nodes
            based on the feature geometries.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        id_field_name (str): Name of the ID field. Defaults to feature OID.

    Returns:
        dict: Mapping of coordinate tuples to node-info dictionaries.
            {(x, y): {'node_id': <id>, 'ids': {'from': set(), 'to': set()}}}
    """
    def node_feature_count(node):
        """Return feature count for node from info map."""
        return len(node['ids']['from'].union(node['ids']['to']))
    def update_coord_node_info_map(coord_node_info, node_id_metadata):
        """Return updated coordinate node info map."""
        coord_node_info = copy.deepcopy(coord_node_info)
        used_ids = {node['node_id'] for node in coord_node_info.values()
                    if node['node_id'] is not None}
        node_id_type = arcobj.python_type(node_id_metadata['type'])
        node_id_length = node_id_metadata['length']
        unused_ids = (
            i for i in helpers.unique_ids(node_id_type, node_id_length)
            if i not in used_ids
            )
        id_coords = {}
        for coord, node in coord_node_info.items():
            count = node_feature_count(node)
            # Assign IDs where missing.
            if node['node_id'] is None:
                node['node_id'] = next(unused_ids)
            # If ID duplicate, re-ID node with least features.
            elif node['node_id'] in id_coords:
                other_coord = id_coords[node['node_id']]
                new_node_id = next(unused_ids)
                if count > node_feature_count(coord_node_info[other_coord]):
                    coord_node_info[other_coord]['node_id'] = new_node_id
                    id_coords[new_node_id] = id_coords.pop(node['node_id'])
                else:
                    node['node_id'] = new_node_id
                    coord_node_info[coord]['node_id'] = node['node_id']
            id_coords[node['node_id']] = coord
        return coord_node_info
    id_key = kwargs.get('id_field_name', 'oid@')
    field_names = (id_key, from_id_field_name, to_id_field_name, 'shape@')
    coord_node_info = {}
    for feature in as_dicts(dataset_path, field_names,
                            dataset_where_sql=kwargs.get('dataset_where_sql')):
        for node in ({'name': 'from', 'id_key': from_id_field_name,
                      'geom_attr': 'firstPoint'},
                     {'name': 'to', 'id_key': to_id_field_name,
                      'geom_attr': 'lastPoint'}):
            geom = getattr(feature['shape@'], node['geom_attr'])
            coord = (geom.X, geom.Y)
            if coord not in coord_node_info:
                coord_node_info[coord] = {'node_id': feature[node['id_key']],
                                          'ids': collections.defaultdict(set)}
            if coord_node_info[coord]['node_id'] is None:
                coord_node_info[coord]['node_id'] = feature[node['id_key']]
            elif feature[node['id_key']] is not None:
                # Use lowest ID at coordinate.
                coord_node_info[coord]['node_id'] = min(
                    coord_node_info[coord]['node_id'], feature[node['id_key']]
                    )
            # Add feature ID to end-ID set.
            coord_node_info[coord]['ids'][node['name']].add(feature[id_key])
    if update_nodes:
        node_id_meta = arcobj.field_metadata(dataset_path, from_id_field_name)
        coord_node_info = update_coord_node_info_map(coord_node_info,
                                                     node_id_meta)
    return coord_node_info


def id_map(dataset_path, field_names, id_field_names=('oid@',), **kwargs):
    """Return dictionary mapping of field attribute for each feature ID.

    Note:
        There is no guarantee that the ID field(s) are unique.
        Use ArcPy cursor token names for object IDs and geometry objects/
        properties.

    Args:
        dataset_path (str): Path of the dataset.
        id_field_names (iterm, str): Name(s) of the ID field(s). Defaults to
            feature object ID.
        field_names (iter, str): Name(s) of the field(s).
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        spatial_reference_id (int): EPSG code indicating the spatial reference
            output geometry will be in.

    Returns:
        dict: Mapping of feature ID to feature attribute(s).
    """
    if isinstance(field_names, six.string_types):
        field_names = (field_names,)
    else:
        field_names = tuple(field_names)
    if isinstance(id_field_names, six.string_types):
        id_field_names = (id_field_names,)
    else:
        id_field_names = tuple(id_field_names)
    sref = arcobj.spatial_reference(kwargs.get('spatial_reference_id'))
    with arcpy.da.SearchCursor(
        dataset_path, field_names=id_field_names + field_names,
        where_clause=kwargs.get('dataset_where_sql'), spatial_reference=sref
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
        dataset_path (str): Path of the dataset.
        from_id_field_name (str): Name of the from-ID field.
        to_id_field_name (str): Name of the to-ID field.
        update_nodes (bool): Flag to indicate whether to update the nodes
            based on the feature geometries.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        field_names_as_keys (bool): Flag to indicate use of dataset's node
            ID field names as the ID field names in the map. Defaults to
            False.
        id_field_name (str): Name of the ID field. Defaults to feature OID.

    Returns:
        dict: Mapping of feature IDs to node-end ID dictionaries.
    """
    field_meta = {
        'from': arcobj.field_metadata(dataset_path, from_id_field_name),
        'to': arcobj.field_metadata(dataset_path, to_id_field_name),
        }
    if field_meta['from']['type'] != field_meta['to']['type']:
        raise ValueError("Fields %s & %s must be of same type.")
    if kwargs.get('field_names_as_keys', False):
        end_key = {'from': from_id_field_name, 'to': to_id_field_name}
    else:
        end_key = {'from': from_id_field_name, 'to': to_id_field_name}
    id_nodes = collections.defaultdict(dict)
    if update_nodes:
        coord_node_info = coordinate_node_info_map(
            dataset_path, from_id_field_name, to_id_field_name, update_nodes,
            **kwargs
            )
        for node in coord_node_info.values():
            for end in end_key:
                for feature_id in node['ids'][end]:
                    id_nodes[feature_id][end_key[end]] = node['node_id']
    else:
        id_key = kwargs.get('id_field_name', 'oid@')
        for feature in as_dicts(
                dataset_path, (id_key, from_id_field_name, to_id_field_name),
                dataset_where_sql=kwargs.get('dataset_where_sql')
            ):
            for end in end_key:
                id_nodes[feature[id_key]][end_key[end]] = feature[end_key[end]]
    return id_nodes


def update_by_domain_code(dataset_path, field_name, code_field_name,
                          domain_name, domain_workspace_path, **kwargs):
    """Update attribute values using a coded-values domain.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        code_field_name (str): Name of the field with related domain code.
        domain_name (str): Name of the domain.
        domain_workspace_path (str) Path of the workspace the domain is in.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        str: Name of the field updated.
    """
    log_level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(log_level, ("Start: Update attributes in %s on %s"
                        " by domain code in %s, using domain %s in %s."),
            field_name, dataset_path, code_field_name,
            domain_name, domain_workspace_path)
    domain_meta = arcobj.domain_metadata(domain_name, domain_workspace_path)
    update_by_function(
        dataset_path, field_name,
        function=domain_meta['code_description_map'].get,
        field_as_first_arg=False, arg_field_names=[code_field_name],
        dataset_where_sql=kwargs.get('dataset_where_sql'), log_level=None
        )
    LOG.log(log_level, "End: Update.")
    return field_name


def update_by_expression(dataset_path, field_name, expression, **kwargs):
    """Update attribute values using a (single) code-expression.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        expression (str): Python string expression to evaluate values from.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        str: Name of the field updated.
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
        'flag-value': Apply the flag_value argument value to matched features.
        'match-count': Apply the count of matched features.
        'sort-order': Apply the position of the feature sorted with matches.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        identifier_field_names (iter): Iterable of the field names used to
            identify a feature.
        update_type (str): Code indicating what values to apply to matched
            features.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        flag_value: Value to apply to matched features. Only used when
            update_type='flag-value'.
        log_level (str): Level to log the function at. Defaults to 'info'.
        sort_field_names (iter): Iterable of field names used to sort matched
            features. Only used when update_type='sort-order'.

    Returns:
        str: Name of the field updated.
    """
    log_level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(log_level, ("Start: Update attributes in %s on %s"
                        " by feature-matching %s on identifiers (%s)."),
            field_name, dataset_path, update_type, identifier_field_names)
    # valid_update_value_types = ('flag-value', 'match-count', 'sort-order')
    raise NotImplementedError
    # LOG.log(log_level, "End: Update.")
    # return field_name


def update_by_function(dataset_path, field_name, function, **kwargs):
    """Update attribute values by passing them to a function.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        function (types.FunctionType): Function to get values from.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        arg_field_names (iter): Iterable of the field names whose values will
            be the method arguments (not including the primary field).
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        field_as_first_arg (bool): Flag to indicate the field value will be
            the first argument for the method. Defaults to True.
        kwarg_field_names (iter): Iterable of the field names whose names &
            values will be the method keyword arguments.
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        str: Name of the field updated.
    """
    log_level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(log_level, "Start: Update attributes in %s on %s by function %s.",
            field_name, dataset_path, function)
    field_names = ((field_name,) + tuple(kwargs.get('arg_field_names', ()))
                   + tuple(kwargs.get('kwarg_field_names', ())))
    with arcpy.da.UpdateCursor(dataset_path, field_names,
                               kwargs.get('dataset_where_sql')) as cursor:
        for row in cursor:
            args_idx = len(kwargs.get('arg_field_names', ())) + 1
            if kwargs.get('field_as_first_arg', True):
                func_args = row[0:args_idx]
            else:
                func_args = row[1:args_idx]
            func_kwargs = dict(zip(kwargs.get('kwarg_field_names', ()),
                                   row[args_idx:]))
            new_value = function(*func_args, **func_kwargs)
            if row[0] != new_value:
                try:
                    cursor.updateRow([new_value] + row[1:])
                except RuntimeError:
                    LOG.error("Offending value is %s", new_value)
                    raise RuntimeError
    LOG.log(log_level, "End: Update.")
    return field_name


def update_by_function_map(dataset_path, field_name, function, key_field_name,
                           **kwargs):
    """Update attribute values by finding them in a function-created mapping.

    Note:
        Wraps update_by_function.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        function (types.FunctionType): Function to get values from.
        key_field_name (str): Name of the field whose values will be the
            mapping's key.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        default_value: Value to return from mapping if key value on feature
            not present. Defaults to NoneType.
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        str: Name of the field updated.
    """
    log_level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(log_level, ("Start: Update attributes in %s on %s"
                        " by function %s mapping with key in %s."),
            field_name, dataset_path, function, key_field_name)
    update_map = function()
    update_function = (lambda x: update_map.get(x, kwargs.get('default_value')))
    update_by_function(
        dataset_path, field_name, update_function,
        field_as_first_arg=False, arg_field_names=(key_field_name,),
        dataset_where_sql=kwargs.get('dataset_where_sql'), log_level=None,
        )
    LOG.log(log_level, "End: Update.")
    return field_name


def update_by_geometry(dataset_path, field_name, geometry_properties, **kwargs):
    """Update attribute values by cascading through the geometry properties.

    Note:
        If the spatial reference ID is not specified, the spatial reference of
        the dataset is used.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        geometry_properties (iter): Collection of the geometry property
            names in object-access order to retrieve the update value.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level to log the function at. Defaults to 'info'.
        spatial_reference_id (int): EPSG code indicating the spatial reference
            the geometry property will represent.

    Returns:
        str: Name of the field updated.
    """
    log_level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(log_level, ("Start: Update attributes in %s on %s"
                        " by geometry properties %s."),
            field_name, dataset_path, geometry_properties)
    def geometry_property_value(properties, geometry):
        """Return value of geometry property via ordered object properties."""
        property_transform = {
            'x-coordinate': ('X',), 'x': ('X',),
            'x-maximum': ('extent', 'XMax'), 'xmax': ('extent', 'XMax'),
            'x-minimum': ('extent', 'XMin'), 'xmin': ('extent', 'XMin'),
            'y-coordinate': 'Y', 'y': 'Y',
            'y-maximum': ('extent', 'YMax'), 'ymax': ('extent', 'YMax'),
            'y-minimum': ('extent', 'YMin'), 'ymin': ('extent', 'YMin'),
            'z-coordinate': 'Z', 'z': 'Z',
            'z-maximum': ('extent', 'ZMax'), 'zmax': ('extent', 'ZMax'),
            'z-minimum': ('extent', 'ZMin'), 'zmin': ('extent', 'ZMin'),
            }
        if geometry is None:
            value = None
        else:
            # Ensure properties are iterable.
            if isinstance(properties, six.string_types):
                properties = (properties,)
            # Replace stand-in codes with ordered properties.
            properties = tuple(property_transform.get(prop, (prop,))
                               for prop in properties)
            # Flatten iterable.
            properties = tuple(prop for props in properties for prop in props)
            value = functools.reduce(getattr, properties, geometry)
        return value
    sref = arcobj.spatial_reference(kwargs.get('spatial_reference_id'))
    with arcpy.da.UpdateCursor(
        in_table=dataset_path, field_names=(field_name, 'shape@'),
        where_clause=kwargs.get('dataset_where_sql'), spatial_reference=sref
        ) as cursor:
        for old_value, geometry in cursor:
            if geometry is None:
                new_value = None
            else:
                new_value = geometry_property_value(geometry_properties,
                                                    geometry)
            if new_value != old_value:
                cursor.updateRow((new_value, geometry))
    LOG.log(log_level, "End: Update.")
    return field_name


def update_by_joined_value(dataset_path, field_name, join_dataset_path,
                           join_field_name, on_field_pairs, **kwargs):
    """Update attribute values by referencing a joinable field.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        join_dataset_path (str): Path of the join-dataset.
        join_field_name (str): Name of the join-field.
        on_field_pairs (iter): Iterable of the field name pairs to used to
            determine join.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        str: Name of the field updated.
    """
    log_level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(log_level, ("Start: Update attributes in %s on %s"
                        " by joined values in %s on %s."),
            field_name, dataset_path, join_field_name, join_dataset_path)
    # Build join-reference.
    field_names = {
        'dataset': (field_name,) + tuple(pair[0] for pair in on_field_pairs),
        'join': (join_field_name,) + tuple(pair[1] for pair in on_field_pairs),
        }
    join_value_map = {
        tuple(feature[1:]): feature[0]
        for feature in as_iters(join_dataset_path, field_names['join'])
        }
    with arcpy.da.UpdateCursor(dataset_path, field_names['dataset'],
                               kwargs.get('dataset_where_sql')) as cursor:
        for row in cursor:
            new_value = join_value_map.get(tuple(row[1:]))
            if row[0] != new_value:
                cursor.updateRow([new_value] + list(row[1:]))
    LOG.log(log_level, "End: Update.")
    return field_name


def update_by_overlay(dataset_path, field_name, overlay_dataset_path,
                      overlay_field_name, **kwargs):
    """Update attribute values by finding overlay feature value.

    Note:
        Since only one value will be selected in the overlay, operations with
        multiple overlaying features will respect the geoprocessing
        environment's merge rule. This rule generally defaults to the
        'first' feature's value.

        Only one overlay flag at a time can be used (e.g.
        overlay_most_coincident, overlay_central_coincident). If multiple
        are set to True, the first one referenced in the code will be
        used. If no overlay flags are set, the operation will perform a
        basic intersection check, and the result will be at the whim of
        the geoprocessing environment's merge rule for the update field.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        overlay_dataset_path (str): Path of the overlay-dataset.
        overlay_field_name (str): Name of the overlay-field.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level to log the function at. Defaults to 'info'.
        overlay_central_coincident (bool): Flag to indicate overlay will use
            the centrally-coincident value. Defaults to False.
        overlay_most_coincident (bool): Flag to indicate overlay will use the
            most coincident value. Defaults to False.
        overlay_where_sql (str): SQL where-clause for overlay dataset
            subselection.
        replacement_value: Value to replace a present overlay-field value
            with.
        tolerance (float): Tolerance for coincidence, in dataset's units.

    Returns:
        str: Name of the field updated.
    """
    log_level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(log_level, ("Start: Update attributes in %s on %s"
                        " by overlay values in %s on %s."),
            field_name, dataset_path, overlay_field_name, overlay_dataset_path)
    # Check flags & set details for spatial join call.
    if kwargs.get('overlay_most_coincident', False):
        raise NotImplementedError(
            "overlay_most_coincident not yet implemented.")
    elif kwargs.get('overlay_central_coincident', False):
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
        if kwargs.get('tolerance') is not None:
            old_tolerance = arcpy.env.XYTolerance
            arcpy.env.XYTolerance = kwargs['tolerance']
        temp_output_path = helpers.unique_temp_dataset_path('output')
        arcpy.analysis.SpatialJoin(target_features=dataset_view.name,
                                   join_features=temp_overlay.path,
                                   out_feature_class=temp_output_path,
                                   **join_kwargs)
        if kwargs.get('tolerance') is not None:
            arcpy.env.XYTolerance = old_tolerance
    # Push overlay (or replacement) value from temp to update field.
    if kwargs.get('replacement_value') is not None:
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
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        str: Name of the field updated.
    """
    log_level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(log_level,
            "Start: Update attributes in %s on %s by assigning unique IDs.",
            field_name, dataset_path)
    field_meta = arcobj.field_metadata(dataset_path, field_name)
    unique_id_pool = helpers.unique_ids(
        data_type=arcobj.python_type(field_meta['type']),
        string_length=field_meta.get('length', 16)
        )
    with arcpy.da.UpdateCursor(dataset_path, (field_name,),
                               kwargs.get('dataset_where_sql')) as cursor:
        for _ in cursor:
            cursor.updateRow([next(unique_id_pool)])
    LOG.log(log_level, "End: Update.")
    return field_name


def update_by_value(dataset_path, field_name, value, **kwargs):
    """Update attribute values by assigning a given value.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        value (types.FunctionType): Static value to assign.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        str: Name of the field updated.
    """
    log_level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(log_level, "Start: Update attributes in %s on %s by given value.",
            field_name, dataset_path)
    with arcpy.da.UpdateCursor(dataset_path, (field_name,),
                               kwargs.get('dataset_where_sql')) as cursor:
        for row in cursor:
            if row[0] != value:
                cursor.updateRow([value])
    LOG.log(log_level, "End: Update.")
    return field_name
