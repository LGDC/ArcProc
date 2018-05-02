"""Attribute operations."""
from collections import Counter, defaultdict
import copy
import logging
import sys

import arcpy

from arcetl import arcobj
from arcetl import dataset
from arcetl.helpers import (
    contain, leveled_logger, property_value, unique_ids, unique_name, unique_path
)

if sys.version_info.major >= 3:
    basestring = str


LOG = logging.getLogger(__name__)

GEOMETRY_PROPERTY_TRANSFORM = {
    'x-coordinate': ['X'],
    'x': ['X'],
    'x-maximum': ['extent', 'XMax'],
    'xmax': ['extent', 'XMax'],
    'x-minimum': ['extent', 'XMin'],
    'xmin': ['extent', 'XMin'],
    'y-coordinate': ['Y'],
    'y': ['Y'],
    'y-maximum': ['extent', 'YMax'],
    'ymax': ['extent', 'YMax'],
    'y-minimum': ['extent', 'YMin'],
    'ymin': ['extent', 'YMin'],
    'z-coordinate': ['Z'],
    'z': ['Z'],
    'z-maximum': ['extent', 'ZMax'],
    'zmax': ['extent', 'ZMax'],
    'z-minimum': ['extent', 'ZMin'],
    'zmin': ['extent', 'ZMin'],
}
"""dict: Mapping of geometry property tag to cascade of geometry object properties."""


class FeatureMatcher(object):
    """Object for tracking features that match under certain criteria."""

    def __init__(self, dataset_path, identifier_field_names,
                 dataset_where_sql=None):
        """Initialize instance.

        Args:
            dataset_path (str): Path of the dataset.
            identifier_field_names (iter): Iterable of the field names used
                to identify a feature.
            dataset_where_sql (str): SQL where-clause for dataset
                subselection. Default is None.
        """
        self.assigned = Counter()
        self.matched = Counter(as_iters(dataset_path, identifier_field_names,
                                        dataset_where_sql=dataset_where_sql))

    def assigned_count(self, identifier_values):
        """Return the assigned count for features with the given identifier.

        Args:
            identifier_values (iter): Iterable of the attribute values that
                identify the feature(s) to query.
        """
        return self.assigned[tuple(identifier_values)]

    def increment_assigned(self, identifier_values):
        """Increment the assigned count for features wtih given identifier.

        Args:
            identifier_values (iter): Iterable of the attribute values that
                identify the feature(s) to query.
        """
        self.assigned[tuple(identifier_values)] += 1
        return self.assigned[tuple(identifier_values)]

    def is_duplicate(self, identifier_values):
        """Return True if more than one feature with given identifier.

        Args:
            identifier_values (iter): Iterable of the attribute values that
                identify the feature(s) to query.
        """
        return self.matched[tuple(identifier_values)] > 1

    def match_count(self, identifier_values):
        """Return the match count for features with given identifier.

        Args:
            identifier_values (iter): Iterable of the attribute values that
                identify the feature(s) to query.
        """
        return self.matched[tuple(identifier_values)]


def as_dicts(dataset_path, field_names=None, **kwargs):
    """Generator for dictionaries of feature attributes.

    Use ArcPy cursor token names for object IDs and geometry objects/properties.

    Args:
        dataset_path (str): Path of the dataset.
        field_names (iter): Collection of field names. Names will be the keys in the
            dictionary mapping to their values. If value is None, all attributes fields
            will be used.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        spatial_reference_item: Item from which the output geometry's spatial
            reference will be derived.

    Yields:
        dict: Mapping of feature attribute field names to values.

    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('spatial_reference_item')
    if field_names is None:
        meta = {'dataset': arcobj.dataset_metadata(dataset_path)}
        keys = {'field': tuple(key.lower() for key
                               in meta['dataset']['field_names_tokenized'])}
    else:
        keys = {'field': tuple(contain(field_names))}
    sref = arcobj.spatial_reference(kwargs['spatial_reference_item'])
    cursor = arcpy.da.SearchCursor(in_table=dataset_path, field_names=keys['field'],
                                   where_clause=kwargs['dataset_where_sql'],
                                   spatial_reference=sref)
    with cursor:
        for feature in cursor:
            yield dict(zip(cursor.fields, feature))


def as_iters(dataset_path, field_names, **kwargs):
    """Generator for iterables of feature attributes.

    Use ArcPy cursor token names for object IDs and geometry objects/properties.

    Args:
        dataset_path (str): Path of the dataset.
        field_names (iter): Collection of field names. The order of the names in the
            collection will determine where its value will fall in the yielded item.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        spatial_reference_item: Item from which the output geometry's spatial
            reference will be derived.
        iter_type: Iterable type to yield. Default is tuple.

    Yields:
        iter: Collection of attribute values.

    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('spatial_reference_item')
    kwargs.setdefault('iter_type', tuple)
    keys = {'field': tuple(contain(field_names))}
    sref = arcobj.spatial_reference(kwargs['spatial_reference_item'])
    cursor = arcpy.da.SearchCursor(in_table=dataset_path, field_names=keys['field'],
                                   where_clause=kwargs['dataset_where_sql'],
                                   spatial_reference=sref)
    with cursor:
        for feature in cursor:
            yield kwargs['iter_type'](feature)


def coordinate_node_map(dataset_path, from_id_field_name, to_id_field_name,
                        id_field_name='oid@', **kwargs):
    """Return dictionary mapping of coordinates to node-info dictionaries.

    Note:
        From & to IDs must be the same attribute type.

    Args:
        dataset_path (str): Path of the dataset.
        from_id_field_name (str): Name of the from-ID field.
        to_id_field_name (str): Name of the to-ID field.
        id_field_name (str): Name of the ID field. Default is 'oid@'.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        update_nodes (bool): Flag to indicate whether to update nodes based on feature
            geometries. Default is False.

    Returns:
        dict: Mapping of coordinate tuples to node-info dictionaries.
            {(x, y): {'node_id': <id>, 'ids': {'from': set(), 'to': set()}}}

    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('update_nodes', False)
    def _node_feature_count(node):
        """Return feature count for node from info map."""
        return len(node['ids']['from'].union(node['ids']['to']))
    def _update_coord_node_map(coord_node, node_id_metadata):
        """Return updated coordinate node info map."""
        coord_node = copy.deepcopy(coord_node)
        used_ids = {node['node_id'] for node in coord_node.values()
                    if node['node_id'] is not None}
        unused_ids = (
            _id for _id in unique_ids(arcobj.python_type(node_id_metadata['type']),
                                      node_id_metadata['length'])
            if _id not in used_ids
        )
        id_coords = {}
        for coord, node in coord_node.items():
            # Assign IDs where missing.
            if node['node_id'] is None:
                node['node_id'] = next(unused_ids)
            # If ID duplicate, re-ID node with least features.
            elif node['node_id'] in id_coords:
                other_coord = id_coords[node['node_id']]
                other_node = coord_node[other_coord]
                new_node_id = next(unused_ids)
                if _node_feature_count(node) > _node_feature_count(other_node):
                    other_node['node_id'] = new_node_id  # Does update coord_node!
                    id_coords[new_node_id] = id_coords.pop(node['node_id'])
                else:
                    node['node_id'] = new_node_id  # Does update coord_node!
            id_coords[node['node_id']] = coord
        return coord_node
    keys = {'field': (id_field_name, from_id_field_name, to_id_field_name, 'shape@')}
    coord_node = {}
    g_features = as_iters(dataset_path, keys['field'],
                          dataset_where_sql=kwargs['dataset_where_sql'])
    for feature_id, from_node_id, to_node_id, geom in g_features:
        for end, node_id, point in [('from', from_node_id, geom.firstPoint),
                                    ('to', to_node_id, geom.lastPoint)]:
            coord = (point.X, point.Y)
            if coord not in coord_node:
                # Create new coordinate-node.
                coord_node[coord] = {'node_id': node_id, 'ids': defaultdict(set)}
            coord_node[coord]['node_id'] = (
                # Assign new ID if current is missing.
                node_id if coord_node[coord]['node_id'] is None
                # Assign new ID if lower than current.
                else min(coord_node[coord]['node_id'], node_id)
            )
            # Add feature ID to end-ID set.
            coord_node[coord]['ids'][end].add(feature_id)
    if kwargs['update_nodes']:
        field_meta = {'node_id': arcobj.field_metadata(dataset_path,
                                                       from_id_field_name)}
        coord_node = _update_coord_node_map(coord_node, field_meta['node_id'])
    return coord_node


def id_map(dataset_path, id_field_names, field_names, **kwargs):
    """Return dictionary mapping of field attribute for each feature ID.

    Note:
        There is no guarantee that the ID field(s) are unique.
        Use ArcPy cursor token names for object IDs and geometry objects/
        properties.

    Args:
        dataset_path (str): Path of the dataset.
        id_field_names (iter, str): Name(s) of the ID field(s).
        field_names (iter, str): Name(s) of the field(s).
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        spatial_reference_item: Item from which the output geometry's spatial reference
            will be derived.

    Returns:
        dict: Mapping of feature ID to feature attribute(s).
    """
    field_names = tuple(contain(field_names))
    id_field_names = tuple(contain(id_field_names))
    sref = arcobj.spatial_reference(kwargs.get('spatial_reference_item'))
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
                id_field_name='oid@', **kwargs):
    """Return dictionary mapping of feature ID to from- & to-node IDs.

    From & to IDs must be the same attribute type.

    Args:
        dataset_path (str): Path of the dataset.
        from_id_field_name (str): Name of the from-ID field.
        to_id_field_name (str): Name of the to-ID field.
        id_field_name (str): Name of the ID field. Default is 'oid@'.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        field_names_as_keys (bool): Flag to indicate use of dataset's node ID field
            names as the ID field names in the map. Default is False.
        update_nodes (bool): Flag to indicate whether to update the nodes based on the
            feature geometries. Default is False.

    Returns:
        dict: Mapping of feature IDs to node-end ID dictionaries.
            `{feature_id: {'from': from_node_id, 'to': to_node_id}}`

    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('field_names_as_keys', False)
    kwargs.setdefault('update_nodes', False)
    field_meta = {'from': arcobj.field_metadata(dataset_path, from_id_field_name),
                  'to': arcobj.field_metadata(dataset_path, to_id_field_name)}
    if field_meta['from']['type'] != field_meta['to']['type']:
        raise ValueError("Fields %s & %s must be of same type.")
    key = {'id': id_field_name, 'from': from_id_field_name, 'to': to_id_field_name}
    end_key = {'from': from_id_field_name if kwargs['field_names_as_keys'] else 'from',
               'to': to_id_field_name if kwargs['field_names_as_keys'] else 'to'}
    id_nodes = defaultdict(dict)
    if kwargs['update_nodes']:
        coord_node_info = coordinate_node_map(dataset_path, from_id_field_name,
                                              to_id_field_name, id_field_name, **kwargs)
        for node in coord_node_info.values():
            for end, key in end_key.items():
                for feat_id in node['ids'][end]:
                    id_nodes[feat_id][key] = node['node_id']
    # If not updating nodes, don't need to bother with geometry/coordinates.
    else:
        g_id_nodes = as_iters(
            dataset_path, field_names=(key['id'], from_id_field_name, to_id_field_name),
            dataset_where_sql=kwargs['dataset_where_sql'],
        )
        for feat_id, from_node_id, to_node_id in g_id_nodes:
            id_nodes[feat_id][end_key['from']] = from_node_id
            id_nodes[feat_id][end_key['to']] = to_node_id
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
        use_edit_session (bool): Flag to perform updates in an edit session.
            Default is False.

    Returns:
        str: Name of the field updated.
    """
    log = leveled_logger(LOG, kwargs.get('log_level', 'info'))
    log("Start: Update attributes in %s on %s"
        " by domain code in %s, using domain %s in %s.",
        field_name, dataset_path, code_field_name, domain_name, domain_workspace_path)
    domain_meta = arcobj.domain_metadata(domain_name, domain_workspace_path)
    update_by_function(dataset_path, field_name,
                       function=domain_meta['code_description_map'].get,
                       field_as_first_arg=False,
                       arg_field_names=(code_field_name,),
                       dataset_where_sql=kwargs.get('dataset_where_sql'),
                       use_edit_session=kwargs.get('use_edit_session', False),
                       log_level=None)
    log("End: Update.")
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
        use_edit_session (bool): Flag to perform updates in an edit session.
            Default is False.

    Returns:
        str: Name of the field updated.
    """
    log = leveled_logger(LOG, kwargs.get('log_level', 'info'))
    log("Start: Update attributes in %s on %s by expression: `%s`.",
        field_name, dataset_path, expression)
    session = arcobj.Editor(
        arcobj.dataset_metadata(dataset_path)['workspace_path'],
        kwargs.get('use_edit_session', False),
        )
    dataset_view = arcobj.DatasetView(dataset_path,
                                      kwargs.get('dataset_where_sql'))
    with session, dataset_view:
        arcpy.management.CalculateField(in_table=dataset_view.name,
                                        field=field_name,
                                        expression=expression,
                                        expression_type='python_9.3')
    log("End: Update.")
    return field_name


def update_by_feature_match(dataset_path, field_name, identifier_field_names,
                            update_type, **kwargs):
    """Update attribute values by aggregating info about matching features.

    Valid update_type codes:
        'flag_value': Apply the flag_value argument value to matched features.
        'match_count': Apply the count of matched features.
        'sort_order': Apply the position of the feature sorted with matches.

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
            update_type='flag_value'.
        log_level (str): Level to log the function at. Defaults to 'info'.
        sort_field_names (iter): Iterable of field names used to sort matched
            features. Only affects output when update_type='sort_order'.
        use_edit_session (bool): Flag to perform updates in an edit session.
            Default is False.

    Returns:
        str: Name of the field updated.

    """
    identifier_field_names = tuple(identifier_field_names)
    log = leveled_logger(LOG, kwargs.get('log_level', 'info'))
    log("Start: Update attributes in %s on %s"
        " by feature-matching %s on identifiers (%s).",
        field_name, dataset_path, update_type, identifier_field_names)
    sort_field_names = tuple(kwargs.get('sort_field_names', ()))
    matcher = FeatureMatcher(dataset_path, identifier_field_names,
                             kwargs.get('dataset_where_sql'))
    if update_type not in ('flag_value', 'match_count', 'sort_order'):
        raise ValueError("Invalid update_type.")
    if update_type == 'flag_value' and 'flag_value' not in kwargs:
        raise TypeError("When update_type == 'flag_value',"
                        " flag_value is a required keyword argument.")
    cursor_kwargs = {'field_names': (identifier_field_names + sort_field_names
                                     + (field_name,)),
                     'where_clause': kwargs.get('dataset_where_sql')}
    if sort_field_names:
        cursor_kwargs['sql_clause'] = (
            None, "order by " + ", ".join(sort_field_names)
            )
    session = arcobj.Editor(
        arcobj.dataset_metadata(dataset_path)['workspace_path'],
        kwargs.get('use_edit_session', False),
        )
    cursor = arcpy.da.UpdateCursor(dataset_path, **cursor_kwargs)
    with session, cursor:
        for row in cursor:
            identifier_values = row[:len(identifier_field_names)]
            old_value = row[-1]
            if update_type == 'flag_value':
                new_value = (kwargs['flag_value']
                             if matcher.is_duplicate(identifier_values)
                             else old_value)
            elif update_type == 'match_count':
                new_value = matcher.match_count(identifier_values)
            elif update_type == 'sort_order':
                matcher.increment_assigned(identifier_values)
                new_value = matcher.assigned_count(identifier_values)
            if old_value != new_value:
                try:
                    cursor.updateRow(row[:-1] + [new_value])
                except RuntimeError:
                    LOG.error("Offending value is %s", new_value)
                    raise
    log("End: Update.")
    return field_name


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
        use_edit_session (bool): Flag to perform updates in an edit session.
            Default is False.

    Returns:
        str: Name of the field updated.

    """
    log = leveled_logger(LOG, kwargs.get('log_level', 'info'))
    log("Start: Update attributes in %s on %s by function %s.",
        field_name, dataset_path, function)
    field_names = {
        'args': tuple(kwargs.get('arg_field_names', ())),
        'kwargs': tuple(kwargs.get('kwarg_field_names', ())),
        }
    field_names['row'] = ((field_name,) + field_names['args']
                          + field_names['kwargs'])
    args_idx = len(field_names['args']) + 1
    session = arcobj.Editor(
        arcobj.dataset_metadata(dataset_path)['workspace_path'],
        kwargs.get('use_edit_session', False),
        )
    cursor = arcpy.da.UpdateCursor(dataset_path, field_names['row'],
                                   kwargs.get('dataset_where_sql'))
    with session, cursor:
        for row in cursor:
            func_args = (row[0:args_idx]
                         if kwargs.get('field_as_first_arg', True)
                         else row[1:args_idx])
            func_kwargs = dict(zip(field_names['kwargs'], row[args_idx:]))
            new_value = function(*func_args, **func_kwargs)
            if row[0] != new_value:
                try:
                    cursor.updateRow([new_value] + row[1:])
                except RuntimeError:
                    LOG.error("Offending value is %s", new_value)
                    raise RuntimeError
    log("End: Update.")
    return field_name


def update_by_mapping_function(dataset_path, field_name, function,
                               key_field_names, **kwargs):
    """Update attribute values by finding them in a function-created mapping.

    Note:
        Wraps update_by_mapping.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        function (types.FunctionType): Function to get mapping from.
        key_field_names (iter): Name of the fields whose values will be the mapping's
            keys.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        default_value: Value to return from mapping if key value on feature not
            present. Defaults to NoneType.
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        str: Name of the field updated.

    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('default_value')
    kwargs.setdefault('use_edit_session', False)
    log = leveled_logger(LOG, kwargs.get('log_level', 'info'))
    log("Start: Update attributes in %s on %s by mapping function %s with key in %s.",
        field_name, dataset_path, function, key_field_names)
    mapping = function()
    update_by_mapping(dataset_path, field_name, mapping, key_field_names,
                      dataset_where_sql=kwargs['dataset_where_sql'],
                      default_value=kwargs['default_value'],
                      use_edit_session=kwargs['use_edit_session'], log_level=None)
    log("End: Update.")
    return field_name


def update_by_mapping(dataset_path, field_name, mapping, key_field_names, **kwargs):
    """Update attribute values by finding them in a mapping.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        mapping (object): Mapping to get values from.
        key_field_names (iter): Name of the fields whose values will be the mapping's
            keys.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        default_value: Value to return from mapping if key value on feature not
            present. Defaults to None.
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        str: Name of the field updated.

    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('default_value')
    kwargs.setdefault('use_edit_session', False)
    log = leveled_logger(LOG, kwargs.get('log_level', 'info'))
    log("Start: Update attributes in %s on %s by mapping with key(s) in %s.",
        field_name, dataset_path, key_field_names)
    keys = tuple(contain(key_field_names))
    session = arcobj.Editor(arcobj.dataset_metadata(dataset_path)['workspace_path'],
                            kwargs['use_edit_session'])
    cursor = arcpy.da.UpdateCursor(dataset_path, (field_name,)+keys,
                                   kwargs['dataset_where_sql'])
    with session, cursor:
        for row in cursor:
            old_value = row[0]
            key = row[1] if len(keys) == 1 else tuple(row[1:])
            new_value = mapping.get(key, kwargs['default_value'])
            if old_value != new_value:
                try:
                    cursor.updateRow([new_value] + row[1:])
                except RuntimeError:
                    LOG.error("Offending value is %s", new_value)
                    raise RuntimeError
    log("End: Update.")
    return field_name


def update_by_geometry(dataset_path, field_name, geometry_properties, **kwargs):
    """Update attribute values by cascading through geometry properties.

    Note:
        If the spatial reference ID is not specified, the spatial reference of the
        dataset is used.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        geometry_properties (iter): Collection of the geometry property names in
            object-access order to retrieve the update value.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        spatial_reference_item: Item from which the output geometry's spatial reference
            will be derived.
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        str: Name of the field updated.

    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('spatial_reference_item')
    kwargs.setdefault('use_edit_session', False)
    log = leveled_logger(LOG, kwargs.get('log_level', 'info'))
    log(
        "Start: Update attributes in %s on %s by geometry properties %s.",
        field_name,
        dataset_path,
        geometry_properties,
    )
    meta = {
        'dataset': arcobj.dataset_metadata(dataset_path),
        'sref': arcobj.spatial_reference_metadata(kwargs['spatial_reference_item']),
    }
    session = arcobj.Editor(
        meta['dataset']['workspace_path'], kwargs['use_edit_session']
    )
    cursor = arcpy.da.UpdateCursor(
        dataset_path,
        field_names=['shape@', field_name],
        where_clause=kwargs['dataset_where_sql'],
        spatial_reference=meta['sref']['object'],
    )
    with session, cursor:
        for feat in cursor:
            val = {'old': feat[-1], 'geom': feat[0]}
            val['new'] = property_value(val['geom'], GEOMETRY_PROPERTY_TRANSFORM,
                                        *contain(geometry_properties))
            if val['old'] != val['new']:
                cursor.updateRow([val['geom'], val['new']])
    log("End: Update.")
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
        use_edit_session (bool): Flag to perform updates in an edit session.
            Default is False.

    Returns:
        str: Name of the field updated.
    """
    log = leveled_logger(LOG, kwargs.get('log_level', 'info'))
    log("Start: Update attributes in %s on %s by joined values in %s on %s.",
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
    session = arcobj.Editor(
        arcobj.dataset_metadata(dataset_path)['workspace_path'],
        kwargs.get('use_edit_session', False),
        )
    cursor = arcpy.da.UpdateCursor(dataset_path, field_names['dataset'],
                                   kwargs.get('dataset_where_sql'))
    with session, cursor:
        for row in cursor:
            new_value = join_value_map.get(tuple(row[1:]))
            if row[0] != new_value:
                cursor.updateRow([new_value] + list(row[1:]))
    log("End: Update.")
    return field_name


def update_by_node_ids(dataset_path, from_id_field_name, to_id_field_name, **kwargs):
    """Update attribute values by passing them to a function.

    Args:
        dataset_path (str): Path of the dataset.
        from_id_field_name (str): Name of the from-ID field.
        to_id_field_name (str): Name of the to-ID field.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        tuple: Names (str) of the fields updated.

    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('use_edit_session', False)
    log = leveled_logger(LOG, kwargs.get('log_level', 'info'))
    log("Start: Update attributes in %s on %s by node IDs.",
        (from_id_field_name, to_id_field_name), dataset_path)
    oid_nodes = id_node_map(dataset_path, from_id_field_name, to_id_field_name,
                            field_names_as_keys=True, update_nodes=True)
    session = arcobj.Editor(arcobj.dataset_metadata(dataset_path)['workspace_path'],
                            kwargs['use_edit_session'])
    cursor = arcpy.da.UpdateCursor(
        dataset_path, field_names=('oid@', from_id_field_name, to_id_field_name),
        where_clause=kwargs['dataset_where_sql'],
    )
    with session, cursor:
        for row in cursor:
            oid = row[0]
            new_row = (oid, oid_nodes[oid][from_id_field_name],
                       oid_nodes[oid][to_id_field_name])
            if row != new_row:
                try:
                    cursor.updateRow(new_row)
                except RuntimeError:
                    LOG.error("Offending values one of %s, %s", new_row[1], new_row[2])
                    raise RuntimeError
    log("End: Update.")
    return (from_id_field_name, to_id_field_name)


def update_by_overlay(dataset_path, field_name, overlay_dataset_path,
                      overlay_field_name, **kwargs):
    """Update attribute values by finding overlay feature value.

    Note:
        Since only one value will be selected in the overlay, operations with multiple
        overlaying features will respect the geoprocessing environment's merge rule.
        This rule generally defaults to the 'first' feature's value.

        Only one overlay flag at a time can be used (e.g. overlay_most_coincident,
        overlay_central_coincident). If multiple are set to True, the first one
        referenced in the code will be used. If no overlay flags are set, the operation
        will perform a basic intersection check, and the result will be at the whim of
        the geoprocessing environment's merge rule for the update field.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        overlay_dataset_path (str): Path of the overlay-dataset.
        overlay_field_name (str): Name of the overlay-field.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level to log the function at. Defaults is 'info'.
        overlay_central_coincident (bool): Flag to indicate overlay will use the
            centrally-coincident value. Defaults to False.
        overlay_most_coincident (bool): Flag to indicate overlay will use the most
            coincident value. Defaults to False.
        overlay_where_sql (str): SQL where-clause for overlay dataset subselection.
        replacement_value: Value to replace a present overlay-field value with.
        tolerance (float): Tolerance for coincidence, in dataset's units.
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.

    Returns:
        str: Name of the field updated.

    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('overlay_central_coincident', False)
    kwargs.setdefault('overlay_most_coincident', False)
    kwargs.setdefault('overlay_where_sql')
    kwargs.setdefault('use_edit_session', False)
    log = leveled_logger(LOG, kwargs.get('log_level', 'info'))
    log("Start: Update attributes in %s on %s by overlay values in %s on %s.",
        field_name, dataset_path, overlay_field_name, overlay_dataset_path)
    # Check flags & set details for spatial join call.
    if kwargs['overlay_most_coincident']:
        raise NotImplementedError("overlay_most_coincident not yet implemented.")
    elif kwargs['overlay_central_coincident']:
        join_kwargs = {'join_operation': 'join_one_to_many',
                       'join_type': 'keep_all',
                       'match_option': 'have_their_center_in'}
    else:
        join_kwargs = {'join_operation': 'join_one_to_many',
                       'join_type': 'keep_all',
                       'match_option': 'intersect'}
    dataset_view = arcobj.DatasetView(dataset_path, kwargs['dataset_where_sql'])
    # Create temporary copy of overlay dataset.
    temp_overlay = arcobj.TempDatasetCopy(overlay_dataset_path,
                                          kwargs['overlay_where_sql'],
                                          field_names=[overlay_field_name])
    with dataset_view, temp_overlay:
        # Avoid field name collisions with neutral name.
        temp_overlay.field_name = dataset.rename_field(
            temp_overlay.path, overlay_field_name,
            new_field_name=unique_name(overlay_field_name), log_level=None,
        )
        # Create temp output of the overlay.
        if 'tolerance' in kwargs:
            old_tolerance = arcpy.env.XYTolerance
            arcpy.env.XYTolerance = kwargs['tolerance']
        temp_output_path = unique_path('output')
        arcpy.analysis.SpatialJoin(target_features=dataset_view.name,
                                   join_features=temp_overlay.path,
                                   out_feature_class=temp_output_path,
                                   **join_kwargs)
        if 'tolerance' in kwargs:
            arcpy.env.XYTolerance = old_tolerance
    # Push overlay (or replacement) value from temp to update field.
    if 'replacement_value' in kwargs and kwargs['replacement_value'] is not None:
        function = (lambda x: kwargs['replacement_value'] if x else None)
    else:
        function = (lambda x: x)
    update_by_function(temp_output_path, field_name, function,
                       field_as_first_arg=False,
                       arg_field_names=[temp_overlay.field_name], log_level=None)
    # Update values in original dataset.
    oid_field_name = arcobj.dataset_metadata(dataset_path)['oid_field_name']
    update_by_joined_value(
        dataset_path, field_name,
        join_dataset_path=temp_output_path, join_field_name=field_name,
        on_field_pairs=[(oid_field_name, 'target_fid')],
        dataset_where_sql=kwargs['dataset_where_sql'],
        use_edit_session=kwargs['use_edit_session'], log_level=None,
    )
    dataset.delete(temp_output_path, log_level=None)
    log("End: Update.")
    return field_name


def update_by_unique_id(dataset_path, field_name, **kwargs):
    """Update attribute values by assigning a unique ID.

    Existing IDs are preserved, if unique.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        dict: Mapping of new IDs to existing old IDs.

    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('use_edit_session', True)
    log = leveled_logger(LOG, kwargs.get('log_level', 'info'))
    log("Start: Update attributes in %s on %s by assigning unique IDs.",
        field_name, dataset_path)
    meta = {'field': arcobj.field_metadata(dataset_path, field_name)}
    def _corrected_id(current_id, unique_id_pool, used_ids, ignore_nonetype=False):
        """Return corrected ID to ensure uniqueness."""
        if any((ignore_nonetype and current_id is None, current_id not in used_ids)):
            corrected_id = current_id
        else:
            corrected_id = next(unique_id_pool)
            while corrected_id in used_ids:
                corrected_id = next(unique_id_pool)
        return corrected_id
    unique_id_pool = unique_ids(data_type=arcobj.python_type(meta['field']['type']),
                                string_length=meta['field'].get('length', 16))
    oid_id = id_map(dataset_path, id_field_names='oid@', field_names=field_name)
    used_ids = set()
    new_old_id = {}
    # Ensure current IDs are unique.
    for oid, current_id in oid_id.items():
        _id = _corrected_id(current_id, unique_id_pool, used_ids, ignore_nonetype=True)
        if _id != current_id:
            new_old_id[_id] = oid_id[oid]
            oid_id[oid] = _id
        used_ids.add(_id)
    # Take care of unassigned IDs now that we know all the used IDs.
    for oid, current_id in oid_id.items():
        if current_id is None:
            _id = _corrected_id(current_id, unique_id_pool, used_ids,
                                ignore_nonetype=False)
        oid_id[oid] = _id
        used_ids.add(_id)
    update_by_mapping(dataset_path, field_name,
                      mapping=oid_id, key_field_names='oid@',
                      dataset_where_sql=kwargs.get('dataset_where_sql'),
                      use_edit_session=kwargs.get('use_edit_session', False),
                      log_level=None)
    log("End: Update.")
    return new_old_id


def update_by_value(dataset_path, field_name, value, **kwargs):
    """Update attribute values by assigning a given value.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        value (object): Static value to assign.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level to log the function at. Defaults to 'info'.
        use_edit_session (bool): Flag to perform updates in an edit session.
            Default is False.

    Returns:
        str: Name of the field updated.
    """
    log = leveled_logger(LOG, kwargs.get('log_level', 'info'))
    log("Start: Update attributes in %s on %s by given value.",
        field_name, dataset_path)
    session = arcobj.Editor(
        arcobj.dataset_metadata(dataset_path)['workspace_path'],
        kwargs.get('use_edit_session', False),
        )
    cursor = arcpy.da.UpdateCursor(dataset_path, (field_name,),
                                   kwargs.get('dataset_where_sql'))
    with session, cursor:
        for row in cursor:
            if row[0] != value:
                cursor.updateRow([value])
    log("End: Update.")
    return field_name
