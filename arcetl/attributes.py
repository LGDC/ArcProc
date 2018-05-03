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
    """Defining a basestring type instance for Py3+."""


LOG = logging.getLogger(__name__)
"""logging.Logger: Toolbox-level logger."""

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
    """Tracks features that share ID values.

    Attributes:
        assigned (collections.Counter): Running counts of features that have been
            assigned each ID value.
        matched (collections.Counter): Counts of how many features match each ID value.

    """

    def __init__(self, dataset_path, identifier_field_names, dataset_where_sql=None):
        """Initialize instance.

        Args:
            dataset_path (str): Path of the dataset.
            identifier_field_names (iter): Iterable of the field names used
                to identify a feature.
            dataset_where_sql (str): SQL where-clause for dataset
                subselection. Default is None.

        """
        self.assigned = Counter()
        self.matched = Counter(
            as_iters(
                dataset_path,
                identifier_field_names,
                dataset_where_sql=dataset_where_sql,
            )
        )

    def assigned_count(self, identifier_values):
        """Return the assigned count for features with the given identifier.

        Args:
            identifier_values (iter): Iterable of feature ID values.

        """
        _id = tuple(contain(identifier_values))
        return self.assigned[_id]

    def increment_assigned(self, identifier_values):
        """Increment assigned count for given feature ID.

        Args:
            identifier_values (iter): Iterable of feature ID values.

        """
        _id = tuple(contain(identifier_values))
        self.assigned[_id] += 1
        return self.assigned[_id]

    def is_duplicate(self, identifier_values):
        """Return True if more than one feature has given ID.

        Args:
            identifier_values (iter): Iterable of feature ID values.

        """
        _id = tuple(contain(identifier_values))
        return self.matched[_id] > 1

    def match_count(self, identifier_values):
        """Return match count for features with given ID.

        Args:
            identifier_values (iter): Iterable of feature ID values.

        """
        _id = tuple(contain(identifier_values))
        return self.matched[_id]


def _updated_coordinate_node_map(coordinate_node, node_id_field_metadata):
    """Return updated coordinate node info map.

    Args:
        coordinate_node
        node_id_field_metadata

    Returns:
        dict: Mapping of coordinate tuples to node-info dictionaries.
            {(x, y): {'node_id': <id>, 'ids': {'from': set(), 'to': set()}}}

    """

    def _feature_count(node):
        """Return count of features associated with node.
        Args:
            node (dict): Info dictionary for node.

        Returns:
            int: Count of features.

        """
        return len(node['ids']['from'].union(node['ids']['to']))

    ids = {
        'used': {
            node['node_id']
            for node in coordinate_node.values()
            if node['node_id'] is not None
        }
    }
    ids['unused'] = (
        _id
        for _id in unique_ids(
            arcobj.python_type(node_id_field_metadata['type']),
            node_id_field_metadata['length'],
        )
        if _id not in ids['used']
    )
    updated_coord_node = {}
    id_coords = {}
    for coord in coordinate_node:
        node = copy.copy(coordinate_node(coord))
        # Assign IDs where missing.
        if node['node_id'] is None:
            node['node_id'] = next(ids['unused'])
        # If ID duplicate, re-ID node with least features.
        elif node['node_id'] in id_coords:
            other_coord = id_coords[node['node_id']]
            other_node = copy.copy(updated_coord_node[other_coord])
            new_node_id = next(ids['unused'])
            if _feature_count(node) > _feature_count(other_node):
                other_node['node_id'] = new_node_id
                updated_coord_node[other_coord] = other_node
                id_coords[new_node_id] = id_coords.pop(node['node_id'])
            else:
                node['node_id'] = new_node_id
                updated_coord_node[coord] = node
        id_coords[node['node_id']] = coord
    return updated_coord_node


def as_dicts(dataset_path, field_names=None, **kwargs):
    """Generate feature attribute dictionaries.

    Use ArcPy cursor token names for object IDs and geometry objects/properties.

    Args:
        dataset_path (str): Path of the dataset.
        field_names (iter): Collection of field names. Names will be the keys in the
            dictionary mapping to their values. If value is None, all attributes fields
            will be used.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        spatial_reference_item: Item from which the output geometry's spatial reference
            will be derived.

    Yields:
        dict: Mapping of feature attribute field names to values.

    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('spatial_reference_item')
    meta = {'sref': arcobj.spatial_reference_metadata(kwargs['spatial_reference_item'])}
    if field_names is None:
        meta['dataset'] = arcobj.dataset_metadata(dataset_path)
        keys = {
            'field': [key.lower() for key in meta['dataset']['field_names_tokenized']]
        }
    else:
        keys = {'field': list(contain(field_names))}
    cursor = arcpy.da.SearchCursor(
        in_table=dataset_path,
        field_names=keys['field'],
        where_clause=kwargs['dataset_where_sql'],
        spatial_reference=meta['sref']['object'],
    )
    with cursor:
        for feature in cursor:
            yield dict(zip(cursor.fields, feature))


def as_iters(dataset_path, field_names, **kwargs):
    """Generate feature attribute iterables.

    Use ArcPy cursor token names for object IDs and geometry objects/properties.

    Args:
        dataset_path (str): Path of the dataset.
        field_names (iter): Collection of field names. The order of the names in the
            collection will determine where its value will fall in the yielded item.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        spatial_reference_item: Item from which the output geometry's spatial reference
            will be derived.
        iter_type: Iterable type to yield. Default is tuple.

    Yields:
        iter: Collection of attribute values.

    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('spatial_reference_item')
    kwargs.setdefault('iter_type', tuple)
    meta = {'sref': arcobj.spatial_reference_metadata(kwargs['spatial_reference_item'])}
    keys = {'field': list(contain(field_names))}
    cursor = arcpy.da.SearchCursor(
        in_table=dataset_path,
        field_names=keys['field'],
        where_clause=kwargs['dataset_where_sql'],
        spatial_reference=meta['sref']['object'],
    )
    with cursor:
        for feature in cursor:
            yield kwargs['iter_type'](feature)


def coordinate_node_map(
    dataset_path, from_id_field_name, to_id_field_name, id_field_name='oid@', **kwargs
):
    """Return mapping of coordinates to node-info dictionaries.

    Note:
        From & to IDs must be same attribute type.

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
    meta = {
        'from_id_field': arcobj.field_metadata(dataset_path, from_id_field_name),
        'to_id_field': arcobj.field_metadata(dataset_path, to_id_field_name),
    }
    if meta['from_id_field']['type'] != meta['to_id_field']['type']:
        raise ValueError("Fields %s & %s must be of same type.")

    keys = {'field': (id_field_name, from_id_field_name, to_id_field_name, 'shape@')}
    coord_node = {}
    g_features = as_iters(
        dataset_path, keys['field'], dataset_where_sql=kwargs['dataset_where_sql']
    )
    for feat in g_features:
        _id = {'feature': feat[0], 'node': {'from': feat[1], 'to': feat[2]}}
        coord = {
            'from': (feat[-1].firstPoint.X, feat[-1].firstPoint.Y),
            'to': (feat[-1].lastPoint.X, feat[-1].lastPoint.Y),
        }
        for end in ['from', 'to']:
            if coord[end] not in coord_node:
                # Create new coordinate-node.
                coord_node[coord[end]] = {
                    'node_id': _id['node'][end], 'ids': defaultdict(set)
                }

            # Assign new ID if current is missing.
            if coord_node[coord[end]]['node_id'] is None:
                coord_node[coord[end]]['node_id'] = _id['node'][end]
            # Assign lower ID if different than current.
            else:
                coord_node[coord[end]]['node_id'] = min(
                    coord_node[coord[end]]['node_id'], _id['node'][end]
                )
            # Add feature ID to end-ID set.
            coord_node[coord[end]]['ids'][end].add(_id['feature'])
    if kwargs['update_nodes']:
        coord_node = _updated_coordinate_node_map(coord_node, meta['from_id_field'])
    return coord_node


def id_map(dataset_path, id_field_names, field_names, **kwargs):
    """Return mapping of feature ID to field attribute(s).

    Note:
        There is no guarantee that the ID field(s) are unique.
        Use ArcPy cursor token names for object IDs and geometry objects/properties.

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
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('spatial_reference_item')
    meta = {'sref': arcobj.spatial_reference_metadata(kwargs['spatial_reference_item'])}
    keys = {'ids': list(contain(id_field_names)), 'attrs': list(contain(field_names))}
    cursor = arcpy.da.SearchCursor(
        in_table=dataset_path,
        field_names=keys['ids'] + keys['attrs'],
        where_clause=kwargs['dataset_where_sql'],
        spatial_reference=meta['sref']['object'],
    )
    id_attrs = {}
    with cursor:
        for feat in cursor:
            _id = feat[:len(keys['ids'])]
            attrs = feat[len(keys['ids']):]
            if len(keys['ids']) == 1:
                _id = _id[0]
            if len(keys['attrs']) == 1:
                attrs = attrs[0]
            id_attrs[_id] = attrs
    return id_attrs


def id_node_map(
    dataset_path, from_id_field_name, to_id_field_name, id_field_name='oid@', **kwargs
):
    """Return mapping of feature ID to from- & to-node IDs.

    From & to IDs must be same attribute type.

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
    meta = {
        'from_id_field': arcobj.field_metadata(dataset_path, from_id_field_name),
        'to_id_field': arcobj.field_metadata(dataset_path, to_id_field_name),
    }
    if meta['from_id_field']['type'] != meta['to_id_field']['type']:
        raise ValueError("Fields %s & %s must be of same type.")

    keys = {
        'fields': [id_field_name, from_id_field_name, to_id_field_name],
        'from': from_id_field_name if kwargs['field_names_as_keys'] else 'from',
        'to': to_id_field_name if kwargs['field_names_as_keys'] else 'to',
    }
    id_nodes = defaultdict(dict)
    if kwargs['update_nodes']:
        coord_node = coordinate_node_map(
            dataset_path, from_id_field_name, to_id_field_name, id_field_name, **kwargs
        )
        for node in coord_node.values():
            for end in ['from', 'to']:
                for feat_id in node['ids'][end]:
                    id_nodes[feat_id][keys[end]] = node['node_id']
    # If not updating nodes, don't need to bother with geometry/coordinates.
    else:
        g_id_node_ids = as_iters(
            dataset_path,
            field_names=keys['fields'],
            dataset_where_sql=kwargs['dataset_where_sql'],
        )
        for feat_id, from_node_id, to_node_id in g_id_node_ids:
            id_nodes[feat_id][keys['from']] = from_node_id
            id_nodes[feat_id][keys['to']] = to_node_id
    return id_nodes


def update_by_domain_code(
    dataset_path,
    field_name,
    code_field_name,
    domain_name,
    domain_workspace_path,
    **kwargs
):
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
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        str: Name of the field updated.

    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('use_edit_session', False)
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    log(
        "Start: Update attributes in %s on %s by code in %s using domain %s.",
        field_name,
        dataset_path,
        code_field_name,
        domain_name,
    )
    meta = {'domain': arcobj.domain_metadata(domain_name, domain_workspace_path)}
    update_by_function(
        dataset_path,
        field_name,
        function=meta['domain']['code_description_map'].get,
        field_as_first_arg=False,
        arg_field_names=[code_field_name],
        dataset_where_sql=kwargs['dataset_where_sql'],
        use_edit_session=kwargs['use_edit_session'],
        log_level=None,
    )
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
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        str: Name of the field updated.

    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('use_edit_session', False)
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    log(
        "Start: Update attributes in %s on %s using expression: `%s`.",
        field_name,
        dataset_path,
        expression,
    )
    meta = {'dataset': arcobj.dataset_metadata(dataset_path)}
    session = arcobj.Editor(
        meta['dataset']['workspace_path'], kwargs['use_edit_session']
    )
    dataset_view = arcobj.DatasetView(dataset_path, kwargs['dataset_where_sql'])
    with session, dataset_view:
        arcpy.management.CalculateField(
            in_table=dataset_view.name,
            field=field_name,
            expression=expression,
            expression_type='python_9.3',
        )
    log("End: Update.")
    return field_name


def update_by_feature_match(
    dataset_path, field_name, identifier_field_names, update_type, **kwargs
):
    """Update attribute values by aggregating info about matching features.

    Valid update_type codes:
        'flag_value': Apply the flag_value argument value to matched features.
        'match_count': Apply the count of matched features.
        'sort_order': Apply the position of the feature sorted with matches.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        identifier_field_names (iter): Iterable of the field names used to identify a
            feature.
        update_type (str): Code indicating what values to apply to matched features.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        flag_value: Value to apply to matched features. Only used when update_type=
            'flag_value'.
        sort_field_names (iter): Iterable of field names used to sort matched features.
            Only affects output when update_type='sort_order'.
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        str: Name of the field updated.

    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('use_edit_session', False)
    if update_type not in ('flag_value', 'match_count', 'sort_order'):
        raise ValueError("Invalid update_type.")

    for _type, kwarg in [
        ('flag_value', 'flag_value'), ('sort_order', 'sort_field_names')
    ]:
        if update_type == _type and kwarg not in kwargs:
            raise TypeError(
                "When update_type == '{}', {} is a required keyword argument.".format(
                    _type, kwarg
                )
            )

    meta = {'dataset': arcobj.dataset_metadata(dataset_path)}
    keys = {'id': list(contain(identifier_field_names))}
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    log(
        "Start: Update attributes in %s on %s" +
        " by feature-matching %s on identifiers (%s).",
        field_name,
        dataset_path,
        update_type.replace('_', ' '),
        keys['id'],
    )
    keys['sort'] = list(contain(kwargs.get('sort_field_names', [])))
    session = arcobj.Editor(
        meta['dataset']['workspace_path'], kwargs['use_edit_session']
    )
    keys['cursor'] = keys['sort'] + keys['id'] + [field_name]
    cursor = arcpy.da.UpdateCursor(
        in_table=dataset_path,
        field_names=keys['cursor'],
        where_clause=kwargs['dataset_where_sql'],
    )
    with session, cursor:
        feats = sorted(feat for feat in cursor)
    matcher = FeatureMatcher(dataset_path, keys['id'], kwargs['dataset_where_sql'])
    for feat in feats:
        val = {
            'old': feat[-1],
            'id': feat[len(keys['sort']):len(keys['sort']) + len(keys['id'])],
        }
        if update_type == 'flag_value':
            if not matcher.is_duplicate(val['id']):
                continue

            val['new'] = kwargs['flag_value']
        elif update_type == 'match_count':
            val['new'] = matcher.match_count(val['id'])
        elif update_type == 'sort_order':
            matcher.increment_assigned(val['id'])
            val['new'] = matcher.assigned_count(val['id'])
        if val['old'] != val['new']:
            try:
                cursor.updateRow(feat[:-1] + [val['new']])
            except RuntimeError:
                LOG.error("Offending value is %s", val['new'])
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
        field_as_first_arg (bool): Flag to indicate the field value will be the first
            argument for the method. Default is True.
        arg_field_names (iter): Iterable of the field names whose values will be the
            method arguments (not including the primary field).
        kwarg_field_names (iter): Iterable of the field names whose names & values will
            be the method keyword arguments.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        str: Name of the field updated.

    """
    kwargs.setdefault('field_as_first_arg', True)
    kwargs.setdefault('arg_field_names', [])
    kwargs.setdefault('kwarg_field_names', [])
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('use_edit_session', False)
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    log(
        "Start: Update attributes in %s on %s by function %s.",
        field_name,
        dataset_path,
        function,
    )
    meta = {'dataset': arcobj.dataset_metadata(dataset_path)}
    keys = {
        'args': list(contain(kwargs['arg_field_names'])),
        'kwargs': list(contain(kwargs['kwarg_field_names'])),
    }
    keys['cursor'] = keys['args'] + keys['kwargs'] + [field_name]
    session = arcobj.Editor(
        meta['dataset']['workspace_path'], kwargs['use_edit_session']
    )
    cursor = arcpy.da.UpdateCursor(
        in_table=dataset_path,
        field_names=keys['cursor'],
        where_clause=kwargs['dataset_where_sql'],
    )
    with session, cursor:
        for feat in cursor:
            val = {
                'old': feat[-1],
                'args': feat[:len(keys['args'])],
                'kwargs': dict(zip(keys['kwargs'], feat[len(keys['args']):-1])),
            }
            if kwargs['field_as_first_arg']:
                val['args'] = [val['old']] + val['args']
            val['new'] = function(*val['args'], ** val['kwargs'])
            if val['old'] != val['new']:
                try:
                    cursor.updateRow(feat[:-1] + [val['new']])
                except RuntimeError:
                    LOG.error("Offending value is %s", val['new'])
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
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
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
        in_table=dataset_path,
        field_names=['shape@', field_name],
        where_clause=kwargs['dataset_where_sql'],
        spatial_reference=meta['sref']['object'],
    )
    with session, cursor:
        for feat in cursor:
            val = {'old': feat[-1], 'geom': feat[0]}
            val['new'] = property_value(
                val['geom'], GEOMETRY_PROPERTY_TRANSFORM, *contain(geometry_properties)
            )
            if val['old'] != val['new']:
                try:
                    cursor.updateRow(feat[:-1] + [val['new']])
                except RuntimeError:
                    LOG.error("Offending value is %s", val['new'])
                    raise RuntimeError

    log("End: Update.")
    return field_name


def update_by_joined_value(
    dataset_path,
    field_name,
    join_dataset_path,
    join_field_name,
    on_field_pairs,
    **kwargs
):
    """Update attribute values by referencing a joinable field.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        join_dataset_path (str): Path of the join-dataset.
        join_field_name (str): Name of the join-field.
        on_field_pairs (iter): Iterable of the field name pairs to used to determine
            join.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        str: Name of the field updated.

    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('use_edit_session', False)
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    log(
        "Start: Update attributes in %s on %s by joined values in %s on %s.",
        field_name,
        dataset_path,
        join_field_name,
        join_dataset_path,
    )
    meta = {'dataset': arcobj.dataset_metadata(dataset_path)}
    keys = {
        'dataset_id': list(pair[0] for pair in on_field_pairs),
        'join_id': list(pair[1] for pair in on_field_pairs),
    }
    keys['cursor'] = keys['dataset_id'] + [field_name]
    join_value = id_map(
        join_dataset_path, id_field_names=keys['join_id'], field_names=join_field_name
    )
    session = arcobj.Editor(
        meta['dataset']['workspace_path'], kwargs['use_edit_session']
    )
    cursor = arcpy.da.UpdateCursor(
        in_table=dataset_path,
        field_names=keys['cursor'],
        where_clause=kwargs['dataset_where_sql'],
    )
    with session, cursor:
        for feat in cursor:
            val = {'id': feat[:-1], 'old': feat[-1]}
            val['new'] = join_value.get(tuple(val['id']))
            if val['old'] != val['new']:
                try:
                    cursor.updateRow(feat[:-1] + [val['new']])
                except RuntimeError:
                    LOG.error("Offending value is %s", val['new'])
                    raise RuntimeError

    log("End: Update.")
    return field_name


def update_by_mapping_function(
    dataset_path, field_name, function, key_field_names, **kwargs
):
    """Update attribute values by finding them in a function-created mapping.

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
            present. Default is None.
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        str: Name of the field updated.

    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('default_value')
    kwargs.setdefault('use_edit_session', False)
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    log(
        "Start: Update attributes in %s on %s by mapping function %s with key in %s.",
        field_name,
        dataset_path,
        function,
        key_field_names,
    )
    mapping = function()
    update_by_mapping(
        dataset_path,
        field_name,
        mapping,
        key_field_names,
        dataset_where_sql=kwargs['dataset_where_sql'],
        default_value=kwargs['default_value'],
        use_edit_session=kwargs['use_edit_session'],
        log_level=None,
    )
    log("End: Update.")
    return field_name


def update_by_mapping(dataset_path, field_name, mapping, key_field_names, **kwargs):
    """Update attribute values by finding them in a mapping.

    Note: If mapping's keys are an iterable, it must be a tuple.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        mapping: Mapping to get values from.
        key_field_names (iter): Name of the fields whose values will be the mapping's
            keys.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        default_value: Value to return from mapping if key value on feature not
            present. Default is None.
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        str: Name of the field updated.

    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('default_value')
    kwargs.setdefault('use_edit_session', False)
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    log(
        "Start: Update attributes in %s on %s by mapping with key(s) in %s.",
        field_name,
        dataset_path,
        key_field_names,
    )
    meta = {'dataset': arcobj.dataset_metadata(dataset_path)}
    keys = {'map': list(contain(key_field_names))}
    keys['cursor'] = keys['map'] + [field_name]
    session = arcobj.Editor(
        meta['dataset']['workspace_path'], kwargs['use_edit_session']
    )
    cursor = arcpy.da.UpdateCursor(
        in_table=dataset_path,
        field_names=keys['cursor'],
        where_clause=kwargs['dataset_where_sql'],
    )
    with session, cursor:
        for feat in cursor:
            val = {
                'old': feat[-1],
                'map_key': feat[0] if len(keys['map']) == 1 else tuple(feat[:-1]),
            }
            val['new'] = mapping.get(val['map_key'], kwargs['default_value'])
            if val['old'] != val['new']:
                try:
                    cursor.updateRow(feat[:-1] + [val['new']])
                except RuntimeError:
                    LOG.error("Offending value is %s", val['new'])
                    raise RuntimeError

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
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        tuple: Names (str) of the fields updated.

    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('use_edit_session', False)
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    log(
        "Start: Update attributes in %s & %s on %s by node IDs.",
        from_id_field_name,
        to_id_field_name,
        dataset_path,
    )
    meta = {'dataset': arcobj.dataset_metadata(dataset_path)}
    keys = {'cursor': ['oid@', from_id_field_name, to_id_field_name]}
    oid_node = id_node_map(
        dataset_path, from_id_field_name, to_id_field_name, update_nodes=True
    )
    session = arcobj.Editor(
        meta['dataset']['workspace_path'], kwargs['use_edit_session']
    )
    cursor = arcpy.da.UpdateCursor(
        in_table=dataset_path,
        field_names=keys['cursor'],
        where_clause=kwargs['dataset_where_sql'],
    )
    with session, cursor:
        for feat in cursor:
            val = {'id': feat[0], 'olds': feat[1:]}
            val['news'] = [oid_node[val['id']]['from'], oid_node[val['id']]['to']]
            if val['olds'] != val['news']:
                try:
                    cursor.updateRow(feat[:1] + val['news'])
                except RuntimeError:
                    LOG.error("Offending value one of %s", val['news'])
                    raise RuntimeError

    log("End: Update.")
    return (from_id_field_name, to_id_field_name)


def update_by_overlay(
    dataset_path, field_name, overlay_dataset_path, overlay_field_name, **kwargs
):
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
        overlay_central_coincident (bool): Flag to indicate overlay will use the
            centrally-coincident value. Default is False.
        overlay_most_coincident (bool): Flag to indicate overlay will use the most
            coincident value. Default is False.
        overlay_where_sql (str): SQL where-clause for overlay dataset subselection.
        replacement_value: Value to replace a present overlay-field value with.
        tolerance (float): Tolerance for coincidence, in dataset's units.
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        str: Name of the field updated.

    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('overlay_central_coincident', False)
    kwargs.setdefault('overlay_most_coincident', False)
    kwargs.setdefault('overlay_where_sql')
    kwargs.setdefault('use_edit_session', False)
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    log(
        "Start: Update attributes in %s on %s by overlay values in %s on %s.",
        field_name,
        dataset_path,
        overlay_field_name,
        overlay_dataset_path,
    )
    meta = {'dataset': arcobj.dataset_metadata(dataset_path)}
    join_kwargs = {'join_operation': 'join_one_to_many', 'join_type': 'keep_all'}
    if kwargs['overlay_central_coincident']:
        join_kwargs['match_option'] = 'have_their_center_in'
    elif kwargs['overlay_most_coincident']:
        raise NotImplementedError("overlay_most_coincident not yet implemented.")

    else:
        join_kwargs['match_option'] = 'intersect'
    temp = {
        'view': arcobj.DatasetView(dataset_path, kwargs['dataset_where_sql']),
        'overlay': arcobj.TempDatasetCopy(
            overlay_dataset_path,
            kwargs['overlay_where_sql'],
            field_names=[overlay_field_name],
        ),
    }
    with temp['view'], temp['overlay']:
        # Avoid field name collisions with neutral name.
        temp['overlay'].field_name = dataset.rename_field(
            temp['overlay'].path,
            overlay_field_name,
            new_field_name=unique_name(overlay_field_name),
            log_level=None,
        )
        if 'tolerance' in kwargs:
            old_tolerance = arcpy.env.XYTolerance
            arcpy.env.XYTolerance = kwargs['tolerance']
        # Create temp output of the overlay.
        temp['output_path'] = unique_path('output')
        arcpy.analysis.SpatialJoin(
            target_features=temp['view'].name,
            join_features=temp['overlay'].path,
            out_feature_class=temp['output_path'],
            **join_kwargs
        )
        if 'tolerance' in kwargs:
            arcpy.env.XYTolerance = old_tolerance
    # Push overlay (or replacement) value from output to update field.
    if 'replacement_value' in kwargs and kwargs['replacement_value'] is not None:
        function = (lambda x: kwargs['replacement_value'] if x else None)
    else:
        function = (lambda x: x)
    update_by_function(
        temp['output_path'],
        field_name,
        function,
        field_as_first_arg=False,
        arg_field_names=[temp['overlay'].field_name],
        log_level=None,
    )
    # Update values in original dataset.
    update_by_joined_value(
        dataset_path,
        field_name,
        join_dataset_path=temp['output_path'],
        join_field_name=field_name,
        on_field_pairs=[(meta['dataset']['oid_field_name'], 'target_fid')],
        dataset_where_sql=kwargs['dataset_where_sql'],
        use_edit_session=kwargs['use_edit_session'],
        log_level=None,
    )
    dataset.delete(temp['output_path'], log_level=None)
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
        str: Name of the field updated.

    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('use_edit_session', True)
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    log(
        "Start: Update attributes in %s on %s by assigning unique IDs.",
        field_name,
        dataset_path,
    )
    meta = {
        'dataset': arcobj.dataset_metadata(dataset_path),
        'field': arcobj.field_metadata(dataset_path, field_name),
    }
    session = arcobj.Editor(
        meta['dataset']['workspace_path'], kwargs['use_edit_session']
    )
    cursor = arcpy.da.UpdateCursor(
        in_table=dataset_path,
        field_names=[field_name],
        where_clause=kwargs['dataset_where_sql'],
    )
    with session:
        used_ids = set()
        # First run will clear duplicate IDs & gather used IDs.
        with cursor:
            for id_val, in cursor:
                if id_val in used_ids:
                    cursor.updateRow([None])
                else:
                    used_ids.add(id_val)
        id_pool = unique_ids(
            data_type=arcobj.python_type(meta['field']['type']),
            string_length=meta['field'].get('length'),
        )
        # Second run will fill in missing IDs.
        with cursor:
            for id_val, in cursor:
                if id_val is None:
                    id_val = next(id_pool)
                    while id_val in used_ids:
                        id_val = next(id_pool)
                    try:
                        cursor.updateRow([id_val])
                    except RuntimeError:
                        LOG.error("Offending value is %s", id_val)
                        raise RuntimeError

                    used_ids.add(id_val)
    log("End: Update.")
    return field_name


def update_by_value(dataset_path, field_name, value, **kwargs):
    """Update attribute values by assigning a given value.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        value (object): Static value to assign.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        str: Name of the field updated.
    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('use_edit_session', True)
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    log(
        "Start: Update attributes in %s on %s by given value.", field_name, dataset_path
    )
    meta = {'dataset': arcobj.dataset_metadata(dataset_path)}
    session = arcobj.Editor(
        meta['dataset']['workspace_path'], kwargs['use_edit_session']
    )
    cursor = arcpy.da.UpdateCursor(
        in_table=dataset_path,
        field_names=[field_name],
        where_clause=kwargs['dataset_where_sql'],
    )
    with session, cursor:
        for old_val, in cursor:
            if old_val != value:
                try:
                    cursor.updateRow([value])
                except RuntimeError:
                    LOG.error("Offending value is %s", value)
                    raise RuntimeError

    log("End: Update.")
    return field_name
