"""Attribute operations."""
from collections import Counter, defaultdict
from copy import copy, deepcopy
import logging
from types import BuiltinFunctionType, BuiltinMethodType, FunctionType, MethodType

import arcpy

from arcetl.arcobj import (
    DatasetView,
    Editor,
    TempDatasetCopy,
    dataset_metadata,
    domain_metadata,
    field_metadata,
    python_type,
    same_feature,
    same_value,
    spatial_reference_metadata,
)
from arcetl import dataset
from arcetl.helpers import (
    contain,
    leveled_logger,
    property_value,
    unique_ids,
    unique_name,
    unique_path,
)


LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""

GEOMETRY_PROPERTY_TRANSFORM = {
    "x": ["X"],
    "x-coordinate": ["X"],
    "x-maximum": ["extent", "XMax"],
    "xmax": ["extent", "XMax"],
    "x-minimum": ["extent", "XMin"],
    "xmin": ["extent", "XMin"],
    "y": ["Y"],
    "y-coordinate": ["Y"],
    "y-maximum": ["extent", "YMax"],
    "ymax": ["extent", "YMax"],
    "y-minimum": ["extent", "YMin"],
    "ymin": ["extent", "YMin"],
    "z": ["Z"],
    "z-coordinate": ["Z"],
    "z-maximum": ["extent", "ZMax"],
    "zmax": ["extent", "ZMax"],
    "z-minimum": ["extent", "ZMin"],
    "zmin": ["extent", "ZMin"],
}
"""dict: Mapping of geometry property tag to cascade of geometry object properties."""

UPDATE_TYPES = ["altered", "unchanged"]
"""list: Types of attribute updates commonly associated with update counters."""


class FeatureMatcher(object):
    """Tracks features that share ID values.

    Attributes:
        assigned (collections.Counter): Running count of features that have been
            assigned each ID value. Useful for stepping through in a specific order.
        matched (collections.Counter): Count of how many features match each ID value.
    """

    def __init__(self, dataset_path, id_field_names, dataset_where_sql=None):
        """Initialize instance.

        Args:
            dataset_path (str): Path of the dataset.
            id_field_names (iter): Field names used to identify a feature.
            dataset_where_sql (str): SQL where-clause for dataset subselection. Default
                is None.
        """
        self.assigned = Counter()
        self.matched = Counter(
            as_iters(dataset_path, id_field_names, dataset_where_sql=dataset_where_sql)
        )

    def assigned_count(self, id_values):
        """Return the assigned count for features with the given identifier.

        Args:
            id_values (iter): Feature identifier values.
        """
        return self.assigned[tuple(contain(id_values))]

    def increment_assigned(self, id_values):
        """Increment assigned count for given feature ID.

        Args:
            id_values (iter): Feature identifier values.
        """
        _id = tuple(contain(id_values))
        self.assigned[_id] += 1
        return self.assigned[_id]

    def is_duplicate(self, id_values):
        """Return True if more than one feature has given ID.

        Args:
            id_values (iter): Feature identifier values.
        """
        return self.matched[tuple(contain(id_values))] > 1

    def match_count(self, id_values):
        """Return match count for features with given ID.

        Args:
            id_values (iter): Feature identifier values.
        """
        return self.matched[tuple(contain(id_values))]


def _update_coordinate_node_map(coordinate_node, node_id_field_metadata):
    """Return updated coordinate/node info map."""

    def _feature_count(node):
        """Return count of features associated with node."""
        return len(node["ids"]["from"].union(node["ids"]["to"]))

    ids = {
        "used": {
            node["node_id"]
            for node in coordinate_node.values()
            if node["node_id"] is not None
        }
    }
    ids["unused"] = (
        _id
        for _id in unique_ids(
            python_type(node_id_field_metadata["type"]),
            node_id_field_metadata["length"],
        )
        if _id not in ids["used"]
    )
    coordinate_node = deepcopy(coordinate_node)
    id_coordinates = {}
    for coordinates, node in coordinate_node.items():
        # Assign IDs where missing.
        if node["node_id"] is None:
            node["node_id"] = next(ids["unused"])
        # If ID duplicate, re-ID node with least features.
        elif node["node_id"] in id_coordinates:
            other_coord = id_coordinates[node["node_id"]]
            other_node = copy(coordinate_node[other_coord])
            new_node_id = next(ids["unused"])
            if _feature_count(node) > _feature_count(other_node):
                other_node["node_id"] = new_node_id
                coordinate_node[other_coord] = other_node
                id_coordinates[new_node_id] = id_coordinates.pop(node["node_id"])
            else:
                node["node_id"] = new_node_id
        id_coordinates[node["node_id"]] = coordinates
    return coordinate_node


def as_dicts(dataset_path, field_names=None, **kwargs):
    """Generate mappings of feature attribute name to value.

    Notes:
        Use ArcPy cursor token names for object IDs and geometry objects/properties.

    Args:
        dataset_path (str): Path of the dataset.
        field_names (iter): Collection of field names. Names will be the keys in the
            dictionary mapping to their values. If value is None, all attributes fields
            will be used.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        spatial_reference_item: Item from which the spatial reference of the output
            geometry will be derived.

    Yields:
        dict.
    """
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("spatial_reference_item")
    meta = {"spatial": spatial_reference_metadata(kwargs["spatial_reference_item"])}
    if field_names is None:
        meta["dataset"] = dataset_metadata(dataset_path)
        keys = {"output": [key for key in meta["dataset"]["field_names_tokenized"]]}
    else:
        keys = {"feature": list(contain(field_names))}
    cursor = arcpy.da.SearchCursor(
        in_table=dataset_path,
        field_names=keys["feature"],
        where_clause=kwargs["dataset_where_sql"],
        spatial_reference=meta["spatial"]["object"],
    )
    with cursor:
        for feature in cursor:
            yield dict(zip(cursor.fields, feature))


def as_iters(dataset_path, field_names, **kwargs):
    """Generate iterables of feature attribute values.

    Notes:
        Use ArcPy cursor token names for object IDs and geometry objects/properties.

    Args:
        dataset_path (str): Path of the dataset.
        field_names (iter): Collection of field names. The order of the names in the
            collection will determine where its value will fall in the generated item.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        spatial_reference_item: Item from which the spatial reference of the output
            geometry will be derived.
        iter_type: Iterable type to yield. Default is tuple.

    Yields:
        iter.
    """
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("spatial_reference_item")
    kwargs.setdefault("iter_type", tuple)
    meta = {"spatial": spatial_reference_metadata(kwargs["spatial_reference_item"])}
    keys = {"feature": list(contain(field_names))}
    cursor = arcpy.da.SearchCursor(
        in_table=dataset_path,
        field_names=keys["feature"],
        where_clause=kwargs["dataset_where_sql"],
        spatial_reference=meta["spatial"]["object"],
    )
    with cursor:
        for feature in cursor:
            yield kwargs["iter_type"](feature)


##TODO: Add spatial_reference_item kwarg?
def coordinate_node_map(
    dataset_path,
    from_id_field_name,
    to_id_field_name,
    id_field_names=("oid@",),
    **kwargs
):
    """Return mapping of coordinates to node-info dictionary.

    Notes:
        From & to IDs must be same attribute type.
        Default output format:
            {(x, y): {"node_id": <id>, "ids": {"from": set(), "to": set()}}}

    Args:
        dataset_path (str): Path of the dataset.
        from_id_field_name (str): Name of the from-ID field.
        to_id_field_name (str): Name of the to-ID field.
        id_field_names (iter, str): Name(s) of the ID field(s).
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        update_nodes (bool): Update nodes based on feature geometries if True. Default
            is False.

    Returns:
        dict.
    """
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("update_nodes", False)
    meta = {
        "from_id_field": field_metadata(dataset_path, from_id_field_name),
        "to_id_field": field_metadata(dataset_path, to_id_field_name),
    }
    if meta["from_id_field"]["type"] != meta["to_id_field"]["type"]:
        raise ValueError("From- and to-ID fields must be of same type.")

    keys = {"id": list(contain(id_field_names))}
    keys["feature"] = ["shape@", from_id_field_name, to_id_field_name] + keys["id"]
    coordinate_node = {}
    for feature in as_iters(
        dataset_path, keys["feature"], dataset_where_sql=kwargs["dataset_where_sql"]
    ):
        _id = tuple(feature[3:])
        if len(keys["id"]) == 1:
            _id = _id[0]
        geom = feature[0]
        node_id = {"from": feature[1], "to": feature[2]}
        coordinate = {
            "from": (geom.firstPoint.X, geom.firstPoint.Y),
            "to": (geom.lastPoint.X, geom.lastPoint.Y),
        }
        for end in ["from", "to"]:
            if coordinate[end] not in coordinate_node:
                # Create new coordinate-node.
                coordinate_node[coordinate[end]] = {
                    "node_id": node_id[end],
                    "ids": defaultdict(set),
                }
            # Assign new ID if current is missing.
            if coordinate_node[coordinate[end]]["node_id"] is None:
                coordinate_node[coordinate[end]]["node_id"] = node_id[end]
            # Assign lower ID if different than current.
            else:
                coordinate_node[coordinate[end]]["node_id"] = min(
                    coordinate_node[coordinate[end]]["node_id"], node_id[end]
                )
            # Add feature ID to end-ID set.
            coordinate_node[coordinate[end]]["ids"][end].add(_id)
    if kwargs["update_nodes"]:
        coordinate_node = _update_coordinate_node_map(
            coordinate_node, meta["from_id_field"]
        )
    return coordinate_node


def id_map(dataset_path, id_field_names, field_names, **kwargs):
    """Return mapping of feature ID to attribute or list of attributes.

    Notes:
        There is no guarantee that the ID value(s) are unique.
        Use ArcPy cursor token names for object IDs and geometry objects/properties.

    Args:
        dataset_path (str): Path of the dataset.
        id_field_names (iter, str): Name(s) of the ID field(s).
        field_names (iter, str): Name(s) of the field(s).
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        spatial_reference_item: Item from which the spatial reference of the output
            geometry will be derived.

    Returns:
        dict.
    """
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("spatial_reference_item")
    meta = {"spatial": spatial_reference_metadata(kwargs["spatial_reference_item"])}
    keys = {
        "id": list(contain(id_field_names)),
        "attribute": list(contain(field_names)),
    }
    cursor = arcpy.da.SearchCursor(
        in_table=dataset_path,
        field_names=keys["id"] + keys["attribute"],
        where_clause=kwargs["dataset_where_sql"],
        spatial_reference=meta["spatial"]["object"],
    )
    id_attributes = {}
    with cursor:
        for feature in cursor:
            value = {
                "id": feature[0]
                if len(keys["id"]) == 1
                else feature[: len(keys["id"])],
                "attributes": (
                    feature[len(keys["id"])]
                    if len(keys["attribute"]) == 1
                    else feature[len(keys["id"]) :]
                ),
            }
            id_attributes[value["id"]] = value["attributes"]
    return id_attributes


##TODO: Add spatial_reference_item kwarg?
def id_node_map(
    dataset_path,
    from_id_field_name,
    to_id_field_name,
    id_field_names=("oid@",),
    **kwargs
):
    """Return mapping of feature ID to from- & to-node ID dictionary.

    Notes:
        From & to IDs must be same attribute type.
        Default output format: `{feature_id: {"from": from_node_id, "to": to_node_id}}`

    Args:
        dataset_path (str): Path of the dataset.
        from_id_field_name (str): Name of from-ID field.
        to_id_field_name (str): Name of to-ID field.
        id_field_names (iter, str): Name(s) of the ID field(s).
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        field_names_as_keys (bool): Use of node ID field names as keys in the map-value
            if True; use "from" and "to" if False. Default is False.
        update_nodes (bool): Update nodes based on feature geometries if True. Default
            is False.

    Returns:
        dict: Mapping of feature IDs to node-end ID dictionaries.
            `{feature_id: {"from": from_node_id, "to": to_node_id}}`
    """
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("field_names_as_keys", False)
    kwargs.setdefault("update_nodes", False)
    keys = {
        "id": list(contain(id_field_names)),
        "node": {
            "from": from_id_field_name if kwargs["field_names_as_keys"] else "from",
            "to": to_id_field_name if kwargs["field_names_as_keys"] else "to",
        },
    }
    keys["feature"] = [from_id_field_name, to_id_field_name] + keys["id"]
    id_nodes = defaultdict(dict)
    # If updating nodes, need to gather geometry/coordinates.
    if kwargs["update_nodes"]:
        coordinate_node = coordinate_node_map(
            dataset_path, from_id_field_name, to_id_field_name, keys["id"], **kwargs
        )
        for node in coordinate_node.values():
            for end in ["from", "to"]:
                for feature_id in node["ids"][end]:
                    id_nodes[feature_id][keys["node"][end]] = node["node_id"]
    else:
        for feature in as_iters(
            dataset_path,
            field_names=keys["feature"],
            dataset_where_sql=kwargs["dataset_where_sql"],
        ):
            from_node_id, to_node_id = feature[:2]
            feature_id = feature[2:]
            if len(keys["id"]) == 1:
                feature_id = feature_id[0]
            id_nodes[feature_id][keys["node"]["from"]] = from_node_id
            id_nodes[feature_id][keys["node"]["to"]] = to_node_id
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
        use_edit_session (bool): Updates are done in an edit session if True. Default is
            False.
        log_level (str): Level to log the function at. Default is "info".

    Returns:
        collections.Counter: Counts for each update type.
    """
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("use_edit_session", False)
    log = leveled_logger(LOG, kwargs.setdefault("log_level", "info"))
    log(
        "Start: Update attributes in %s on %s by code in %s using domain %s.",
        field_name,
        dataset_path,
        code_field_name,
        domain_name,
    )
    meta = {"domain": domain_metadata(domain_name, domain_workspace_path)}
    update_count = update_by_function(
        dataset_path,
        field_name,
        function=meta["domain"]["code_description_map"].get,
        field_as_first_arg=False,
        arg_field_names=[code_field_name],
        dataset_where_sql=kwargs["dataset_where_sql"],
        use_edit_session=kwargs["use_edit_session"],
        log_level=None,
    )
    for key in UPDATE_TYPES:
        log("%s attributes %s.", update_count[key], key)
    log("End: Update.")
    return update_count


def update_by_expression(dataset_path, field_name, expression, **kwargs):
    """Update attribute values using a (single) code-expression.

    Wraps arcpy.management.CalculateField.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        expression (str): Python string expression to evaluate values from.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        use_edit_session (bool): Updates are done in an edit session if True. Default is
            False.
        log_level (str): Level to log the function at. Default is "info".

    Returns:
        str: Name of the field updated.
    """
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("use_edit_session", False)
    log = leveled_logger(LOG, kwargs.setdefault("log_level", "info"))
    log(
        "Start: Update attributes in %s on %s using expression: `%s`.",
        field_name,
        dataset_path,
        expression,
    )
    meta = {"dataset": dataset_metadata(dataset_path)}
    session = Editor(meta["dataset"]["workspace_path"], kwargs["use_edit_session"])
    dataset_view = DatasetView(dataset_path, kwargs["dataset_where_sql"])
    with session, dataset_view:
        arcpy.management.CalculateField(
            in_table=dataset_view.name,
            field=field_name,
            expression=expression,
            expression_type="python_9.3",
        )
    log("End: Update.")
    return field_name


def update_by_feature_match(
    dataset_path, field_name, id_field_names, update_type, **kwargs
):
    """Update attribute values by aggregating info about matching features.

    Note: Currently, the sort_order update type uses functionality that only works with
        datasets contained in databases.

    Valid update_type codes:
        "flag_value": Apply the flag_value argument value to matched features.
        "match_count": Apply the count of matched features.
        "sort_order": Apply the position of the feature sorted with matches.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        id_field_names (iter): Field names used to identify a feature.
        update_type (str): Code indicating what values to apply to matched features.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        flag_value: Value to apply to matched features. Only used when update_type is
            "flag_value".
        sort_field_names (iter): Iterable of field names used to sort matched features.
            Only affects output when update_type="sort_order".
        use_edit_session (bool): Updates are done in an edit session if True. Default is
            False.
        log_level (str): Level to log the function at. Default is "info".

    Returns:
        collections.Counter: Counts for each update type.
    """
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("use_edit_session", False)
    log = leveled_logger(LOG, kwargs.setdefault("log_level", "info"))
    log(
        "Start: Update attributes in %s on %s"
        + " by feature-matching %s on identifiers (%s).",
        field_name,
        dataset_path,
        update_type.replace("_", " "),
        id_field_names,
    )
    if update_type not in ["flag_value", "match_count", "sort_order"]:
        raise ValueError("Invalid update_type.")

    for _type, kwarg in {
        "flag_value": "flag_value",
        "sort_order": "sort_field_names",
    }.items():
        if update_type == _type and kwarg not in kwargs:
            raise TypeError(
                """{} is required keyword argument when update_type == "{}", .""".format(
                    _type, kwarg
                )
            )

    meta = {"dataset": dataset_metadata(dataset_path)}
    keys = {
        "id": list(contain(id_field_names)),
        "sort": list(contain(kwargs.get("sort_field_names", []))),
    }
    keys["feature"] = keys["id"] + [field_name]
    matcher = FeatureMatcher(dataset_path, keys["id"], kwargs["dataset_where_sql"])
    session = Editor(meta["dataset"]["workspace_path"], kwargs["use_edit_session"])
    cursor = arcpy.da.UpdateCursor(
        in_table=dataset_path,
        field_names=keys["feature"],
        where_clause=kwargs["dataset_where_sql"],
        sql_clause=(
            (None, "order by " + ", ".join(keys["sort"]))
            if update_type == "sort_order"
            else None
        ),
    )
    update_count = Counter()
    with session, cursor:
        for feature in cursor:
            value = {
                "id": feature[0] if len(keys["id"]) == 1 else tuple(feature[:-1]),
                "old": feature[-1],
            }
            if update_type == "flag_value":
                if matcher.is_duplicate(value["id"]):
                    value["new"] = kwargs["flag_value"]
                else:
                    value["new"] = value["old"]
            elif update_type == "match_count":
                value["new"] = matcher.match_count(value["id"])
            elif update_type == "sort_order":
                value["new"] = matcher.increment_assigned(value["id"])
            if same_value(value["old"], value["new"]):
                update_count["unchanged"] += 1
            else:
                try:
                    cursor.updateRow(feature[:-1] + [value["new"]])
                    update_count["altered"] += 1
                except RuntimeError:
                    LOG.error("Offending value is %s", value["new"])
                    raise

    for key in UPDATE_TYPES:
        log("%s attributes %s.", update_count[key], key)
    log("End: Update.")
    return update_count


def update_by_function(dataset_path, field_name, function, **kwargs):
    """Update attribute values by passing them to a function.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        function (types.FunctionType): Function to get values from.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        field_as_first_arg (bool): True if field value will be the first positional
            argument. Default is True.
        arg_field_names (iter): Field names whose values will be the positional
            arguments (not including primary field).
        kwarg_field_names (iter): Field names whose names & values will be the method
            keyword arguments.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        use_edit_session (bool): Updates are done in an edit session if True. Default is
            False.
        log_level (str): Level to log the function at. Default is "info".

    Returns:
        collections.Counter: Counts for each update type.
    """
    kwargs.setdefault("field_as_first_arg", True)
    kwargs.setdefault("arg_field_names", [])
    kwargs.setdefault("kwarg_field_names", [])
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("use_edit_session", False)
    log = leveled_logger(LOG, kwargs.setdefault("log_level", "info"))
    log(
        "Start: Update attributes in %s on %s by function %s.",
        field_name,
        dataset_path,
        function,
    )
    meta = {"dataset": dataset_metadata(dataset_path)}
    keys = {
        "args": list(contain(kwargs["arg_field_names"])),
        "kwargs": list(contain(kwargs["kwarg_field_names"])),
    }
    keys["feature"] = keys["args"] + keys["kwargs"] + [field_name]
    session = Editor(meta["dataset"]["workspace_path"], kwargs["use_edit_session"])
    cursor = arcpy.da.UpdateCursor(
        in_table=dataset_path,
        field_names=keys["feature"],
        where_clause=kwargs["dataset_where_sql"],
    )
    update_count = Counter()
    with session, cursor:
        for feature in cursor:
            value = {
                "old": feature[-1],
                "args": feature[: len(keys["args"])],
                "kwargs": dict(zip(keys["kwargs"], feature[len(keys["args"]) : -1])),
            }
            if kwargs["field_as_first_arg"]:
                value["args"] = [value["old"]] + value["args"]
            value["new"] = function(*value["args"], **value["kwargs"])
            if same_value(value["old"], value["new"]):
                update_count["unchanged"] += 1
            else:
                try:
                    cursor.updateRow(feature[:-1] + [value["new"]])
                    update_count["altered"] += 1
                except RuntimeError:
                    LOG.error("Offending value is %s", value["new"])
                    raise

    for key in UPDATE_TYPES:
        log("%s attributes %s.", update_count[key], key)
    log("End: Update.")
    return update_count


def update_by_geometry(dataset_path, field_name, geometry_properties, **kwargs):
    """Update attribute values by cascading through geometry properties.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        geometry_properties (iter): Geometry property names in object-access order to
            retrieve the update value.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        spatial_reference_item: Item from which the spatial reference for the output
            geometry property will be derived. Default is the update dataset.
        use_edit_session (bool): Updates are done in an edit session if True. If not
            not specified or None, the spatial reference of the dataset is used.
        log_level (str): Level to log the function at. Default is "info".

    Returns:
        collections.Counter: Counts for each update type.
    """
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("spatial_reference_item")
    kwargs.setdefault("use_edit_session", False)
    log = leveled_logger(LOG, kwargs.setdefault("log_level", "info"))
    log(
        "Start: Update attributes in %s on %s by geometry properties %s.",
        field_name,
        dataset_path,
        geometry_properties,
    )
    meta = {
        "dataset": dataset_metadata(dataset_path),
        "spatial": spatial_reference_metadata(kwargs["spatial_reference_item"]),
    }
    session = Editor(meta["dataset"]["workspace_path"], kwargs["use_edit_session"])
    cursor = arcpy.da.UpdateCursor(
        in_table=dataset_path,
        field_names=["shape@", field_name],
        where_clause=kwargs["dataset_where_sql"],
        spatial_reference=meta["spatial"]["object"],
    )
    update_count = Counter()
    with session, cursor:
        for feature in cursor:
            value = {"geometry": feature[0], "old": feature[-1]}
            value["new"] = property_value(
                value["geometry"],
                GEOMETRY_PROPERTY_TRANSFORM,
                *contain(geometry_properties)
            )
            if same_value(value["old"], value["new"]):
                update_count["unchanged"] += 1
            else:
                try:
                    cursor.updateRow([value["geometry"], value["new"]])
                    update_count["altered"] += 1
                except RuntimeError:
                    LOG.error("Offending value is %s", value["new"])
                    raise

    for key in UPDATE_TYPES:
        log("%s attributes %s.", update_count[key], key)
    log("End: Update.")
    return update_count


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
        on_field_pairs (iter): Field name pairs used to to determine join.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        use_edit_session (bool): Updates are done in an edit session if True. Default is
            False.
        log_level (str): Level to log the function at. Default is "info".

    Returns:
        collections.Counter: Counts for each update type.
    """
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("use_edit_session", False)
    log = leveled_logger(LOG, kwargs.setdefault("log_level", "info"))
    log(
        "Start: Update attributes in %s on %s by joined values in %s on %s.",
        field_name,
        dataset_path,
        join_field_name,
        join_dataset_path,
    )
    meta = {"dataset": dataset_metadata(dataset_path)}
    keys = {
        "dataset_id": list(pair[0] for pair in on_field_pairs),
        "join_id": list(pair[1] for pair in on_field_pairs),
    }
    keys["feature"] = keys["dataset_id"] + [field_name]
    join_value = id_map(
        join_dataset_path, id_field_names=keys["join_id"], field_names=join_field_name
    )
    session = Editor(meta["dataset"]["workspace_path"], kwargs["use_edit_session"])
    cursor = arcpy.da.UpdateCursor(
        in_table=dataset_path,
        field_names=keys["feature"],
        where_clause=kwargs["dataset_where_sql"],
    )
    update_count = Counter()
    with session, cursor:
        for feature in cursor:
            value = {
                "id": (
                    feature[0] if len(keys["dataset_id"]) == 1 else tuple(feature[:-1])
                ),
                "old": feature[-1],
            }
            value["new"] = join_value.get(value["id"])
            if same_value(value["old"], value["new"]):
                update_count["unchanged"] += 1
            else:
                try:
                    cursor.updateRow(feature[:-1] + [value["new"]])
                    update_count["altered"] += 1
                except RuntimeError:
                    LOG.error("Offending value is %s", value["new"])
                    raise

    for key in UPDATE_TYPES:
        log("%s attributes %s.", update_count[key], key)
    log("End: Update.")
    return update_count


def update_by_mapping(dataset_path, field_name, mapping, key_field_names, **kwargs):
    """Update attribute values by finding them in a mapping.

    Note: Mapping key must be a tuple if an iterable.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        mapping: Mapping to get values from.
        key_field_names (iter): Fields names whose values will comprise the mapping key.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        default_value: Value to return from mapping if key value on feature not
            present. Default is None.
        use_edit_session (bool): Updates are done in an edit session if True. Default is
            False.
        log_level (str): Level to log the function at. Default is "info".

    Returns:
        collections.Counter: Counts for each update type.
    """
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("default_value")
    kwargs.setdefault("use_edit_session", False)
    log = leveled_logger(LOG, kwargs.setdefault("log_level", "info"))
    log(
        "Start: Update attributes in %s on %s by mapping with key in %s.",
        field_name,
        dataset_path,
        key_field_names,
    )
    meta = {"dataset": dataset_metadata(dataset_path)}
    keys = {"map": list(contain(key_field_names))}
    keys["feature"] = keys["map"] + [field_name]
    if isinstance(
        mapping, (BuiltinFunctionType, BuiltinMethodType, FunctionType, MethodType)
    ):
        mapping = mapping()
    session = Editor(meta["dataset"]["workspace_path"], kwargs["use_edit_session"])
    cursor = arcpy.da.UpdateCursor(
        in_table=dataset_path,
        field_names=keys["feature"],
        where_clause=kwargs["dataset_where_sql"],
    )
    update_count = Counter()
    with session, cursor:
        for feature in cursor:
            value = {
                "map_key": feature[0] if len(keys["map"]) == 1 else tuple(feature[:-1]),
                "old": feature[-1],
            }
            value["new"] = mapping.get(value["map_key"], kwargs["default_value"])
            if same_value(value["old"], value["new"]):
                update_count["unchanged"] += 1
            else:
                try:
                    cursor.updateRow(feature[:-1] + [value["new"]])
                except RuntimeError:
                    LOG.error("Offending value is %s", value["new"])
                    raise

    for key in UPDATE_TYPES:
        log("%s attributes %s.", update_count[key], key)
    log("End: Update.")
    return update_count


def update_by_node_ids(dataset_path, from_id_field_name, to_id_field_name, **kwargs):
    """Update attribute values by passing them to a function.

    Args:
        dataset_path (str): Path of the dataset.
        from_id_field_name (str): Name of the from-ID field.
        to_id_field_name (str): Name of the to-ID field.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        use_edit_session (bool): Updates are done in an edit session if True. Default is
            False.
        log_level (str): Level to log the function at. Default is "info".

    Returns:
        collections.Counter: Counts for each update type.

    """
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("use_edit_session", False)
    log = leveled_logger(LOG, kwargs.setdefault("log_level", "info"))
    log(
        "Start: Update attributes in %s & %s on %s by node IDs.",
        from_id_field_name,
        to_id_field_name,
        dataset_path,
    )
    meta = {"dataset": dataset_metadata(dataset_path)}
    keys = {"feature": ["oid@", from_id_field_name, to_id_field_name]}
    oid_node = id_node_map(
        dataset_path, from_id_field_name, to_id_field_name, update_nodes=True
    )
    session = Editor(meta["dataset"]["workspace_path"], kwargs["use_edit_session"])
    cursor = arcpy.da.UpdateCursor(
        in_table=dataset_path,
        field_names=keys["feature"],
        where_clause=kwargs["dataset_where_sql"],
    )
    update_count = Counter()
    with session, cursor:
        for feature in cursor:
            value = {"oid": feature[0], "old_nodes": feature[1:]}
            value["new_nodes"] = [
                oid_node[value["oid"]]["from"],
                oid_node[value["oid"]]["to"],
            ]
            if same_feature(value["old_nodes"], value["new_nodes"]):
                update_count["unchanged"] += 1
            else:
                try:
                    cursor.updateRow([value["oid"]] + value["new_nodes"])
                    update_count["altered"] += 1
                except RuntimeError:
                    LOG.error("Offending value one of %s", value["new_nodes"])
                    raise

    for key in UPDATE_TYPES:
        log("%s attributes %s.", update_count[key], key)
    log("End: Update.")
    return update_count


def update_by_overlay(
    dataset_path, field_name, overlay_dataset_path, overlay_field_name, **kwargs
):
    """Update attribute values by finding overlay feature value.

    Note:
        Since only one value will be selected in the overlay, operations with multiple
        overlaying features will respect the geoprocessing environment merge rule. This
        rule generally defaults to the value of the "first" feature.

        Only one overlay flag at a time can be used (e.g. "overlay_most_coincident",
        "overlay_central_coincident"). If multiple are set to True, the first one
        referenced in the code will be used. If no overlay flags are set, the operation
        will perform a basic intersection check, and the result will be at the whim of
        the geoprocessing environment merge rule for the update field.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        overlay_dataset_path (str): Path of the overlay-dataset.
        overlay_field_name (str): Name of the overlay-field.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        overlay_central_coincident (bool): Overlay will use the centrally-coincident
            value if True. Default is False.
        overlay_most_coincident (bool): Overlay will use the most coincident value if
            True. Default is False.
        overlay_where_sql (str): SQL where-clause for overlay dataset subselection.
        replacement_value: Value to replace a present overlay-field value with.
        tolerance (float): Tolerance for coincidence, in units of the dataset.
        use_edit_session (bool): Updates are done in an edit session if True. Default is
            False.
        log_level (str): Level to log the function at. Default is "info".

    Returns:
        collections.Counter: Counts for each update type.
    """
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("overlay_central_coincident", False)
    kwargs.setdefault("overlay_most_coincident", False)
    kwargs.setdefault("overlay_where_sql")
    kwargs.setdefault("use_edit_session", False)
    log = leveled_logger(LOG, kwargs.setdefault("log_level", "info"))
    log(
        "Start: Update attributes in %s on %s by overlay values in %s on %s.",
        field_name,
        dataset_path,
        overlay_field_name,
        overlay_dataset_path,
    )
    meta = {
        "dataset": dataset_metadata(dataset_path),
        "original_tolerance": arcpy.env.XYTolerance,
    }
    join_kwargs = {"join_operation": "join_one_to_many", "join_type": "keep_all"}
    if kwargs["overlay_central_coincident"]:
        join_kwargs["match_option"] = "have_their_center_in"
    ##TODO: Implement overlay_most_coincident.
    elif kwargs["overlay_most_coincident"]:
        raise NotImplementedError("overlay_most_coincident not yet implemented.")

    # else:
    #     join_kwargs["match_option"] = "intersect"
    dataset_view = DatasetView(dataset_path, kwargs["dataset_where_sql"])
    overlay_copy = TempDatasetCopy(
        overlay_dataset_path,
        kwargs["overlay_where_sql"],
        field_names=[overlay_field_name],
    )
    with dataset_view, overlay_copy:
        # Avoid field name collisions with neutral name.
        overlay_copy.field_name = dataset.rename_field(
            overlay_copy.path,
            overlay_field_name,
            new_field_name=unique_name(overlay_field_name),
            log_level=None,
        )
        if "tolerance" in kwargs:
            arcpy.env.XYTolerance = kwargs["tolerance"]
        # Create temp output of the overlay.
        temp_output_path = unique_path("output")
        arcpy.analysis.SpatialJoin(
            target_features=dataset_view.name,
            join_features=overlay_copy.path,
            out_feature_class=temp_output_path,
            **join_kwargs
        )
        if "tolerance" in kwargs:
            arcpy.env.XYTolerance = meta["original_tolerance"]
    # Push overlay (or replacement) value from output to update field.
    if "replacement_value" in kwargs and kwargs["replacement_value"] is not None:
        function = lambda x: kwargs["replacement_value"] if x else None
    else:
        function = lambda x: x
    update_by_function(
        temp_output_path,
        field_name,
        function,
        field_as_first_arg=False,
        arg_field_names=[overlay_copy.field_name],
        log_level=None,
    )
    # Update values in original dataset.
    update_count = update_by_joined_value(
        dataset_path,
        field_name,
        join_dataset_path=temp_output_path,
        join_field_name=field_name,
        on_field_pairs=[(meta["dataset"]["oid_field_name"], "target_fid")],
        dataset_where_sql=kwargs["dataset_where_sql"],
        use_edit_session=kwargs["use_edit_session"],
        log_level=None,
    )
    dataset.delete(temp_output_path, log_level=None)
    for key in UPDATE_TYPES:
        log("%s attributes %s.", update_count[key], key)
    log("End: Update.")
    return update_count


def update_by_unique_id(dataset_path, field_name, **kwargs):
    """Update attribute values by assigning a unique ID.

    Existing IDs are preserved, if unique.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        use_edit_session (bool): Updates are done in an edit session if True. Default is
            False.
        log_level (str): Level to log the function at. Default is "info".

    Returns:
        collections.Counter: Counts for each update type.
    """
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("use_edit_session", True)
    log = leveled_logger(LOG, kwargs.setdefault("log_level", "info"))
    log(
        "Start: Update attributes in %s on %s by assigning unique IDs.",
        field_name,
        dataset_path,
    )
    meta = {
        "dataset": dataset_metadata(dataset_path),
        "field": field_metadata(dataset_path, field_name),
    }
    session = Editor(meta["dataset"]["workspace_path"], kwargs["use_edit_session"])
    cursor = arcpy.da.UpdateCursor(
        in_table=dataset_path,
        field_names=[field_name],
        where_clause=kwargs["dataset_where_sql"],
    )
    with session:
        used_ids = set()
        # First run will clear duplicate IDs & gather used IDs.
        with cursor:
            for [id_value] in cursor:
                if id_value in used_ids:
                    cursor.updateRow([None])
                else:
                    used_ids.add(id_value)
        id_pool = unique_ids(
            data_type=python_type(meta["field"]["type"]),
            string_length=meta["field"].get("length"),
        )
        # Second run will fill in missing IDs.
        update_count = Counter()
        with cursor:
            for [id_value] in cursor:
                if id_value is not None:
                    update_count["unchanged"] += 1
                else:
                    id_value = next(id_pool)
                    while id_value in used_ids:
                        id_value = next(id_pool)
                    try:
                        cursor.updateRow([id_value])
                        update_count["altered"] += 1
                        used_ids.add(id_value)
                    except RuntimeError:
                        LOG.error("Offending value is %s", id_value)
                        raise

    for key in UPDATE_TYPES:
        log("%s attributes %s.", update_count[key], key)
    log("End: Update.")
    return update_count


def update_by_value(dataset_path, field_name, value, **kwargs):
    """Update attribute values by assigning a given value.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        value (object): Static value to assign.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        use_edit_session (bool): Updates are done in an edit session if True. Default is
            False.
        log_level (str): Level to log the function at. Default is "info".

    Returns:
        collections.Counter: Counts for each update type.
    """
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("use_edit_session", True)
    log = leveled_logger(LOG, kwargs.setdefault("log_level", "info"))
    log(
        "Start: Update attributes in %s on %s by given value.", field_name, dataset_path
    )
    meta = {"dataset": dataset_metadata(dataset_path)}
    session = Editor(meta["dataset"]["workspace_path"], kwargs["use_edit_session"])
    cursor = arcpy.da.UpdateCursor(
        in_table=dataset_path,
        field_names=[field_name],
        where_clause=kwargs["dataset_where_sql"],
    )
    update_count = Counter()
    with session, cursor:
        for [old_value] in cursor:
            if same_value(old_value, value):
                update_count["unchanged"] += 1
            else:
                try:
                    cursor.updateRow([value])
                    update_count["altered"] += 1
                except RuntimeError:
                    LOG.error("Offending value is %s", value)
                    raise

    for key in UPDATE_TYPES:
        log("%s attributes %s.", update_count[key], key)
    log("End: Update.")
    return update_count
