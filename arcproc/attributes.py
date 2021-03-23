"""Attribute operations."""
from collections import Counter, defaultdict
from copy import copy, deepcopy
from functools import partial
import logging
from operator import itemgetter
import sys
from types import BuiltinFunctionType, BuiltinMethodType, FunctionType, MethodType

from xsorted import xsorted

import arcpy

from arcproc.arcobj import (
    DatasetView,
    Editor,
    TempDatasetCopy,
    dataset_metadata,
    domain_metadata,
    field_metadata,
    python_type,
    spatial_reference,
)
from arcproc import dataset
from arcproc.helpers import (
    contain,
    log_entity_states,
    property_value,
    same_feature,
    same_value,
    unique_ids,
    unique_name,
    unique_path,
)


LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""

EXEC_TYPES = [BuiltinFunctionType, BuiltinMethodType, FunctionType, MethodType, partial]
"""list: Executable object types. Useful for determining if an object can execute."""
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

arcpy.SetLogHistory(False)


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

    def assigned_count(self, feature_id):
        """Return the assigned count for features with the given identifier.

        Args:
            feature_id (iter): Feature identifier values.
        """
        return self.assigned[tuple(contain(feature_id))]

    def increment_assigned(self, feature_id):
        """Increment assigned count for given feature ID.

        Args:
            feature_id (iter): Feature identifier values.
        """
        _id = tuple(contain(feature_id))
        self.assigned[_id] += 1
        return self.assigned[_id]

    def is_duplicate(self, feature_id):
        """Return True if more than one feature has given ID.

        Args:
            feature_id (iter): Feature identifier values.
        """
        return self.matched[tuple(contain(feature_id))] > 1

    def match_count(self, feature_id):
        """Return match count for features with given ID.

        Args:
            feature_id (iter): Feature identifier values.
        """
        return self.matched[tuple(contain(feature_id))]


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
    if field_names is None:
        keys = {"feature": dataset_metadata(dataset_path)["field_names_tokenized"]}
    else:
        keys = {"feature": list(contain(field_names))}
    cursor = arcpy.da.SearchCursor(
        in_table=dataset_path,
        field_names=keys["feature"],
        where_clause=kwargs["dataset_where_sql"],
        spatial_reference=spatial_reference(kwargs["spatial_reference_item"]),
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
    keys = {"feature": list(contain(field_names))}
    cursor = arcpy.da.SearchCursor(
        in_table=dataset_path,
        field_names=keys["feature"],
        where_clause=kwargs["dataset_where_sql"],
        spatial_reference=spatial_reference(kwargs["spatial_reference_item"]),
    )
    with cursor:
        for feature in cursor:
            yield kwargs["iter_type"](feature)


def as_values(dataset_path, field_names, **kwargs):
    """Generate feature attribute values.

    Values of all given field names will be yielded as they are uncovered.

    Notes:
        Use ArcPy cursor token names for object IDs and geometry objects/properties.

    Args:
        dataset_path (str): Path of the dataset.
        field_names (iter): Collection of field names.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        spatial_reference_item: Item from which the spatial reference of the output
            geometry will be derived.

    Yields:
        object.
    """
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("spatial_reference_item")
    keys = {"feature": list(contain(field_names))}
    cursor = arcpy.da.SearchCursor(
        in_table=dataset_path,
        field_names=keys["feature"],
        where_clause=kwargs["dataset_where_sql"],
        spatial_reference=spatial_reference(kwargs["spatial_reference_item"]),
    )
    with cursor:
        for feature in cursor:
            for value in feature:
                yield value


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
    keys["feature"] = ["SHAPE@", from_id_field_name, to_id_field_name] + keys["id"]
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


def id_values(dataset_path, id_field_names, field_names, **kwargs):
    """Generate tuple with feature ID & attributes value or tuple of values.

    If there is only one field name listed to retrieve values from, value will be
        returned as itself, rather than in a value-tuple.

    Args:
        dataset_path (str): Path of dataset.
        id_field_names (iter): Ordered collection of fields used to identify a feature.
        field_names (iter): Ordered collection of fields to attribute to feature.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        sort_by_id (bool): Sort generated tuples in ID order. Default is False.
        use_external_sort (bool): Use external sort if sort_by_id=True. Helpful for
            memory management with large datasets. Default is False.
        dataset_where_sql (str): SQL where-clause for dataset subselection. Default is
            None.
        spatial_reference_item: Item from which the spatial reference of the output
            geometry will be derived. Only relevant if geometry or geometry property are
            attributed. Default is None (uses spatial reference of the dataset).

    Yields:
        tuple
    """
    keys = {"id": list(contain(id_field_names)), "val": list(contain(field_names))}
    pivot = len(keys["id"])
    feats = as_iters(
        dataset_path=dataset_path,
        field_names=keys["id"] + keys["val"],
        dataset_where_sql=kwargs.get("dataset_where_sql"),
        spatial_reference_item=kwargs.get("spatial_reference_item"),
    )
    if kwargs.get("sort_by_id", False):
        sort_func = xsorted if kwargs.get("use_external_sort", False) else sorted
        feats = (feat for feat in sort_func(feats, key=(lambda x: x[:pivot])))
    for feat in feats:
        feat_id = tuple(feat[:pivot]) if len(keys["val"]) > 1 else feat[0]
        feat_vals = tuple(feat[pivot:]) if len(keys["val"]) > 1 else feat[pivot]
        yield (feat_id, feat_vals)


def id_values_map(dataset_path, id_field_names, field_names, **kwargs):
    """Return mapping of feature ID to attribute value or tuple of values.

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
    keys = {
        "id": list(contain(id_field_names)),
        "attribute": list(contain(field_names)),
    }
    cursor = arcpy.da.SearchCursor(
        in_table=dataset_path,
        field_names=keys["id"] + keys["attribute"],
        where_clause=kwargs["dataset_where_sql"],
        spatial_reference=spatial_reference(kwargs["spatial_reference_item"]),
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
                    else tuple(feature[len(keys["id"]) :])
                ),
            }
            id_attributes[value["id"]] = value["attributes"]
    return id_attributes


def update_by_central_overlay(
    dataset_path, field_name, overlay_dataset_path, overlay_field_name, **kwargs
):
    """Update attribute values by finding the central overlay feature value.

    Note:
        Since only one value will be selected in the overlay, operations with multiple
        overlaying features will respect the geoprocessing environment merge rule. This
        rule generally defaults to the value of the "first" feature.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        overlay_dataset_path (str): Path of the overlay-dataset.
        overlay_field_name (str): Name of the overlay-field.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        overlay_where_sql (str): SQL where-clause for overlay dataset subselection.
        replacement_value: Value to replace a present overlay-field value with.
        tolerance (float): Tolerance for coincidence, in units of the dataset.
        use_edit_session (bool): Updates are done in an edit session if True. Default is
            False.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        collections.Counter: Counts of features for each update-state.
    """
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level,
        "Start: Update attributes in `%s.%s` by central-overlay value in `%s.%s`.",
        dataset_path,
        field_name,
        overlay_dataset_path,
        overlay_field_name,
    )
    meta = {"original_tolerance": arcpy.env.XYTolerance}
    view = {
        "dataset": DatasetView(
            # Do *not* include any fields here (avoids name collisions in temp output).
            dataset_path,
            kwargs.get("dataset_where_sql"),
            field_names=[],
        ),
        "overlay": DatasetView(
            overlay_dataset_path,
            kwargs.get("overlay_where_sql"),
            field_names=[overlay_field_name],
        ),
    }
    with view["dataset"], view["overlay"]:
        temp_output_path = unique_path("output")
        if "tolerance" in kwargs:
            arcpy.env.XYTolerance = kwargs["tolerance"]
        arcpy.analysis.SpatialJoin(
            target_features=view["dataset"].name,
            join_features=view["overlay"].name,
            out_feature_class=temp_output_path,
            join_operation="JOIN_ONE_TO_ONE",
            join_type="KEEP_ALL",
            match_option="HAVE_THEIR_CENTER_IN",
        )
        if "tolerance" in kwargs:
            arcpy.env.XYTolerance = meta["original_tolerance"]
    if kwargs.get("replacement_value") is not None:
        update_by_function(
            temp_output_path,
            field_name=overlay_field_name,
            function=lambda x: kwargs["replacement_value"] if x else None,
            log_level=logging.DEBUG,
        )
    states = update_by_joined_value(
        dataset_path,
        field_name,
        join_dataset_path=temp_output_path,
        join_field_name=overlay_field_name,
        on_field_pairs=[("OID@", "TARGET_FID")],
        dataset_where_sql=kwargs.get("dataset_where_sql"),
        use_edit_session=kwargs.get("use_edit_session", False),
        log_level=logging.DEBUG,
    )
    dataset.delete(temp_output_path, log_level=logging.DEBUG)
    log_entity_states("attributes", states, LOG, log_level=level)
    LOG.log(level, "End: Update.")
    return states


def update_by_dominant_overlay(
    dataset_path, field_name, overlay_dataset_path, overlay_field_name, **kwargs
):
    """Update attribute values by finding the dominant overlay feature value.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        overlay_dataset_path (str): Path of the overlay-dataset.
        overlay_field_name (str): Name of the overlay-field.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        include_missing_overlay_value_area (bool): If True, the collective area where no
            overlay value exists (i.e. no overlay geometry + overlay of NoneType value)
            is considered a valid candidate for the dominant overlay. Default is False.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        overlay_where_sql (str): SQL where-clause for overlay dataset subselection.
        tolerance (float): Tolerance for coincidence, in units of the dataset.
        use_edit_session (bool): Updates are done in an edit session if True. Default is
            False.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        collections.Counter: Counts of features for each update-state.
    """
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level,
        "Start: Update attributes in `%s.%s` by dominant overlay value in `%s.%s`.",
        dataset_path,
        field_name,
        overlay_dataset_path,
        overlay_field_name,
    )
    meta = {"original_tolerance": arcpy.env.XYTolerance}
    view = {
        "dataset": DatasetView(
            # Do *not* include any fields here (avoids name collisions in temp output).
            dataset_path,
            kwargs.get("dataset_where_sql"),
            field_names=[],
        ),
        "overlay": DatasetView(
            overlay_dataset_path,
            kwargs.get("overlay_where_sql"),
            field_names=[overlay_field_name],
        ),
    }
    with view["dataset"], view["overlay"]:
        temp_output_path = unique_path("output")
        if "tolerance" in kwargs:
            arcpy.env.XYTolerance = kwargs["tolerance"]
        arcpy.analysis.Identity(
            in_features=view["dataset"].name,
            identity_features=view["overlay"].name,
            out_feature_class=temp_output_path,
            join_attributes="ALL",
        )
        if "tolerance" in kwargs:
            arcpy.env.XYTolerance = meta["original_tolerance"]
    meta["output"] = dataset_metadata(temp_output_path)
    # Identity makes custom FID field names - in_features FID field comes first.
    fid_keys = [key for key in meta["output"]["field_names"] if key.startswith("FID_")]
    coverage = {}
    for oid, overlay_oid, value, area in as_iters(
        temp_output_path, field_names=fid_keys + [overlay_field_name, "SHAPE@AREA"]
    ):
        # Def check for -1 OID (no overlay feature): identity does not set to None.
        if overlay_oid == -1:
            value = None
        if value is None and not kwargs.get(
            "include_missing_overlay_value_area", False
        ):
            continue

        if oid not in coverage:
            coverage[oid] = defaultdict(float)
        coverage[oid][value] += area
    dataset.delete(temp_output_path, log_level=logging.DEBUG)
    oid_value_map = {
        oid: max(value_area.items(), key=itemgetter(1))[0]
        for oid, value_area in coverage.items()
    }
    states = update_by_mapping(
        dataset_path,
        field_name,
        mapping=oid_value_map,
        key_field_names=["OID@"],
        dataset_where_sql=kwargs.get("dataset_where_sql"),
        use_edit_session=kwargs.get("use_edit_session", False),
        log_level=logging.DEBUG,
    )
    log_entity_states("attributes", states, LOG, log_level=level)
    LOG.log(level, "End: Update.")
    return states


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
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        collections.Counter: Counts of features for each update-state.
    """
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("use_edit_session", False)
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level,
        "Start: Update attributes in `%s.%s` by code in `%s` using domain `%s`.",
        dataset_path,
        field_name,
        code_field_name,
        domain_name,
    )
    meta = {"domain": domain_metadata(domain_name, domain_workspace_path)}
    states = update_by_function(
        dataset_path,
        field_name,
        function=meta["domain"]["code_description_map"].get,
        field_as_first_arg=False,
        arg_field_names=[code_field_name],
        dataset_where_sql=kwargs["dataset_where_sql"],
        use_edit_session=kwargs["use_edit_session"],
        log_level=logging.DEBUG,
    )
    log_entity_states("attributes", states, LOG, log_level=level)
    LOG.log(level, "End: Update.")
    return states


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
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        str: Name of the field updated.
    """
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("use_edit_session", False)
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level,
        "Start: Update attributes in `%s.%s` using expression `%s`.",
        dataset_path,
        field_name,
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
            # Py2.
            expression_type="python3" if sys.version_info.major < 3 else "python_9.3",
        )
    LOG.log(level, "End: Update.")
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
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        collections.Counter: Counts of features for each update-state.
    """
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("use_edit_session", False)
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level,
        "Start: Update attributes in `%s.%s` with %s by feature-matching"
        " on identifiers `%s`.",
        dataset_path,
        field_name,
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
                """{} is required keyword argument when update_type == "{}".""".format(
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
    states = Counter()
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
                states["unchanged"] += 1
            else:
                try:
                    cursor.updateRow(feature[:-1] + [value["new"]])
                    states["altered"] += 1
                except RuntimeError:
                    LOG.error("Offending value is %s", value["new"])
                    raise

    log_entity_states("attributes", states, LOG, log_level=level)
    LOG.log(level, "End: Update.")
    return states


def update_by_field(dataset_path, field_name, source_field_name, **kwargs):
    """Update attribute values with values from another field.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        source_field_name (str): Name of the field to get values from.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        spatial_reference_item: Item from which the spatial reference for the output
            geometry property will be derived. If not specified or None, the spatial
            reference of the dataset is used as the default.
        use_edit_session (bool): Updates are done in an edit session if True. Default is
            False.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        collections.Counter: Counts of features for each update-state.
    """
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("spatial_reference_item")
    kwargs.setdefault("use_edit_session", False)
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level,
        "Start: Update attributes in `%s.%s` from `%s.%s`.",
        dataset_path,
        field_name,
        dataset_path,
        source_field_name,
    )
    meta = {"dataset": dataset_metadata(dataset_path)}
    session = Editor(meta["dataset"]["workspace_path"], kwargs["use_edit_session"])
    cursor = arcpy.da.UpdateCursor(
        in_table=dataset_path,
        field_names=[field_name, source_field_name],
        where_clause=kwargs["dataset_where_sql"],
        spatial_reference=spatial_reference(kwargs["spatial_reference_item"]),
    )
    states = Counter()
    with session, cursor:
        for feature in cursor:
            value = {"old": feature[0], "new": feature[1]}
            if same_value(value["old"], value["new"]):
                states["unchanged"] += 1
            else:
                try:
                    cursor.updateRow([value["new"], value["new"]])
                    states["altered"] += 1
                except RuntimeError:
                    LOG.error("Offending value is `%s`", value["new"])
                    raise

    log_entity_states("attributes", states, LOG, log_level=level)
    LOG.log(level, "End: Update.")
    return states


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
        spatial_reference_item: Item from which the spatial reference for the output
            geometry property will be derived. If not specified or None, the spatial
            reference of the dataset is used as the default.
        use_edit_session (bool): Updates are done in an edit session if True. Default is
            False.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        collections.Counter: Counts of features for each update-state.
    """
    kwargs.setdefault("field_as_first_arg", True)
    kwargs.setdefault("arg_field_names", [])
    kwargs.setdefault("kwarg_field_names", [])
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("spatial_reference_item")
    kwargs.setdefault("use_edit_session", False)
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level,
        "Start: Update attributes in `%s.%s` by %s.",
        dataset_path,
        field_name,
        # Partials show all the pre-loaded arg & kwarg values, which is cumbersome.
        "partial version of function {}".format(function.func)
        if isinstance(function, partial)
        else "function `{}`".format(function),
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
        spatial_reference=spatial_reference(kwargs["spatial_reference_item"]),
    )
    states = Counter()
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
                states["unchanged"] += 1
            else:
                try:
                    cursor.updateRow(feature[:-1] + [value["new"]])
                    states["altered"] += 1
                except RuntimeError:
                    LOG.error("Offending value is %s", value["new"])
                    raise

    log_entity_states("attributes", states, LOG, log_level=level)
    LOG.log(level, "End: Update.")
    return states


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
            geometry property will be derived. If not specified or None, the spatial
            reference of the dataset is used as the default.
        use_edit_session (bool): Updates are done in an edit session if True. Default is
            False.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        collections.Counter: Counts of features for each update-state.
    """
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("spatial_reference_item")
    kwargs.setdefault("use_edit_session", False)
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level,
        "Start: Update attributes in `%s.%s` by geometry properties `%s`.",
        dataset_path,
        field_name,
        geometry_properties,
    )
    session = Editor(
        dataset_metadata(dataset_path)["workspace_path"], kwargs["use_edit_session"]
    )
    cursor = arcpy.da.UpdateCursor(
        in_table=dataset_path,
        field_names=["SHAPE@", field_name],
        where_clause=kwargs["dataset_where_sql"],
        spatial_reference=spatial_reference(kwargs["spatial_reference_item"]),
    )
    states = Counter()
    with session, cursor:
        for feature in cursor:
            value = {"geometry": feature[0], "old": feature[-1]}
            value["new"] = property_value(
                value["geometry"],
                GEOMETRY_PROPERTY_TRANSFORM,
                *contain(geometry_properties)
            )
            if same_value(value["old"], value["new"]):
                states["unchanged"] += 1
            else:
                try:
                    cursor.updateRow([value["geometry"], value["new"]])
                    states["altered"] += 1
                except RuntimeError:
                    LOG.error("Offending value is %s", value["new"])
                    raise

    log_entity_states("attributes", states, LOG, log_level=level)
    LOG.log(level, "End: Update.")
    return states


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
        join_where_sql (str): SQL where-clause for join-dataset subselection.
        use_edit_session (bool): Updates are done in an edit session if True. Default is
            False.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        collections.Counter: Counts of features for each update-state.
    """
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("use_edit_session", False)
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level,
        "Start: Update attributes in `%s.%s` by joined values in `%s.%s`.",
        dataset_path,
        field_name,
        join_dataset_path,
        join_field_name,
    )
    meta = {"dataset": dataset_metadata(dataset_path)}
    keys = {
        "dataset_id": list(pair[0] for pair in on_field_pairs),
        "join_id": list(pair[1] for pair in on_field_pairs),
    }
    keys["feature"] = keys["dataset_id"] + [field_name]
    join_value = id_values_map(
        join_dataset_path,
        id_field_names=keys["join_id"],
        field_names=join_field_name,
        dataset_where_sql=kwargs.get("join_where_sql"),
    )
    session = Editor(meta["dataset"]["workspace_path"], kwargs["use_edit_session"])
    cursor = arcpy.da.UpdateCursor(
        in_table=dataset_path,
        field_names=keys["feature"],
        where_clause=kwargs["dataset_where_sql"],
    )
    states = Counter()
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
                states["unchanged"] += 1
            else:
                try:
                    cursor.updateRow(feature[:-1] + [value["new"]])
                    states["altered"] += 1
                except RuntimeError:
                    LOG.error("Offending value is `%s`", value["new"])
                    raise

    log_entity_states("attributes", states, LOG, log_level=level)
    LOG.log(level, "End: Update.")
    return states


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
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        collections.Counter: Counts of features for each update-state.
    """
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("default_value")
    kwargs.setdefault("use_edit_session", False)
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level,
        "Start: Update attributes in `%s.%s` by mapping with key in `%s`.",
        dataset_path,
        field_name,
        key_field_names,
    )
    meta = {"dataset": dataset_metadata(dataset_path)}
    keys = {"map": list(contain(key_field_names))}
    keys["feature"] = keys["map"] + [field_name]
    if isinstance(mapping, tuple(EXEC_TYPES)):
        mapping = mapping()
    session = Editor(meta["dataset"]["workspace_path"], kwargs["use_edit_session"])
    cursor = arcpy.da.UpdateCursor(
        in_table=dataset_path,
        field_names=keys["feature"],
        where_clause=kwargs["dataset_where_sql"],
    )
    states = Counter()
    with session, cursor:
        for feature in cursor:
            value = {
                "map_key": feature[0] if len(keys["map"]) == 1 else tuple(feature[:-1]),
                "old": feature[-1],
            }
            value["new"] = mapping.get(value["map_key"], kwargs["default_value"])
            if same_value(value["old"], value["new"]):
                states["unchanged"] += 1
            else:
                try:
                    cursor.updateRow(feature[:-1] + [value["new"]])
                    states["altered"] += 1
                except RuntimeError:
                    LOG.error("Offending value is `%s`", value["new"])
                    raise

    log_entity_states("attributes", states, LOG, log_level=level)
    LOG.log(level, "End: Update.")
    return states


def update_by_node_ids(dataset_path, from_id_field_name, to_id_field_name, **kwargs):
    """Update attribute values by node IDs.

    Args:
        dataset_path (str): Path of the dataset.
        from_id_field_name (str): Name of the from-ID field.
        to_id_field_name (str): Name of the to-ID field.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        use_edit_session (bool): Updates are done in an edit session if True. Default is
            False.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        collections.Counter: Counts of features for each update-state.
    """
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("use_edit_session", False)
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level,
        "Start: Update attributes in `%s.%s` & `%s` by node IDs.",
        dataset_path,
        from_id_field_name,
        to_id_field_name,
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
    states = Counter()
    with session, cursor:
        for feature in cursor:
            value = {"oid": feature[0], "old_nodes": feature[1:]}
            value["new_nodes"] = [
                oid_node[value["oid"]]["from"],
                oid_node[value["oid"]]["to"],
            ]
            if same_feature(value["old_nodes"], value["new_nodes"]):
                states["unchanged"] += 1
            else:
                try:
                    cursor.updateRow([value["oid"]] + value["new_nodes"])
                    states["altered"] += 1
                except RuntimeError:
                    LOG.error("Offending value one of %s", value["new_nodes"])
                    raise

    log_entity_states("attributes", states, LOG, log_level=level)
    LOG.log(level, "End: Update.")
    return states


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
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        collections.Counter: Counts of features for each update-state.
    """
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("overlay_central_coincident", False)
    kwargs.setdefault("overlay_most_coincident", False)
    kwargs.setdefault("overlay_where_sql")
    kwargs.setdefault("use_edit_session", False)
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level,
        "Start: Update attributes in `%s.%s` by overlay values in `%s.%s`.",
        dataset_path,
        field_name,
        overlay_dataset_path,
        overlay_field_name,
    )
    meta = {
        "dataset": dataset_metadata(dataset_path),
        "original_tolerance": arcpy.env.XYTolerance,
    }
    join_kwargs = {"join_operation": "join_one_to_many", "join_type": "keep_all"}
    if kwargs["overlay_central_coincident"]:
        join_kwargs["match_option"] = "have_their_center_in"
    elif kwargs["overlay_most_coincident"]:
        raise NotImplementedError("overlay_most_coincident not yet implemented.")

    # else:
    #     join_kwargs["match_option"] = "intersect"
    dataset_view = DatasetView(dataset_path, kwargs["dataset_where_sql"])
    overlay_copy = TempDatasetCopy(
        overlay_dataset_path,
        kwargs["overlay_where_sql"],
        field_names=[overlay_field_name],
        # BUG-000134367: "memory" workspaces cannot use Alter Field.
        # Fall back to old "in_memory"; remove once bug is cleared.
        output_path=unique_path("overlay", workspace_path="in_memory"),
    )
    with dataset_view, overlay_copy:
        # Avoid field name collisions with neutral name.
        overlay_copy.field_name = dataset.rename_field(
            overlay_copy.path,
            overlay_field_name,
            new_field_name=unique_name(overlay_field_name),
            log_level=logging.DEBUG,
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
    if kwargs.get("replacement_value") is not None:
        update_by_function(
            temp_output_path,
            field_name=overlay_copy.field_name,
            function=lambda x: kwargs["replacement_value"] if x else None,
            log_level=logging.DEBUG,
        )
    # Update values in original dataset.
    states = update_by_joined_value(
        dataset_path,
        field_name,
        join_dataset_path=temp_output_path,
        join_field_name=overlay_copy.field_name,
        on_field_pairs=[(meta["dataset"]["oid_field_name"], "target_fid")],
        dataset_where_sql=kwargs["dataset_where_sql"],
        use_edit_session=kwargs["use_edit_session"],
        log_level=logging.DEBUG,
    )
    dataset.delete(temp_output_path, log_level=logging.DEBUG)
    log_entity_states("attributes", states, LOG, log_level=level)
    LOG.log(level, "End: Update.")
    return states


def update_by_overlay_count(dataset_path, field_name, overlay_dataset_path, **kwargs):
    """Update attribute values by finding overlay feature value.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        overlay_dataset_path (str): Path of the overlay-dataset.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        overlay_where_sql (str): SQL where-clause for overlay dataset subselection.
        tolerance (float): Tolerance for coincidence, in units of the dataset.
        use_edit_session (bool): Updates are done in an edit session if True. Default is
            False.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        collections.Counter: Counts of features for each update-state.
    """
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level,
        "Start: Update attributes in `%s.%s` by overlay counts from `%s`.",
        dataset_path,
        field_name,
        overlay_dataset_path,
    )
    meta = {
        "dataset": dataset_metadata(dataset_path),
        "original_tolerance": arcpy.env.XYTolerance,
    }

    dataset_view = DatasetView(dataset_path, kwargs.get("dataset_where_sql"))
    overlay_view = DatasetView(
        overlay_dataset_path, kwargs.get("overlay_where_sql"), field_names=[]
    )
    with dataset_view, overlay_view:
        if "tolerance" in kwargs:
            arcpy.env.XYTolerance = kwargs["tolerance"]
        # Create temp output of the overlay.
        temp_output_path = unique_path("output")
        arcpy.analysis.SpatialJoin(
            target_features=dataset_view.name,
            join_features=overlay_view.name,
            out_feature_class=temp_output_path,
            join_operation="join_one_to_one",
            join_type="keep_common",
            match_option="intersect",
        )
        if "tolerance" in kwargs:
            arcpy.env.XYTolerance = meta["original_tolerance"]
    oid_overlay_count = dict(
        as_iters(temp_output_path, field_names=["TARGET_FID", "Join_Count"])
    )
    dataset.delete(temp_output_path, log_level=logging.DEBUG)
    session = Editor(
        meta["dataset"]["workspace_path"], kwargs.get("use_edit_session", False)
    )
    cursor = arcpy.da.UpdateCursor(
        in_table=dataset_path,
        field_names=["oid@", field_name],
        where_clause=kwargs.get("dataset_where_sql"),
    )
    states = Counter()
    with session, cursor:
        for feature in cursor:
            value = {"old": feature[1], "new": oid_overlay_count.get(feature[0], 0)}
            if same_value(value["old"], value["new"]):
                states["unchanged"] += 1
            else:
                try:
                    cursor.updateRow([value["new"], value["new"]])
                    states["altered"] += 1
                except RuntimeError:
                    LOG.error("Offending value is `%s`", value["new"])
                    raise
    log_entity_states("attributes", states, LOG, log_level=level)
    LOG.log(level, "End: Update.")
    return states


def update_by_unique_id(dataset_path, field_name, **kwargs):
    """Update attribute values by assigning a unique ID.

    Existing IDs are preserved, if unique.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        start_after_max_number (bool): Initial number will be one greater than the
            maximum existing ID number if True. Default is False.
        initial_number (int): Initial number for a proposed ID, if using a numeric data
            type. Default is 1. Superseded by start_after_max_number.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        use_edit_session (bool): Updates are done in an edit session if True. Default is
            False.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        collections.Counter: Counts of features for each update-state.
    """
    kwargs.setdefault("start_after_max_number", False)
    kwargs.setdefault("initial_number", 1)
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("use_edit_session", False)
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level,
        "Start: Update attributes in `%s.%s` by assigning unique IDs.",
        dataset_path,
        field_name,
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
                initial_number=(
                    max(used_ids) + 1
                    if kwargs["start_after_max_number"]
                    else kwargs["initial_number"]
                ),
            )
        # Second run will fill in missing IDs.
        states = Counter()
        with cursor:
            for [id_value] in cursor:
                if id_value is not None:
                    states["unchanged"] += 1
                else:
                    id_value = next(id_pool)
                    while id_value in used_ids:
                        id_value = next(id_pool)
                    try:
                        cursor.updateRow([id_value])
                        states["altered"] += 1
                        used_ids.add(id_value)
                    except RuntimeError:
                        LOG.error("Offending value is %s", id_value)
                        raise

    log_entity_states("attributes", states, LOG, log_level=level)
    LOG.log(level, "End: Update.")
    return states


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
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        collections.Counter: Counts of features for each update-state.
    """
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("use_edit_session", False)
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level,
        "Start: Update attributes in `%s.%s` by given value.",
        dataset_path,
        field_name,
    )
    meta = {"dataset": dataset_metadata(dataset_path)}
    session = Editor(meta["dataset"]["workspace_path"], kwargs["use_edit_session"])
    cursor = arcpy.da.UpdateCursor(
        in_table=dataset_path,
        field_names=[field_name],
        where_clause=kwargs["dataset_where_sql"],
    )
    states = Counter()
    with session, cursor:
        for [old_value] in cursor:
            if same_value(old_value, value):
                states["unchanged"] += 1
            else:
                try:
                    cursor.updateRow([value])
                    states["altered"] += 1
                except RuntimeError:
                    LOG.error("Offending value is `%s`.", value)
                    raise

    log_entity_states("attributes", states, LOG, log_level=level)
    LOG.log(level, "End: Update.")
    return states
