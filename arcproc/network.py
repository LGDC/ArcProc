"""Network analysis operations."""
from collections import Counter
from copy import copy, deepcopy
import logging
from pathlib import Path
from typing import Any, Iterable, Optional, Union

import arcpy

from arcproc import attributes
from arcproc import dataset
from arcproc.dataset import DatasetView
from arcproc.helpers import log_entity_states, python_type, same_feature, unique_ids
from arcproc.metadata import Dataset, Field, SpatialReference
from arcproc.workspace import Editing


LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""

UNIT_PLURAL = {"Foot": "Feet", "Meter": "Meters"}
"""Mapping of singular unit to plural. Only need common ones from spatial references."""
TYPE_ID_FUNCTION_MAP = {
    "short": (lambda x: int(x.split(" : ")[0]) if x else None),
    "long": (lambda x: int(x.split(" : ")[0]) if x else None),
    "double": (lambda x: float(x.split(" : ")[0]) if x else None),
    "single": (lambda x: float(x.split(" : ")[0]) if x else None),
    "string": (lambda x: x.split(" : ")[0] if x else None),
    "text": (lambda x: x.split(" : ")[0] if x else None),
}
"""dict: Mapping of ArcGIS field type to ID extract function."""


arcpy.SetLogHistory(False)


def build_network(network_path, **kwargs):
    """Build network dataset.

    Args:
        network_path (pathlib.Path, str): Path of the network dataset.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        pathlib.Path: Path of the network dataset.
    """
    network_path = Path(network_path)
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(level, "Start: Build network `%s`.", network_path)
    # ArcPy2.8.0: Convert to str.
    arcpy.nax.BuildNetwork(in_network_dataset=str(network_path))
    LOG.log(level, "End: Build.")
    return network_path


def closest_facility_route(
    dataset_path,
    id_field_name,
    facility_path,
    facility_id_field_name,
    network_path,
    travel_mode,
    **kwargs,
):
    """Generate route info dictionaries for closest facility to each location feature.

    Args:
        dataset_path (pathlib.Path, str): Path of the dataset.
        id_field_name (str): Name of the dataset ID field.
        facility_path (pathlib.Path, str): Path of the facilities dataset.
        facility_id_field_name (str): Name of the facility ID field.
        network_path (pathlib.Path, str): Path of the network dataset.
        travel_mode (str): Name of the network travel mode to use.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        facility_where_sql (str): SQL where-clause for facility subselection.
        max_cost (float): Maximum travel cost the search will attempt, in the units of
            the cost attribute.
        travel_from_facility (bool): Flag to indicate performing the analysis
            travelling from (True) or to (False) the facility. Default is False.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Yields:
        dict: Analysis result details of feature.
            Dictionary keys: "dataset_id", "facility_id", "cost", "geometry",
            "cost" value (float) will match units of the travel mode impedance.
            "geometry" (arcpy.Geometry) will match spatial reference to the dataset.
    """
    dataset_path = Path(dataset_path)
    facility_path = Path(facility_path)
    network_path = Path(network_path)
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("facility_where_sql")
    kwargs.setdefault("max_cost")
    kwargs.setdefault("travel_from_facility", False)
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level,
        "Start: Generate closest facility in `%s` to locations in `%s`.",
        facility_path,
        dataset_path,
    )
    analysis = arcpy.nax.ClosestFacility(network_path)
    analysis.defaultImpedanceCutoff = kwargs["max_cost"]
    distance_units = UNIT_PLURAL[SpatialReference(dataset_path).linear_unit]
    analysis.distanceUnits = getattr(arcpy.nax.DistanceUnits, distance_units)
    analysis.ignoreInvalidLocations = True
    if kwargs["travel_from_facility"]:
        analysis.travelDirection = arcpy.nax.TravelDirection.FromFacility
    # ArcPy2.8.0: Convert to str.
    analysis.travelMode = arcpy.nax.GetTravelModes(network_path)[travel_mode]
    # Load facilities.
    input_type = arcpy.nax.ClosestFacilityInputDataType.Facilities
    field = Field(
        facility_path,
        Dataset(facility_path).oid_field_name
        if facility_id_field_name.upper() == "OID@"
        else facility_id_field_name,
    )
    field_description = [
        "source_id",
        field.type,
        "#",
        field.length,
        "#",
        "#",
    ]
    if field_description[1] == "OID":
        field_description[1] = "LONG"
    analysis.addFields(input_type, [field_description])
    cursor = analysis.insertCursor(input_type, field_names=["source_id", "SHAPE@"])
    rows = attributes.as_tuples(
        facility_path,
        field_names=[facility_id_field_name, "SHAPE@"],
        dataset_where_sql=kwargs["facility_where_sql"],
    )
    with cursor:
        for row in rows:
            cursor.insertRow(row)
    # Load dataset locations.
    input_type = arcpy.nax.ClosestFacilityInputDataType.Incidents
    field = Field(
        dataset_path,
        Dataset(dataset_path).oid_field_name
        if id_field_name.upper() == "OID@"
        else id_field_name,
    )
    field_description = [
        "source_id",
        field.type,
        "#",
        field.length,
        "#",
        "#",
    ]
    if field_description[1] == "OID":
        field_description[1] = "LONG"
    analysis.addFields(input_type, [field_description])
    cursor = analysis.insertCursor(input_type, field_names=["source_id", "SHAPE@"])
    rows = attributes.as_tuples(
        dataset_path,
        field_names=[id_field_name, "SHAPE@"],
        dataset_where_sql=kwargs["dataset_where_sql"],
    )
    with cursor:
        for row in rows:
            cursor.insertRow(row)
    # Solve & generate.
    result = analysis.solve()
    if not result.solveSucceeded:
        for message in result.solverMessages(arcpy.nax.MessageSeverity.All):
            LOG.error(message)
        raise RuntimeError("Closest facility analysis failed")

    oid_id = {
        sublayer: dict(
            result.searchCursor(
                output_type=getattr(arcpy.nax.ClosestFacilityOutputDataType, sublayer),
                field_names=[id_key, "source_id"],
            )
        )
        for sublayer, id_key in [
            ("Facilities", "FacilityOID"),
            ("Incidents", "IncidentOID"),
        ]
    }
    keys = ["FacilityOID", "IncidentOID", f"Total_{distance_units}", "SHAPE@"]
    cursor = result.searchCursor(
        output_type=arcpy.nax.ClosestFacilityOutputDataType.Routes, field_names=keys
    )
    with cursor:
        for row in cursor:
            route = dict(zip(keys, row))
            yield {
                "dataset_id": oid_id["Incidents"][route["IncidentOID"]],
                "facility_id": oid_id["Facilities"][route["FacilityOID"]],
                "cost": route[f"Total_{distance_units}"],
                "geometry": route["SHAPE@"],
            }
    LOG.log(level, "End: Generate.")


def generate_service_areas(
    dataset_path, output_path, network_path, cost_attribute, max_distance, **kwargs
):
    """Create network service area features.

    Args:
        dataset_path (pathlib.Path, str): Path of the dataset.
        output_path (pathlib.Path, str): Path of the output service areas dataset.
        network_path (pathlib.Path, str): Path of the network dataset.
        cost_attribute (str): Name of the network cost attribute to use.
        max_distance (float): Distance in travel from the facility the outer ring will
            extend to, in the units of the dataset.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        id_field_name (str): Name of facility ID field.
        restriction_attributes (iter): Collection of network restriction attribute
            names to use.
        travel_from_facility (bool): Flag to indicate performing the analysis
            travelling from (True) or to (False) the facility. Default is False.
        detailed_features (bool): Flag to generate high-detail features. Default is
            False.
        overlap_facilities (bool): Flag to overlap different facility service areas.
            Default is True.
        trim_value (float): Dstance from the network features to trim service areas at.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        pathlib.Path: Path of the output service areas dataset.
    """
    dataset_path = Path(dataset_path)
    output_path = Path(output_path)
    network_path = Path(network_path)
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("id_field_name")
    kwargs.setdefault("restriction_attributes")
    kwargs.setdefault("travel_from_facility", False)
    kwargs.setdefault("detailed_features", False)
    kwargs.setdefault("overlap_facilities", True)
    kwargs.setdefault("trim_value")
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(level, "Start: Generate service areas for `%s`.", dataset_path)
    # trim_value assumes meters if not input as linear_unit string.
    if kwargs["trim_value"] is not None:
        trim_value = (
            f"""{kwargs["trim_value"]} {SpatialReference(dataset_path).linear_unit}"""
        )
    else:
        trim_value = None
    view = DatasetView(dataset_path, kwargs["dataset_where_sql"])
    arcpy.na.MakeServiceAreaLayer(
        # ArcPy2.8.0: Convert to str.
        in_network_dataset=str(network_path),
        out_network_analysis_layer="service_area",
        impedance_attribute=cost_attribute,
        travel_from_to=(
            "travel_from" if kwargs["travel_from_facility"] else "travel_to"
        ),
        default_break_values="{}".format(max_distance),
        polygon_type=(
            "detailed_polys" if kwargs["detailed_features"] else "simple_polys"
        ),
        merge=("no_merge" if kwargs["overlap_facilities"] else "no_overlap"),
        nesting_type="disks",
        UTurn_policy="allow_dead_ends_and_intersections_only",
        restriction_attribute_name=kwargs["restriction_attributes"],
        polygon_trim=(True if trim_value else False),
        poly_trim_value=trim_value,
        hierarchy="no_hierarchy",
    )
    with view:
        arcpy.na.AddLocations(
            in_network_analysis_layer="service_area",
            sub_layer="Facilities",
            in_table=view.name,
            field_mappings="Name {} #".format(kwargs["id_field_name"]),
            search_tolerance=max_distance,
            match_type="match_to_closest",
            append="clear",
            snap_to_position_along_network="no_snap",
            exclude_restricted_elements=True,
        )
    arcpy.na.Solve(
        in_network_analysis_layer="service_area",
        ignore_invalids=True,
        terminate_on_solve_error=True,
    )
    dataset.copy("service_area/Polygons", output_path, log_level=logging.DEBUG)
    arcpy.management.Delete("service_area")
    if kwargs["id_field_name"]:
        id_field = Field(dataset_path, kwargs["id_field_name"])
        dataset.add_field(output_path, log_level=logging.DEBUG, **id_field.as_dict)
        attributes.update_by_function(
            output_path,
            field_name=id_field.name,
            function=TYPE_ID_FUNCTION_MAP[id_field.type],
            field_as_first_arg=False,
            arg_field_names=["Name"],
            log_level=logging.DEBUG,
        )
    LOG.log(level, "End: Generate.")
    return output_path


def generate_service_rings(
    dataset_path,
    output_path,
    network_path,
    cost_attribute,
    ring_width,
    max_distance,
    **kwargs,
):
    """Create facility service ring features using a network dataset.

    Args:
        dataset_path (pathlib.Path, str): Path of the dataset.
        output_path (pathlib.Path, str): Path of the output service rings dataset.
        network_path (pathlib.Path, str): Path of the network dataset.
        cost_attribute (str): Name of the network cost attribute to use.
        ring_width (float): Distance a service ring represents in travel, in the
            units of the dataset.
        max_distance (float): Distance in travel from the facility the outer ring will
            extend to, in the units of the dataset.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        id_field_name (str): Name of facility ID field.
        restriction_attributes (iter): Collection of network restriction attribute
            names to use.
        travel_from_facility (bool): Flag to indicate performing the analysis
            travelling from (True) or to (False) the facility. Default is False.
        detailed_features (bool): Flag to generate high-detail features. Default is
            False.
        overlap_facilities (bool): Flag to overlap different facility service areas.
            Default is True.
        trim_value (float): Dstance from the network features to trim service areas at.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        pathlib.Path: Path of the output service rings dataset.
    """
    dataset_path = Path(dataset_path)
    output_path = Path(output_path)
    network_path = Path(network_path)
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("id_field_name")
    kwargs.setdefault("restriction_attributes")
    kwargs.setdefault("travel_from_facility", False)
    kwargs.setdefault("detailed_features", False)
    kwargs.setdefault("overlap_facilities", True)
    kwargs.setdefault("trim_value")
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(level, "Start: Generate service rings for `%s`.", dataset_path)
    # trim_value assumes meters if not input as linear_unit string.
    if kwargs["trim_value"] is not None:
        trim_value = (
            f"""{kwargs["trim_value"]} {SpatialReference(dataset_path).linear_unit}"""
        )
    else:
        trim_value = None
    view = DatasetView(dataset_path, kwargs["dataset_where_sql"])
    arcpy.na.MakeServiceAreaLayer(
        # ArcPy2.8.0: Convert to str.
        in_network_dataset=str(network_path),
        out_network_analysis_layer="service_area",
        impedance_attribute=cost_attribute,
        travel_from_to=(
            "travel_from" if kwargs["travel_from_facility"] else "travel_to"
        ),
        default_break_values=(
            " ".join(str(x) for x in range(ring_width, max_distance + 1, ring_width))
        ),
        polygon_type=(
            "detailed_polys" if kwargs["detailed_features"] else "simple_polys"
        ),
        merge=("no_merge" if kwargs["overlap_facilities"] else "no_overlap"),
        nesting_type="rings",
        UTurn_policy="allow_dead_ends_and_intersections_only",
        restriction_attribute_name=kwargs["restriction_attributes"],
        polygon_trim=(True if trim_value is not None else False),
        poly_trim_value=trim_value,
        hierarchy="no_hierarchy",
    )
    with view:
        arcpy.na.AddLocations(
            in_network_analysis_layer="service_area",
            sub_layer="Facilities",
            in_table=view.name,
            field_mappings="Name {} #".format(kwargs["id_field_name"]),
            search_tolerance=max_distance,
            match_type="match_to_closest",
            append="clear",
            snap_to_position_along_network="no_snap",
            exclude_restricted_elements=True,
        )
    arcpy.na.Solve(
        in_network_analysis_layer="service_area",
        ignore_invalids=True,
        terminate_on_solve_error=True,
    )
    dataset.copy("service_area/Polygons", output_path, log_level=logging.DEBUG)
    arcpy.management.Delete("service_area")
    if kwargs["id_field_name"]:
        id_field = Field(dataset_path, kwargs["id_field_name"])
        dataset.add_field(output_path, log_level=logging.DEBUG, **id_field.as_dict)
        attributes.update_by_function(
            output_path,
            id_field.name,
            function=TYPE_ID_FUNCTION_MAP[id_field.type],
            field_as_first_arg=False,
            arg_field_names=["Name"],
            log_level=logging.DEBUG,
        )
    LOG.log(level, "End: Generate.")
    return output_path


# Node functions.


def _updated_coordinates_node_map(
    coordinates_node: "dict[tuple, dict]",
    node_id_data_type: Any,
    node_id_max_length: int,
) -> "dict[tuple, dict]":
    """Return updated mapping of coordinates to node info mapping.

    Args:
        coordinates_node_map: Mapping of coordinates tuple to node information
            dictionary.
        node_id_type: Value type for node ID.
        node_id_max_length: Maximum length for node ID, if ID data type is string.
    """

    def _feature_count(node: dict) -> int:
        """Return count of features associated with node."""
        return len(node["feature_ids"]["from"].union(node["feature_ids"]["to"]))

    used_node_ids = {
        node["node_id"]
        for node in coordinates_node.values()
        if node["node_id"] is not None
    }
    open_node_ids = (
        node_id
        for node_id in unique_ids(node_id_data_type, string_length=node_id_max_length)
        if node_id not in used_node_ids
    )
    coordinates_node = deepcopy(coordinates_node)
    node_id_coordinates = {}
    for coordinates, node in coordinates_node.items():
        # Assign IDs where missing.
        if node["node_id"] is None:
            node["node_id"] = next(open_node_ids)
        # If ID duplicate, re-ID node with least features.
        elif node["node_id"] in node_id_coordinates:
            other_coordinates = node_id_coordinates[node["node_id"]]
            other_node = copy(coordinates_node[other_coordinates])
            new_node_id = next(open_node_ids)
            if _feature_count(node) > _feature_count(other_node):
                other_node["node_id"] = new_node_id
                coordinates_node[other_coordinates] = other_node
                node_id_coordinates[new_node_id] = node_id_coordinates.pop(
                    node["node_id"]
                )
            else:
                node["node_id"] = new_node_id
        node_id_coordinates[node["node_id"]] = coordinates
    return coordinates_node


def coordinates_node_map(
    dataset_path: Union[Path, str],
    from_id_field_name: str,
    to_id_field_name: str,
    id_field_names: Iterable[str] = ("OID@",),
    update_nodes: bool = False,
    *,
    dataset_where_sql: Optional[str] = None,
    spatial_reference_item: Optional[Any] = None,
) -> "dict[tuple, dict]":
    """Return mapping of coordinates to node info mapping for dataset.

    Notes:
        From- & to-node IDs must be same attribute type.
        Output format:
            `{(x, y): {"node_id": Any, "feature_ids": {"from": set, "to": set}}}`

    Args:
        dataset_path: Path to the dataset.
        from_id_field_name: Name of the from-node ID field.
        to_id_field_name: Name of the to-node ID field.
        id_field_names: Names of the feature ID fields.
        update_nodes: Update nodes based on feature geometries if True.
        dataset_where_sql: SQL where-clause for dataset subselection.
        spatial_reference_item: Item from which the spatial reference of the output
            coordinates will be derived.
    """
    dataset_path = Path(dataset_path)
    id_field_names = list(id_field_names)
    node_id_data_type = None
    node_id_max_length = None
    for node_id_field_name in [from_id_field_name, to_id_field_name]:
        field = Field(dataset_path, node_id_field_name)
        if not node_id_data_type:
            node_id_data_type = python_type(field.type)
        elif python_type(field.type) != node_id_data_type:
            raise ValueError("From- and to-node ID fields must be same type.")

        if node_id_data_type == str:
            if not node_id_max_length or node_id_max_length > field.length:
                node_id_max_length = field.length
    coordinate_node = {}
    for feature in attributes.as_dicts(
        dataset_path,
        field_names=id_field_names + [from_id_field_name, to_id_field_name, "SHAPE@"],
        dataset_where_sql=dataset_where_sql,
        spatial_reference_item=spatial_reference_item,
    ):
        feature["from_id"] = feature[from_id_field_name]
        feature["to_id"] = feature[to_id_field_name]
        feature["id"] = tuple(feature[key] for key in id_field_names)
        feature["from_coordinates"] = (
            feature["SHAPE@"].firstPoint.X,
            feature["SHAPE@"].firstPoint.Y,
        )
        feature["to_coordinates"] = (
            feature["SHAPE@"].lastPoint.X,
            feature["SHAPE@"].lastPoint.Y,
        )
        for end in ["from", "to"]:
            if feature[f"{end}_coordinates"] not in coordinate_node:
                coordinate_node[feature[f"{end}_coordinates"]] = {
                    "node_id": feature[f"{end}_id"],
                    "feature_ids": {"from": set(), "to": set()},
                }
            node = coordinate_node[feature[f"{end}_coordinates"]]
            if node["node_id"] is None:
                node["node_id"] = feature[f"{end}_id"]
            # Assign lower node ID if newer is different than current.
            else:
                node["node_id"] = min(node, feature[f"{end}_id"])
            node["feature_ids"][end].add(feature["id"])
    if update_nodes:
        coordinate_node = _updated_coordinates_node_map(
            coordinate_node, node_id_data_type, node_id_max_length
        )
    return coordinate_node


def id_node_map(
    dataset_path: Union[Path, str],
    from_id_field_name: str,
    to_id_field_name: str,
    id_field_names: Iterable[str] = ("OID@",),
    update_nodes: bool = False,
    *,
    dataset_where_sql: Optional[str] = None,
) -> "dict[tuple, dict[str, Any]]":
    """Return mapping of feature ID to from- & to-node ID dictionary.

    Notes:
        From- & to-node IDs must be same attribute type.
        Output format:
            `{feature_id: {"from": from_node_id, "to": to_node_id}}`

    Args:
        dataset_path: Path to the dataset.
        from_id_field_name: Name of the from-node ID field.
        to_id_field_name: Name of the to-node ID field.
        id_field_names: Names of the feature ID fields.
        update_nodes: Update nodes based on feature geometries if True.
        dataset_where_sql: SQL where-clause for dataset subselection.
    """
    dataset_path = Path(dataset_path)
    id_field_names = list(id_field_names)
    id_node = {}
    if update_nodes:
        coordinate_node = coordinates_node_map(
            dataset_path,
            from_id_field_name,
            to_id_field_name,
            id_field_names,
            update_nodes,
            dataset_where_sql=dataset_where_sql,
        )
        for node in coordinate_node.values():
            for end in ["from", "to"]:
                for feature_id in node["feature_ids"][end]:
                    feature_id = feature_id[0] if len(feature_id) == 1 else feature_id
                    if feature_id not in id_node:
                        id_node[feature_id] = {}
                    id_node[feature_id][end] = node["node_id"]
    else:
        for feature in attributes.as_dicts(
            dataset_path,
            field_names=id_field_names + [from_id_field_name, to_id_field_name],
            dataset_where_sql=dataset_where_sql,
        ):
            feature["from_id"] = feature[from_id_field_name]
            feature["to_id"] = feature[to_id_field_name]
            feature["id"] = tuple(feature[key] for key in id_field_names)
            feature["id"] = (
                feature["id"][0] if len(feature["id"]) == 1 else feature["id"]
            )
            for end in ["from", "to"]:
                id_node[feature["id"]][end] = feature[f"{end}_id"]
    return id_node


def update_node_ids(
    dataset_path: Union[Path, str],
    from_id_field_name: str,
    to_id_field_name: str,
    *,
    dataset_where_sql: Optional[str] = None,
    use_edit_session: bool = False,
    log_level: int = logging.INFO,
) -> Counter:
    """Update node ID values.

    Args:
        dataset_path: Path to the dataset.
        from_id_field_name: Name of the from-node ID field.
        to_id_field_name: Name of the to-node ID field.
        dataset_where_sql: SQL where-clause for dataset subselection.
        use_edit_session: Updates are done in an edit session if True.
        log_level: Level to log the function at.

    Returns:
        Counts of features for each update-state.
    """
    dataset_path = Path(dataset_path)
    LOG.log(
        log_level,
        "Start: Update node IDs in `%s` (from) & `%s` (to) for `%s`.",
        from_id_field_name,
        to_id_field_name,
        dataset_path,
    )
    cursor = arcpy.da.UpdateCursor(
        # ArcPy2.8.0: Convert to str.
        in_table=str(dataset_path),
        field_names=["OID@", from_id_field_name, to_id_field_name],
        where_clause=dataset_where_sql,
    )
    oid_node = id_node_map(
        dataset_path, from_id_field_name, to_id_field_name, update_nodes=True
    )
    session = Editing(Dataset(dataset_path).workspace_path, use_edit_session)
    states = Counter()
    with session, cursor:
        for feature in cursor:
            oid = feature[0]
            new_feature = (oid, oid_node[oid]["from"], oid_node[oid]["to"])
            if same_feature(feature, new_feature):
                states["unchanged"] += 1
            else:
                try:
                    cursor.updateRow(new_feature)
                    states["altered"] += 1
                except RuntimeError as error:
                    raise RuntimeError(
                        f"Update cursor failed: Offending row: `{new_feature}`"
                    ) from error

    log_entity_states("attributes", states, logger=LOG, log_level=log_level)
    LOG.log(log_level, "End: Update.")
    return states
