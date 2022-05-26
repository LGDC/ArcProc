"""Network analysis operations."""
from collections import Counter
from copy import copy, deepcopy
import logging
from pathlib import Path
from types import FunctionType
from typing import Any, Dict, Iterable, Iterator, Mapping, Optional, Tuple, Union

import arcpy

from arcproc.attributes import update_field_with_function
from arcproc import dataset
from arcproc.dataset import DatasetView
from arcproc import features
from arcproc.helpers import log_entity_states, python_type, same_feature, unique_ids
from arcproc.metadata import (
    Dataset,
    Field,
    SpatialReference,
    SpatialReferenceSourceItem,
)
from arcproc.workspace import Editing


LOG: logging.Logger = logging.getLogger(__name__)
"""Module-level logger."""

TYPE_ID_FUNCTION_MAP: Dict[str, FunctionType] = {
    "short": (lambda x: int(x.split(" : ")[0]) if x else None),
    "long": (lambda x: int(x.split(" : ")[0]) if x else None),
    "double": (lambda x: float(x.split(" : ")[0]) if x else None),
    "single": (lambda x: float(x.split(" : ")[0]) if x else None),
    "string": (lambda x: x.split(" : ")[0] if x else None),
    "text": (lambda x: x.split(" : ")[0] if x else None),
}
"""Mapping of ArcGIS field type to ID extract function for network solution layer."""
UNIT_PLURAL: Dict[str, str] = {"Foot": "Feet", "Meter": "Meters"}
"""Mapping of singular unit to plural. Only need common ones from spatial references."""


arcpy.SetLogHistory(False)


def build_network(
    network_path: Union[Path, str], *, log_level: int = logging.INFO
) -> Dataset:
    """Build network dataset.

    Args:
        network_path: Path to network dataset.
        log_level: Level to log the function at.

    Returns:
        Dataset metadata instance for network dataset.
    """
    network_path = Path(network_path)
    LOG.log(log_level, "Start: Build network `%s`.", network_path)
    # ArcPy2.8.0: Convert Path to str.
    arcpy.nax.BuildNetwork(in_network_dataset=str(network_path))
    LOG.log(log_level, "End: Build.")
    return Dataset(network_path)


def closest_facility_route(
    dataset_path: Union[Path, str],
    *,
    id_field_name: str,
    facility_path: Union[Path, str],
    facility_id_field_name: str,
    network_path: Union[Path, str],
    dataset_where_sql: Optional[str] = None,
    facility_where_sql: Optional[str] = None,
    max_cost: Optional[Union[float, int]] = None,
    travel_from_facility: bool = False,
    travel_mode: str,
) -> Iterator[Dict[str, Any]]:
    """Generate route info dictionaries for closest facility to each location feature.

    Args:
        dataset_path: Path to dataset.
        id_field_name: Name of dataset ID field.
        facility_path: Path to facility dataset.
        facility_id_field_name: Name of facility dataset ID field.
        network_path: Path to network dataset.
        dataset_where_sql: SQL where-clause for dataset subselection.
        facility_where_sql: SQL where-clause for the facility dataset subselection.
        max_cost: Maximum travel cost the search will allow, in the units of the cost
            attribute.
        travel_from_facility: Perform the analysis travelling from the facility if True,
            rather than toward the facility.
        travel_mode: Name of the network travel mode to use. Travel mode must exist in
            the network dataset.

    Yields:
        Closest facility route details.
        Keys:
            * dataset_id
            * facility_id
            * cost - Cost of route, in units of travel mode impedance.
            * geometry - Route geometry, in spatial reference of dataset.

    Raises:
        RuntimeError: When analysis fails.
    """
    dataset_path = Path(dataset_path)
    facility_path = Path(facility_path)
    network_path = Path(network_path)
    analysis = arcpy.nax.ClosestFacility(network_path)
    analysis.defaultImpedanceCutoff = max_cost
    distance_units = UNIT_PLURAL[SpatialReference(dataset_path).linear_unit]
    analysis.distanceUnits = getattr(arcpy.nax.DistanceUnits, distance_units)
    analysis.ignoreInvalidLocations = True
    if travel_from_facility:
        analysis.travelDirection = arcpy.nax.TravelDirection.FromFacility
    # ArcPy2.8.0: Convert Path to str.
    analysis.travelMode = arcpy.nax.GetTravelModes(network_path)[travel_mode]
    # Load facilities.
    field = Field(
        facility_path,
        Dataset(facility_path).oid_field_name
        if facility_id_field_name.upper() == "OID@"
        else facility_id_field_name,
    )
    field_description = [
        "source_id",
        field.type if field.type != "OID" else "LONG",
        "#",
        field.length,
        "#",
        "#",
    ]
    analysis.addFields(
        arcpy.nax.ClosestFacilityInputDataType.Facilities, [field_description]
    )
    cursor = analysis.insertCursor(
        arcpy.nax.ClosestFacilityInputDataType.Facilities,
        field_names=["source_id", "SHAPE@"],
    )
    _features = features.as_tuples(
        facility_path,
        field_names=[facility_id_field_name, "SHAPE@"],
        dataset_where_sql=facility_where_sql,
    )
    with cursor:
        for feature in _features:
            cursor.insertRow(feature)
    # Load dataset locations.
    field = Field(
        dataset_path,
        Dataset(dataset_path).oid_field_name
        if id_field_name.upper() == "OID@"
        else id_field_name,
    )
    field_description = [
        "source_id",
        field.type if field.type != "OID" else "LONG",
        "#",
        field.length,
        "#",
        "#",
    ]
    analysis.addFields(
        arcpy.nax.ClosestFacilityInputDataType.Incidents, [field_description]
    )
    cursor = analysis.insertCursor(
        arcpy.nax.ClosestFacilityInputDataType.Incidents,
        field_names=["source_id", "SHAPE@"],
    )
    _features = features.as_tuples(
        dataset_path,
        field_names=[id_field_name, "SHAPE@"],
        dataset_where_sql=dataset_where_sql,
    )
    with cursor:
        for feature in _features:
            cursor.insertRow(feature)
    # Solve & generate.
    result = analysis.solve()
    if not result.solveSucceeded:
        for message in result.solverMessages(arcpy.nax.MessageSeverity.All):
            LOG.error(message)
        raise RuntimeError("Closest facility analysis failed")

    facility_oid_id = dict(
        result.searchCursor(
            output_type=getattr(arcpy.nax.ClosestFacilityOutputDataType, "Facilities"),
            field_names=["FacilityOID", "source_id"],
        )
    )
    location_oid_id = dict(
        result.searchCursor(
            output_type=getattr(arcpy.nax.ClosestFacilityOutputDataType, "Incidents"),
            field_names=["IncidentOID", "source_id"],
        )
    )
    keys = ["FacilityOID", "IncidentOID", f"Total_{distance_units}", "SHAPE@"]
    cursor = result.searchCursor(
        output_type=arcpy.nax.ClosestFacilityOutputDataType.Routes, field_names=keys
    )
    with cursor:
        for row in cursor:
            route = dict(zip(keys, row))
            yield {
                "dataset_id": location_oid_id[route["IncidentOID"]],
                "facility_id": facility_oid_id[route["FacilityOID"]],
                "cost": route[f"Total_{distance_units}"],
                "geometry": route["SHAPE@"],
            }


def generate_service_areas(
    dataset_path: Union[Path, str],
    *,
    id_field_name: str,
    network_path: Union[Path, str],
    dataset_where_sql: Optional[str] = None,
    output_path: Union[Path, str],
    cost_attribute: str,
    detailed_features: bool = False,
    max_distance: Union[float, int],
    overlap_facilities: bool = True,
    restriction_attributes: Optional[Iterable[str]] = None,
    travel_from_facility: bool = False,
    trim_value: Optional[Union[float, int]] = None,
    log_level: int = logging.INFO,
) -> Dataset:
    """Create network service area features.

    Args:
        dataset_path: Path to dataset.
        id_field_name: Name of dataset ID field.
        network_path: Path to network dataset.
        dataset_where_sql: SQL where-clause for dataset subselection.
        output_path: Path to output dataset.
        cost_attribute: Name of network cost attribute to use.
        detailed_features: Generate high-detail features if True.
        max_distance: Distance in travel from the facility the outer ring will extend
            to, in the units of the dataset.
        overlap_facilities: Allow different facility service areas to overlap if True.
        restriction_attributes: Names of network restriction attributes to use in
            analysis.
        travel_from_facility: Perform the analysis travelling from the facility if True,
            rather than toward the facility.
        trim_value: Disstance from network features to trim service areas at, in units
            of the dataset.
        log_level: Level to log the function at.

    Returns:
        Dataset metadata instance for output dataset.
    """
    dataset_path = Path(dataset_path)
    network_path = Path(network_path)
    output_path = Path(output_path)
    LOG.log(log_level, "Start: Generate service areas for `%s`.", dataset_path)
    # `trim_value` assumes meters if not input as linear unit string.
    if trim_value is not None:
        trim_value = f"{trim_value} {SpatialReference(dataset_path).linear_unit}"
    # ArcPy2.8.0: Convert Path to str.
    arcpy.na.MakeServiceAreaLayer(
        in_network_dataset=str(network_path),
        out_network_analysis_layer="service_area",
        impedance_attribute=cost_attribute,
        travel_from_to="TRAVEL_FROM" if travel_from_facility else "TRAVEL_TO",
        default_break_values=f"{max_distance}",
        polygon_type="DETAILED_POLYS" if detailed_features else "SIMPLE_POLYS",
        merge="NO_MERGE" if overlap_facilities else "NO_OVERLAP",
        nesting_type="DISKS",
        UTurn_policy="ALLOW_DEAD_ENDS_AND_INTERSECTIONS_ONLY",
        restriction_attribute_name=restriction_attributes,
        polygon_trim=trim_value is not None,
        poly_trim_value=trim_value,
        hierarchy="NO_HIERARCHY",
    )
    view = DatasetView(dataset_path, dataset_where_sql=dataset_where_sql)
    with view:
        arcpy.na.AddLocations(
            in_network_analysis_layer="service_area",
            sub_layer="Facilities",
            in_table=view.name,
            field_mappings=f"Name {id_field_name} #",
            search_tolerance=max_distance,
            match_type="MATCH_TO_CLOSEST",
            append="CLEAR",
            snap_to_position_along_network="NO_SNAP",
            exclude_restricted_elements=True,
        )
    arcpy.na.Solve(
        in_network_analysis_layer="service_area",
        ignore_invalids=True,
        terminate_on_solve_error=True,
    )
    dataset.copy(
        "service_area/Polygons", output_path=output_path, log_level=logging.DEBUG
    )
    arcpy.management.Delete("service_area")
    id_field = Field(dataset_path, id_field_name)
    dataset.add_field(output_path, log_level=logging.DEBUG, **id_field.field_as_dict)
    update_field_with_function(
        output_path,
        field_name=id_field.name,
        function=TYPE_ID_FUNCTION_MAP[id_field.type.lower()],
        field_as_first_arg=False,
        arg_field_names=["Name"],
        log_level=logging.DEBUG,
    )
    LOG.log(log_level, "End: Generate.")
    return Dataset(output_path)


def generate_service_rings(
    dataset_path: Union[Path, str],
    *,
    id_field_name: str,
    network_path: Union[Path, str],
    dataset_where_sql: Optional[str] = None,
    output_path: Union[Path, str],
    cost_attribute: str,
    detailed_features: bool = False,
    max_distance: Union[float, int],
    overlap_facilities: bool = True,
    restriction_attributes: Optional[Iterable[str]] = None,
    ring_width: Union[float, int],
    travel_from_facility: bool = False,
    trim_value: Optional[Union[float, int]] = None,
    log_level: int = logging.INFO,
) -> Dataset:
    """Create facility service ring features using a network dataset.

    Args:
        dataset_path: Path to dataset.
        id_field_name: Name of dataset ID field.
        network_path: Path to network dataset.
        dataset_where_sql: SQL where-clause for dataset subselection.
        output_path: Path to output dataset.
        cost_attribute: Name of network cost attribute to use.
        detailed_features: Generate high-detail features if True.
        max_distance: Distance in travel from the facility the outer ring will extend
            to, in the units of the dataset.
        overlap_facilities: Allow different facility service areas to overlap if True.
        restriction_attributes: Names of network restriction attributes to use in
            analysis.
        ring_width: Distance a service ring represents in travel, in the units of the
            dataset.
        travel_from_facility: Perform the analysis travelling from the facility if True,
            rather than toward the facility.
        trim_value: Disstance from network features to trim service areas at, in units
            of the dataset.
        log_level: Level to log the function at.

    Returns:
        Dataset metadata instance for output dataset.
    """
    dataset_path = Path(dataset_path)
    network_path = Path(network_path)
    output_path = Path(output_path)
    LOG.log(log_level, "Start: Generate service rings for `%s`.", dataset_path)
    # `trim_value` assumes meters if not input as linear unit string.
    if trim_value is not None:
        trim_value = f"{trim_value} {SpatialReference(dataset_path).linear_unit}"
    # ArcPy2.8.0: Convert Path to str.
    arcpy.na.MakeServiceAreaLayer(
        in_network_dataset=str(network_path),
        out_network_analysis_layer="service_area",
        impedance_attribute=cost_attribute,
        travel_from_to="TRAVEL_FROM" if travel_from_facility else "TRAVEL_TO",
        default_break_values=(
            " ".join(str(x) for x in range(ring_width, max_distance + 1, ring_width))
        ),
        polygon_type="DETAILED_POLYS" if detailed_features else "SIMPLE_POLYS",
        merge="NO_MERGE" if overlap_facilities else "NO_OVERLAP",
        nesting_type="RINGS",
        UTurn_policy="ALLOW_DEAD_ENDS_AND_INTERSECTIONS_ONLY",
        restriction_attribute_name=restriction_attributes,
        polygon_trim=trim_value is not None,
        poly_trim_value=trim_value,
        hierarchy="NO_HIERARCHY",
    )
    view = DatasetView(dataset_path, dataset_where_sql=dataset_where_sql)
    with view:
        arcpy.na.AddLocations(
            in_network_analysis_layer="service_area",
            sub_layer="Facilities",
            in_table=view.name,
            field_mappings=f"Name {id_field_name} #",
            search_tolerance=max_distance,
            match_type="MATCH_TO_CLOSEST",
            append="CLEAR",
            snap_to_position_along_network="NO_SNAP",
            exclude_restricted_elements=True,
        )
    arcpy.na.Solve(
        in_network_analysis_layer="service_area",
        ignore_invalids=True,
        terminate_on_solve_error=True,
    )
    dataset.copy(
        "service_area/Polygons", output_path=output_path, log_level=logging.DEBUG
    )
    arcpy.management.Delete("service_area")
    id_field = Field(dataset_path, id_field_name)
    dataset.add_field(output_path, log_level=logging.DEBUG, **id_field.field_as_dict)
    update_field_with_function(
        output_path,
        field_name=id_field.name,
        function=TYPE_ID_FUNCTION_MAP[id_field.type.lower()],
        field_as_first_arg=False,
        arg_field_names=["Name"],
        log_level=logging.DEBUG,
    )
    LOG.log(log_level, "End: Generate.")
    return Dataset(output_path)


# Node functions.


def _updated_coordinates_node_map(
    coordinates_node: Mapping[tuple, Mapping],
    node_id_data_type: Any,
    node_id_max_length: int,
) -> Dict[Tuple[float], Dict[str, Any]]:
    """Return updated mapping of coordinates pair to node info mapping.

    Args:
        coordinates_node: Mapping of coordinates tuple to node information dictionary.
        node_id_data_type: Value type for node ID.
        node_id_max_length: Maximum length for node ID, if ID data type is string.
    """

    def _feature_count(node: Mapping) -> int:
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
    *,
    from_id_field_name: str,
    to_id_field_name: str,
    id_field_names: Iterable[str] = ("OID@",),
    dataset_where_sql: Optional[str] = None,
    update_nodes: bool = False,
    spatial_reference_item: SpatialReferenceSourceItem = None,
) -> Dict[Tuple[float], Dict[str, Any]]:
    """Return mapping of coordinates to node info mapping for dataset.

    Notes:
        From- & to-node IDs must be same attribute type.
        Output format:
            `{(x, y): {"node_id": Any, "feature_ids": {"from": set, "to": set}}}`

    Args:
        dataset_path: Path to dataset.
        from_id_field_name: Name of from-node ID field.
        to_id_field_name: Name of to-node ID field.
        id_field_names: Names of the feature ID fields.
        dataset_where_sql: SQL where-clause for dataset subselection.
        update_nodes: Update nodes based on feature geometries if True.
        spatial_reference_item: Item from which the spatial reference for any geometry
            properties will be set to. If set to None, will use spatial reference of
            the dataset.

    Raises:
        ValueError: If from- & to-node ID fields are not the same type.
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
            raise ValueError("From- and to-node ID fields must be same type")

        if node_id_data_type == str:
            if not node_id_max_length or node_id_max_length > field.length:
                node_id_max_length = field.length
    coordinate_node = {}
    for feature in features.as_dicts(
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
    *,
    from_id_field_name: str,
    to_id_field_name: str,
    id_field_names: Iterable[str] = ("OID@",),
    dataset_where_sql: Optional[str] = None,
    update_nodes: bool = False,
) -> Dict[Tuple[Any], Dict[str, Any]]:
    """Return mapping of feature ID to from- & to-node ID dictionary.

    Notes:
        From- & to-node IDs must be same attribute type.
        Output format:
            `{feature_id: {"from": from_node_id, "to": to_node_id}}`

    Args:
        dataset_path: Path to dataset.
        from_id_field_name: Name of from-node ID field.
        to_id_field_name: Name of to-node ID field.
        id_field_names: Names of the feature ID fields.
        dataset_where_sql: SQL where-clause for dataset subselection.
        update_nodes: Update nodes based on feature geometries if True.
    """
    dataset_path = Path(dataset_path)
    id_field_names = list(id_field_names)
    id_node = {}
    if update_nodes:
        coordinate_node = coordinates_node_map(
            dataset_path,
            from_id_field_name=from_id_field_name,
            to_id_field_name=to_id_field_name,
            id_field_names=id_field_names,
            update_nodes=update_nodes,
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
        for feature in features.as_dicts(
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
    *,
    from_id_field_name: str,
    to_id_field_name: str,
    dataset_where_sql: Optional[str] = None,
    use_edit_session: bool = False,
    log_level: int = logging.INFO,
) -> Counter:
    """Update node ID values.

    Args:
        dataset_path: Path to the dataset.
        from_id_field_name: Name of from-node ID field.
        to_id_field_name: Name of to-node ID field.
        dataset_where_sql: SQL where-clause for dataset subselection.
        use_edit_session: True if edits are to be made in an edit session.
        log_level: Level to log the function at.

    Returns:
        Feature counts for each update-state.
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
        dataset_path,
        from_id_field_name=from_id_field_name,
        to_id_field_name=to_id_field_name,
        update_nodes=True,
    )
    session = Editing(Dataset(dataset_path).workspace_path, use_edit_session)
    states = Counter()
    with session, cursor:
        for old_feature in cursor:
            oid = old_feature[0]
            new_feature = (oid, oid_node[oid]["from"], oid_node[oid]["to"])
            if same_feature(old_feature, new_feature):
                states["unchanged"] += 1
            else:
                try:
                    cursor.updateRow(new_feature)
                    states["altered"] += 1
                except RuntimeError as error:
                    raise RuntimeError(
                        f"Row failed to update. Offending row: `{new_feature}`"
                    ) from error

    log_entity_states("attributes", states, logger=LOG, log_level=log_level)
    LOG.log(log_level, "End: Update.")
    return states
