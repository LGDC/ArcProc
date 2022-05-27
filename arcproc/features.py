"""Feature operations."""
import inspect
import logging
from collections import Counter
from itertools import chain
from pathlib import Path
from typing import Any, Iterable, Iterator, List, Optional, Sequence, Union

import arcpy
import pint

from arcproc import dataset
from arcproc.dataset import DatasetView
from arcproc.helpers import freeze_values, log_entity_states, same_feature, unique_name
from arcproc.metadata import Dataset, SpatialReference, SpatialReferenceSourceItem
from arcproc.workspace import Editing


LOG: logging.Logger = logging.getLogger(__name__)
"""Module-level logger."""

UNIT: pint.registry.UnitRegistry = pint.UnitRegistry()
"""Registry for units & conversions."""
UPDATE_TYPES: List[str] = ["deleted", "inserted", "altered", "unchanged"]
"""Types of feature updates commonly associated wtth update counters."""


arcpy.SetLogHistory(False)


def as_dicts(
    dataset_path: Union[Path, str],
    field_names: Optional[Iterable[str]] = None,
    *,
    dataset_where_sql: Optional[str] = None,
    spatial_reference_item: SpatialReferenceSourceItem = None,
) -> Iterator[dict]:
    """Generate dictionaries of feature attribute name to their value.

    Notes:
        Use ArcPy cursor token names for object IDs and geometry objects/properties.

    Args:
        dataset_path: Path to dataset.
        field_names: Names of fields to include in generated dictionary. Names will be
            the keys in the dictionary mapping to their attributes values. If set to
            None, all fields will be included.
        dataset_where_sql: SQL where-clause for dataset subselection.
        spatial_reference_item: Item from which the spatial reference for any geometry
            properties will be set to. If set to None, will use spatial reference of
            the dataset.
    """
    dataset_path = Path(dataset_path)
    if field_names:
        field_names = list(field_names)
    else:
        field_names = Dataset(dataset_path).field_names_tokenized
    cursor = arcpy.da.SearchCursor(
        # ArcPy2.8.0: Convert Path to str.
        in_table=str(dataset_path),
        field_names=field_names,
        where_clause=dataset_where_sql,
        spatial_reference=SpatialReference(spatial_reference_item).object,
    )
    with cursor:
        for feature in cursor:
            yield dict(zip(cursor.fields, feature))


def as_tuples(
    dataset_path: Union[Path, str],
    field_names: Iterable[str],
    *,
    dataset_where_sql: Optional[str] = None,
    spatial_reference_item: SpatialReferenceSourceItem = None,
) -> Iterator[tuple]:
    """Generate tuples of feature attribute values.

    Notes:
        Use ArcPy cursor token names for object IDs and geometry objects/properties.

    Args:
        dataset_path: Path to dataset.
        field_names: Names of fields to include in generated dictionary. Attributes will
            be in the tuple index that matches their field name here.
        dataset_where_sql: SQL where-clause for dataset subselection.
        spatial_reference_item: Item from which the spatial reference for any geometry
            properties will be set to. If set to None, will use spatial reference of
            the dataset.
    """
    field_names = list(field_names)
    dataset_path = Path(dataset_path)
    cursor = arcpy.da.SearchCursor(
        # ArcPy2.8.0: Convert Path to str.
        in_table=str(dataset_path),
        field_names=field_names,
        where_clause=dataset_where_sql,
        spatial_reference=SpatialReference(spatial_reference_item).object,
    )
    with cursor:
        yield from cursor


def delete(
    dataset_path: Union[Path, str],
    *,
    dataset_where_sql: Optional[str] = None,
    use_edit_session: bool = False,
    log_level: int = logging.INFO,
) -> Counter:
    """Delete features in dataset.

    Args:
        dataset_path: Path to dataset.
        dataset_where_sql: SQL where-clause for dataset subselection.
        use_edit_session: True if edits are to be made in an edit session.
        log_level: Level to log the function at.

    Returns:
        Feature counts for each delete-state.
    """
    dataset_path = Path(dataset_path)
    LOG.log(log_level, "Start: Delete features from `%s`.", dataset_path)
    session = Editing(Dataset(dataset_path).workspace_path, use_edit_session)
    states = Counter()
    view = DatasetView(dataset_path, dataset_where_sql=dataset_where_sql)
    with view, session:
        states["deleted"] = view.count
        arcpy.management.DeleteRows(in_rows=view.name)
        states["remaining"] = dataset.feature_count(dataset_path)
    log_entity_states("features", states, logger=LOG, log_level=log_level)
    LOG.log(log_level, "End: Delete.")
    return states


def delete_by_id(
    dataset_path: Union[Path, str],
    delete_ids: Iterable[Union[Sequence[Any], Any]],
    id_field_names: Iterable[str],
    use_edit_session: bool = False,
    log_level: int = logging.INFO,
) -> Counter:
    """Delete features in dataset with given IDs.

    Note:
        There is no guarantee that the ID field(s) are unique.
        Use ArcPy cursor token names for object IDs and geometry objects/properties.

    Args:
        dataset_path: Path to dataset.
        delete_ids: ID sequences for features to delete. If id_field_names contains only
            one field, IDs may be provided as non-sequence single-value.
        id_field_names: Names of the feature ID fields.
        use_edit_session: True if edits are to be made in an edit session.
        log_level: Level to log the function at.

    Returns:
        Feature counts for each delete-state.
    """
    dataset_path = Path(dataset_path)
    LOG.log(log_level, "Start: Delete features in `%s` with given IDs.", dataset_path)
    id_field_names = list(id_field_names)
    if inspect.isgeneratorfunction(delete_ids):
        delete_ids = delete_ids()
    ids = set()
    for _id in delete_ids:
        if isinstance(_id, Iterable) and not isinstance(_id, str):
            ids.add(tuple(_id))
        else:
            ids.add((_id,))
    states = Counter()
    if ids:
        # ArcPy2.8.0: Convert Path to str.
        cursor = arcpy.da.UpdateCursor(str(dataset_path), field_names=id_field_names)
        session = Editing(Dataset(dataset_path).workspace_path, use_edit_session)
        with session, cursor:
            for row in cursor:
                _id = tuple(row)
                if _id in ids:
                    cursor.deleteRow()
                    states["deleted"] += 1
    else:
        LOG.log(log_level, "No IDs provided.")
        states["deleted"] = 0
    states["unchanged"] = dataset.feature_count(dataset_path)
    log_entity_states("features", states, logger=LOG, log_level=log_level)
    LOG.log(log_level, "End: Delete.")
    return states


def densify(
    dataset_path: Union[Path, str],
    *,
    dataset_where_sql: Optional[str] = None,
    distance: Union[float, int],
    only_curve_features: bool = False,
    use_edit_session: bool = False,
    log_level: int = logging.INFO,
) -> Counter:
    """Add vertices at a given distance along feature geometry segments.

    Args:
        dataset_path: Path to dataset.
        dataset_where_sql: SQL where-clause for dataset subselection.
        distance: Interval at which to add vertices, in units of the dataset.
        only_curve_features: Only densify curve features if True.
        use_edit_session: True if edits are to be made in an edit session.
        log_level: Level to log the function at.

    Returns:
        Feature counts for each densify-state.
    """
    dataset_path = Path(dataset_path)
    LOG.log(log_level, "Start: Densify feature geometry in `%s`.", dataset_path)
    _dataset = Dataset(dataset_path)
    # Densify method on geometry object uses meters with geodesic densify method.
    if _dataset.spatial_reference.linear_unit != "Meter":
        distance_unit = getattr(UNIT, _dataset.spatial_reference.linear_unit.lower())
        distance_with_unit = (distance * distance_unit).to(UNIT.meter) / UNIT.meter
    cursor = arcpy.da.UpdateCursor(
        # ArcPy2.8.0: Convert Path to str.
        in_table=str(dataset_path),
        field_names=["SHAPE@"],
        where_clause=dataset_where_sql,
    )
    session = Editing(_dataset.workspace_path, use_edit_session)
    states = Counter()
    with session, cursor:
        for (old_geometry,) in cursor:
            if old_geometry:
                if only_curve_features and not old_geometry.hasCurves:
                    continue

                new_geometry = old_geometry.densify(
                    method="GEODESIC", distance=distance_with_unit
                )
                cursor.updateRow((new_geometry,))
                states["densified"] += 1
            else:
                states["unchanged"] += 1
    log_entity_states("features", states, logger=LOG, log_level=log_level)
    LOG.log(log_level, "End: Densify.")
    return states


def eliminate_interior_rings(
    dataset_path: Union[Path, str],
    *,
    dataset_where_sql: Optional[str] = None,
    use_edit_session: bool = False,
    log_level: int = logging.INFO,
) -> Counter:
    """Eliminate interior rings of polygon features.

    Args:
        dataset_path: Path to dataset.
        dataset_where_sql: SQL where-clause for dataset subselection.
        use_edit_session: True if edits are to be made in an edit session.
        log_level: Level to log the function at.

    Returns:
        Feature counts for each ring-eliminate-state.
    """
    dataset_path = Path(dataset_path)
    LOG.log(log_level, "Start: Eliminate interior rings in `%s`.", dataset_path)
    cursor = arcpy.da.UpdateCursor(
        # ArcPy2.8.0: Convert Path to str.
        in_table=str(dataset_path),
        field_names=["SHAPE@"],
        where_clause=dataset_where_sql,
    )
    _dataset = Dataset(dataset_path)
    session = Editing(_dataset.workspace_path, use_edit_session)
    states = Counter()
    with session, cursor:
        for (old_geometry,) in cursor:
            if not any(None in part for part in old_geometry):
                states["unchanged"] += 1
                continue

            parts = arcpy.Array()
            for old_part in old_geometry:
                if None not in old_part:
                    parts.append(old_part)
                else:
                    new_part = arcpy.Array()
                    for point in old_part:
                        if not point:
                            break

                        new_part.append(point)
                    parts.append(new_part)
            new_geometry = arcpy.Polygon(parts, _dataset.spatial_reference.object)
            cursor.updateRow([new_geometry])
            states["rings eliminated"] += 1
    log_entity_states("features", states, logger=LOG, log_level=log_level)
    LOG.log(log_level, "End: Eliminate.")
    return states


def insert_from_dicts(
    dataset_path: Union[Path, str],
    field_names: Iterable[str],
    *,
    source_features: Iterable[dict],
    use_edit_session: bool = False,
    log_level: int = logging.INFO,
) -> Counter:
    """Insert features into dataset from dictionaries.

    Args:
        dataset_path: Path to dataset.
        field_names: Names of fields for insert. Names must be present keys in
            `source_features` elements.
        source_features: Features to insert.
        use_edit_session: True if edits are to be made in an edit session.
        log_level: Level to log the function at.

    Returns:
        Feature counts for each insert-state.
    """
    dataset_path = Path(dataset_path)
    LOG.log(
        log_level, "Start: Insert features into `%s` from dictionaries.", dataset_path
    )
    field_names = list(field_names)
    if inspect.isgeneratorfunction(source_features):
        source_features = source_features()
    states = insert_from_iters(
        dataset_path,
        field_names,
        source_features=(
            (feature[field_name] for field_name in field_names)
            for feature in source_features
        ),
        use_edit_session=use_edit_session,
        log_level=logging.DEBUG,
    )
    log_entity_states("features", states, logger=LOG, log_level=log_level)
    LOG.log(log_level, "End: Insert.")
    return states


def insert_from_iters(
    dataset_path: Union[Path, str],
    field_names: Iterable[str],
    *,
    source_features: Iterable[Sequence[Any]],
    use_edit_session: bool = False,
    log_level: int = logging.INFO,
) -> Counter:
    """Insert features into dataset from sequences.

    Args:
        dataset_path: Path to dataset.
        field_names: Names of fields for insert. Names must be in the same order as
            their corresponding attributes in `source_features` elements.
        source_features: Features to insert.
        use_edit_session: True if edits are to be made in an edit session.
        log_level: Level to log the function at.

    Returns:
        Feature counts for each insert-state.
    """
    dataset_path = Path(dataset_path)
    LOG.log(log_level, "Start: Insert features into `%s` from sequences.", dataset_path)
    field_names = list(field_names)
    if inspect.isgeneratorfunction(source_features):
        source_features = source_features()
    # ArcPy2.8.0: Convert Path to str.
    cursor = arcpy.da.InsertCursor(in_table=str(dataset_path), field_names=field_names)
    session = Editing(Dataset(dataset_path).workspace_path, use_edit_session)
    states = Counter()
    with session, cursor:
        for row in source_features:
            cursor.insertRow(tuple(row))
            states["inserted"] += 1
    log_entity_states("features", states, logger=LOG, log_level=log_level)
    LOG.log(log_level, "End: Insert.")
    return states


def insert_from_path(
    dataset_path: Union[Path, str],
    field_names: Optional[Iterable[str]] = None,
    *,
    source_path: Union[Path, str],
    source_where_sql: Optional[str] = None,
    use_edit_session: bool = False,
    log_level: int = logging.INFO,
) -> Counter:
    """Insert features into dataset from another dataset.

    Args:
        dataset_path: Path to dataset.
        field_names: Names of fields for insert. Fields must exist in both datasets. If
            set to None, all user fields present in both datasets will be inserted,
            along with the geometry field (if present).
        source_path: Path to dataset for features to insert.
        source_where_sql: SQL where-clause for source dataset subselection.
        use_edit_session: True if edits are to be made in an edit session.
        log_level: Level to log the function at.

    Returns:
        Feature counts for each insert-state.
    """
    dataset_path = Path(dataset_path)
    source_path = Path(source_path)
    LOG.log(
        log_level,
        "Start: Insert features into `%s` from `%s`.",
        dataset_path,
        source_path,
    )
    _dataset = Dataset(dataset_path)
    source_dataset = Dataset(source_path)
    if field_names is None:
        field_names = set(
            name.lower() for name in _dataset.field_names_tokenized
        ) & set(name.lower() for name in source_dataset.field_names_tokenized)
    else:
        field_names = set(name.lower() for name in field_names)
    # OIDs & area/length "fields" have no business being part of an insert.
    # Geometry itself is handled separately in append function.
    for i_dataset in [_dataset, source_dataset]:
        for field_name in chain(*i_dataset.field_name_token.items()):
            field_names.discard(field_name)
            field_names.discard(field_name.lower())
            field_names.discard(field_name.upper())
    field_names = list(field_names)
    # Create field maps.
    # ArcGIS Pro's no-test append is case-sensitive (verified 1.0-1.1.1).
    # Avoid this problem by using field mapping.
    # BUG-000090970 - ArcGIS Pro 'No test' field mapping in Append tool does not auto-
    # map to the same field name if naming convention differs.
    field_mapping = arcpy.FieldMappings()
    for field_name in field_names:
        field_map = arcpy.FieldMap()
        field_map.addInputField(source_path, field_name)
        field_mapping.addFieldMap(field_map)
    session = Editing(_dataset.workspace_path, use_edit_session)
    states = Counter()
    view = DatasetView(
        source_path,
        name=unique_name("view"),
        dataset_where_sql=source_where_sql,
        # Must be nonspatial to append to nonspatial table.
        force_nonspatial=(not _dataset.is_spatial),
    )
    with view, session:
        arcpy.management.Append(
            inputs=view.name,
            # ArcPy2.8.0: Convert Path to str.
            target=str(dataset_path),
            schema_type="NO_TEST",
            field_mapping=field_mapping,
        )
        states["inserted"] = view.count
    log_entity_states("features", states, logger=LOG, log_level=log_level)
    LOG.log(log_level, "End: Insert.")
    return states


def keep_by_location(
    dataset_path: Union[Path, str],
    *,
    location_path: Union[Path, str],
    dataset_where_sql: Optional[str] = None,
    location_where_sql: Optional[str] = None,
    use_edit_session: bool = False,
    log_level: int = logging.INFO,
) -> Counter:
    """Keep features where geometry overlaps location-dataset geometry.

    Args:
        dataset_path: Path to dataset.
        location_path: Path to location-dataset.
        dataset_where_sql: SQL where-clause for dataset subselection.
        location_where_sql: SQL where-clause for location-dataset subselection.
        use_edit_session: True if edits are to be made in an edit session.
        log_level: Level to log the function at.

    Returns:
        Feature counts for each keep-state.
    """
    dataset_path = Path(dataset_path)
    location_path = Path(location_path)
    LOG.log(
        log_level,
        "Start: Keep features in `%s` where location overlaps `%s`.",
        dataset_path,
        location_path,
    )
    session = Editing(Dataset(dataset_path).workspace_path, use_edit_session)
    states = Counter()
    view = DatasetView(dataset_path, dataset_where_sql=dataset_where_sql)
    location_view = DatasetView(location_path, dataset_where_sql=location_where_sql)
    with session, view, location_view:
        arcpy.management.SelectLayerByLocation(
            in_layer=view.name,
            overlap_type="INTERSECT",
            select_features=location_view.name,
            selection_type="NEW_SELECTION",
        )
        arcpy.management.SelectLayerByLocation(
            in_layer=view.name, selection_type="SWITCH_SELECTION"
        )
        states["deleted"] = delete(view.name, log_level=logging.DEBUG)["deleted"]
    log_entity_states("features", states, logger=LOG, log_level=log_level)
    LOG.log(log_level, "End: Keep.")
    return states


def replace_feature_true_curves(
    dataset_path: Union[Path, str],
    *,
    dataset_where_sql: Optional[str] = None,
    max_deviation: Union[float, int],
    use_edit_session: bool = False,
    log_level: int = logging.INFO,
) -> Counter:
    """Replace true cuves with extra vertices in features.

    Args:
        dataset_path: Path to dataset.
        dataset_where_sql: SQL where-clause for dataset subselection.
        max_deviation: The maximum allowed distance of deviation from original curve.
        use_edit_session: True if edits are to be made in an edit session.
        log_level: Level to log the function at.

    Returns:
        Feature counts for each replace-state.
    """
    dataset_path = Path(dataset_path)
    LOG.log(
        log_level,
        "Start: Replace true curves for feature geometry in `%s`.",
        dataset_path,
    )
    _dataset = Dataset(dataset_path)
    cursor = arcpy.da.UpdateCursor(
        # ArcPy2.8.0: Convert Path to str.
        in_table=str(dataset_path),
        field_names=["SHAPE@"],
        where_clause=dataset_where_sql,
    )
    session = Editing(_dataset.workspace_path, use_edit_session)
    states = Counter()
    with session, cursor:
        for (old_geometry,) in cursor:
            if old_geometry:
                if not old_geometry.hasCurves:
                    states["unchanged"] += 1
                    continue

                # Using very large distance so most non-curve segments add no vertices.
                new_geometry = old_geometry.densify(
                    method="DISTANCE", distance=100_000_000, deviation=max_deviation
                )
                cursor.updateRow((new_geometry,))
                states["replaced"] += 1
            else:
                states["unchanged"] += 1
    log_entity_states("feature geometries", states, logger=LOG, log_level=log_level)
    LOG.log(log_level, "End: Replace.")
    return states


def update_from_dicts(
    dataset_path: Union[Path, str],
    field_names: Iterable[str],
    *,
    id_field_names: Iterable[str],
    source_features: Iterable[dict],
    delete_missing_features: bool = True,
    use_edit_session: bool = False,
    log_level: int = logging.INFO,
) -> Counter:
    """Update features in dataset from dictionaries.

    Note:
        There is no guarantee that the ID field(s) are unique.
        Use ArcPy cursor token names for object IDs and geometry objects/properties.

    Args:
        dataset_path: Path to dataset.
        field_names: Names of fields for update. Names must be present keys in
            `source_features` elements.
        id_field_names: Names of the feature ID fields. Names must be present keys in
            `source_features` elements.
        source_features: Features from which to source updates.
        delete_missing_features: True if update should delete features missing
            from `source_features`.
        use_edit_session: True if edits are to be made in an edit session.
        log_level: Level to log the function at.

    Returns:
        Feature counts for each update-state.
    """
    dataset_path = Path(dataset_path)
    LOG.log(
        log_level, "Start: Update features into `%s` from dictionaries.", dataset_path
    )
    field_names = list(field_names)
    id_field_names = list(id_field_names)
    if inspect.isgeneratorfunction(source_features):
        source_features = source_features()
    states = update_from_iters(
        dataset_path,
        field_names=id_field_names + field_names,
        id_field_names=id_field_names,
        source_features=(
            (feature[field_name] for field_name in id_field_names + field_names)
            for feature in source_features
        ),
        delete_missing_features=delete_missing_features,
        use_edit_session=use_edit_session,
        log_level=logging.DEBUG,
    )
    log_entity_states("features", states, logger=LOG, log_level=log_level)
    LOG.log(log_level, "End: Update.")
    return states


def update_from_iters(
    dataset_path: Union[Path, str],
    field_names: Iterable[str],
    *,
    id_field_names: Iterable[str],
    source_features: Iterable[Sequence[Any]],
    delete_missing_features: bool = True,
    use_edit_session: bool = False,
    log_level: int = logging.INFO,
) -> Counter:
    """Update features in dataset from sequences.

    Note:
        There is no guarantee that the ID field(s) are unique.
        Use ArcPy cursor token names for object IDs and geometry objects/properties.

    Args:
        dataset_path: Path to dataset.
        field_names: Names of fields for update. Names must be in the same order as
            their corresponding attributes in `source_features` elements.
        id_field_names: Names of the feature ID fields. All ID fields must also be in
            `field_names`.
        source_features: Features to insert.
        delete_missing_features: True if update should delete features missing
            from `source_features`.
        use_edit_session: True if edits are to be made in an edit session.
        log_level: Level to log the function at.

    Returns:
        Feature counts for each update-state.

    Raises:
        ValueError: When `id_field_names` is not a subset of `field_names`.
    """
    dataset_path = Path(dataset_path)
    LOG.log(log_level, "Start: Update features in `%s` from sequences.", dataset_path)
    field_names = list(field_names)
    id_field_names = list(id_field_names)
    if not set(id_field_names).issubset(field_names):
        raise ValueError("id_field_names must be a subset of field_names")

    if inspect.isgeneratorfunction(source_features):
        source_features = source_features()
    dataset_ids = {
        tuple(freeze_values(*_id)) for _id in as_tuples(dataset_path, id_field_names)
    }
    id_feature = {}
    insert_features = []
    for feature in source_features:
        feature = list(freeze_values(*feature))
        _id = tuple(
            feature[field_names.index(field_name)] for field_name in id_field_names
        )
        if _id not in dataset_ids:
            insert_features.append(feature)
        else:
            id_feature[_id] = feature
    if delete_missing_features:
        delete_ids = {_id for _id in dataset_ids if _id not in id_feature}
    else:
        delete_ids = set()
    session = Editing(Dataset(dataset_path).workspace_path, use_edit_session)
    states = Counter()
    if delete_ids or id_feature:
        # ArcPy2.8.0: Convert Path to str.
        cursor = arcpy.da.UpdateCursor(
            in_table=str(dataset_path), field_names=field_names
        )
        with session, cursor:
            for feature in cursor:
                _id = tuple(
                    freeze_values(
                        *(
                            feature[field_names.index(field_name)]
                            for field_name in id_field_names
                        )
                    )
                )
                if _id in delete_ids:
                    cursor.deleteRow()
                    states["deleted"] += 1
                elif _id in id_feature:
                    new_feature = id_feature.pop(_id)
                    if not same_feature(feature, new_feature):
                        try:
                            cursor.updateRow(new_feature)
                        except RuntimeError as error:
                            raise RuntimeError(
                                f"Row failed to update. Offending row: {new_feature}"
                            ) from error

                        states["altered"] += 1
                    else:
                        states["unchanged"] += 1
                else:
                    states["unchanged"] += 1
    if insert_features:
        cursor = arcpy.da.InsertCursor(
            # ArcPy2.8.0: Convert Path to str.
            in_table=str(dataset_path),
            field_names=field_names,
        )
        with session, cursor:
            for new_feature in insert_features:
                try:
                    cursor.insertRow(new_feature)
                except RuntimeError as error:
                    raise RuntimeError(
                        f"Row failed to insert. Offending row: {new_feature}"
                    ) from error

                states["inserted"] += 1
    log_entity_states("features", states, logger=LOG, log_level=log_level)
    LOG.log(log_level, "End: Update.")
    return states


def update_from_path(
    dataset_path: Union[Path, str],
    field_names: Optional[Iterable[str]] = None,
    *,
    id_field_names: Iterable[str],
    source_path: Union[Path, str],
    source_where_sql: Optional[str] = None,
    delete_missing_features: bool = True,
    use_edit_session: bool = False,
    log_level: int = logging.INFO,
) -> Counter:
    """Update features in dataset from another dataset.

    Args:
        dataset_path: Path to dataset.
        field_names: Names of fields for update. Fields must exist in both datasets. If
            set to None, all user fields present in both datasets will be updated,
            along with the geometry field (if present).
        id_field_names: Names of the feature ID fields. Fields must exist in both
            datasets.
        source_path: Path to dataset for features from which to update.
        source_where_sql: SQL where-clause for source dataset subselection.
        delete_missing_features: True if update should delete features missing from
            source dataset.
        use_edit_session: True if edits are to be made in an edit session.
        log_level: Level to log the function at.

    Returns:
        Feature counts for each update-state.
    """
    dataset_path = Path(dataset_path)
    source_path = Path(source_path)
    LOG.log(
        log_level,
        "Start: Update features in `%s` from `%s`.",
        dataset_path,
        source_path,
    )
    _dataset = Dataset(dataset_path)
    source_dataset = Dataset(source_path)
    if field_names is None:
        field_names = set(
            name.lower() for name in _dataset.field_names_tokenized
        ) & set(name.lower() for name in source_dataset.field_names_tokenized)
    else:
        field_names = set(name.lower() for name in field_names)
    # OIDs & area/length "fields" have no business being part of an update.
    for field_token in ["OID@", "SHAPE@AREA", "SHAPE@LENGTH"]:
        field_names.discard(field_token.lower())
    field_names = list(field_names)
    id_field_names = list(id_field_names)
    source_features = as_tuples(
        source_path,
        field_names=id_field_names + field_names,
        dataset_where_sql=source_where_sql,
    )
    states = update_from_iters(
        dataset_path,
        field_names=id_field_names + field_names,
        id_field_names=id_field_names,
        source_features=source_features,
        delete_missing_features=delete_missing_features,
        use_edit_session=use_edit_session,
        log_level=logging.DEBUG,
    )
    log_entity_states("features", states, logger=LOG, log_level=log_level)
    LOG.log(log_level, "End: Update.")
    return states
