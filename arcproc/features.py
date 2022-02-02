"""Feature operations."""
from collections import Counter
import inspect
from itertools import chain
import logging
from pathlib import Path
from typing import Optional, Union

import pint

import arcpy

from arcproc import arcobj
from arcproc import attributes
from arcproc import dataset
from arcproc.helpers import (
    contain,
    freeze_values,
    log_entity_states,
    same_feature,
    unique_name,
    unique_path,
)


LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""

arcpy.SetLogHistory(False)

UNIT = pint.UnitRegistry()
"""pint.registry.UnitRegistry: Registry for units & conversions."""
UPDATE_TYPES = ["deleted", "inserted", "altered", "unchanged"]
"""list of str: Types of feature updates commonly associated wtth update counters."""


def delete(
    dataset_path: Union[Path, str],
    *,
    dataset_where_sql: Optional[str] = None,
    use_edit_session: bool = False,
    log_level: int = logging.INFO,
) -> Counter:
    """Delete features in the dataset.

    Args:
        dataset_path (pathlib.Path, str): Path of the dataset.
        dataset_where_sql: SQL where-clause for dataset subselection.
        use_edit_session: Updates are done in an edit session if True.
        log_level: Level to log the function at.

    Returns:
        Counts of features for each delete-state.
    """
    dataset_path = Path(dataset_path)
    LOG.log(log_level, "Start: Delete features from `%s`.", dataset_path)
    session = arcobj.Editor(
        arcobj.dataset_metadata(dataset_path)["workspace_path"],
        use_edit_session=use_edit_session,
    )
    states = Counter()
    view = arcobj.DatasetView(dataset_path, dataset_where_sql=dataset_where_sql)
    with view, session:
        states["deleted"] = view.count
        arcpy.management.DeleteRows(in_rows=view.name)
    log_entity_states("features", states, LOG, log_level=log_level)
    LOG.log(log_level, "End: Delete.")
    return states


def delete_by_id(dataset_path, delete_ids, id_field_names, **kwargs):
    """Delete features in dataset with given IDs.

    Note:
        There is no guarantee that the ID field(s) are unique.
        Use ArcPy cursor token names for object IDs and geometry objects/properties.

    Args:
        dataset_path (pathlib.Path, str): Path of the dataset.
        delete_ids (iter): Collection of feature IDs.
        id_field_names (iter, str): Name(s) of the ID field/key(s).
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        collections.Counter: Counts of features for each delete-state.
    """
    dataset_path = Path(dataset_path)
    if inspect.isgeneratorfunction(delete_ids):
        delete_ids = delete_ids()
    delete_ids = {tuple(contain(_id)) for _id in delete_ids}
    id_field_names = list(contain(id_field_names))
    kwargs.setdefault("use_edit_session", False)
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(level, "Start: Delete features in `%s` with given IDs.", dataset_path)
    states = Counter()
    if delete_ids:
        # ArcPy2.8.0: Convert to str.
        cursor = arcpy.da.UpdateCursor(str(dataset_path), field_names=id_field_names)
        session = arcobj.Editor(
            arcobj.dataset_metadata(dataset_path)["workspace_path"],
            kwargs["use_edit_session"],
        )
        with session, cursor:
            for row in cursor:
                _id = tuple(row)
                if _id in delete_ids:
                    cursor.deleteRow()
                    states["deleted"] += 1
    states["unchanged"] = dataset.feature_count(dataset_path)
    log_entity_states("features", states, LOG, log_level=level)
    LOG.log(level, "End: Delete.")
    return states


def densify(dataset_path, distance, only_curve_features=False, **kwargs):
    """Add vertices at a given distance along feature geometry segments.

    Args:
        dataset_path (pathlib.Path, str): Path of the dataset.
        distance (float): Interval to add vertices, in the units of the dataset.
        only_curve_features (bool): Only densfiy curve features if True.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        collections.Counter: Counts of features for each replace-state.
    """
    dataset_path = Path(dataset_path)
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("use_edit_session", False)
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(level, "Start: Densify feature geometry in `%s`.", dataset_path)
    dataset_meta = arcobj.dataset_metadata(dataset_path)
    if dataset_meta["spatial_reference"].linearUnitName != "Meter":
        distance_unit = getattr(
            UNIT, dataset_meta["spatial_reference"].linearUnitName.lower()
        )
        distance_with_unit = (distance * distance_unit).to(UNIT.meter) / UNIT.meter
    cursor = arcpy.da.UpdateCursor(
        # ArcPy2.8.0: Convert to str.
        in_table=str(dataset_path),
        field_names=["SHAPE@"],
        where_clause=kwargs["dataset_where_sql"],
    )
    session = arcobj.Editor(dataset_meta["workspace_path"], kwargs["use_edit_session"])
    states = Counter()
    with session, cursor:
        for (geometry,) in cursor:
            if geometry:
                if only_curve_features and not geometry.hasCurves:
                    continue

                new_geometry = geometry.densify(
                    method="GEODESIC", distance=distance_with_unit
                )
                cursor.updateRow((new_geometry,))
                states["densified"] += 1
            else:
                states["unchanged"] += 1
    log_entity_states("features", states, LOG, log_level=level)
    LOG.log(level, "End: Densify.")
    return states


def dissolve(dataset_path, dissolve_field_names=None, multipart=True, **kwargs):
    """Dissolve geometry of features that share values in given fields.

    Args:
        dataset_path (pathlib.Path, str): Path of the dataset.
        dissolve_field_names (iter): Iterable of field names to dissolve on.
        multipart (bool): Flag to allow multipart features in output.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        unsplit_lines (bool): Flag to merge line features when endpoints meet without
            crossing features. Default is False.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        tolerance (float): Tolerance for coincidence, in dataset's units.
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        pathlib.Path: Path of the dataset updated.
    """
    dataset_path = Path(dataset_path)
    dissolve_field_names = list(contain(dissolve_field_names))
    kwargs.setdefault("unsplit_lines", False)
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("use_edit_session", False)
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level,
        "Start: Dissolve features in `%s` on fields `%s`.",
        dataset_path,
        dissolve_field_names,
    )
    original_tolerance = arcpy.env.XYTolerance
    view = arcobj.DatasetView(dataset_path, kwargs["dataset_where_sql"])
    temp_output_path = unique_path("output")
    with view:
        if "tolerance" in kwargs:
            arcpy.env.XYTolerance = kwargs["tolerance"]
        arcpy.management.Dissolve(
            in_features=view.name,
            # ArcPy2.8.0: Convert to str.
            out_feature_class=str(temp_output_path),
            dissolve_field=dissolve_field_names,
            multi_part=multipart,
            unsplit_lines=kwargs["unsplit_lines"],
        )
        if "tolerance" in kwargs:
            arcpy.env.XYTolerance = original_tolerance
    session = arcobj.Editor(
        arcobj.dataset_metadata(dataset_path)["workspace_path"],
        kwargs["use_edit_session"],
    )
    with session:
        delete(
            dataset_path,
            dataset_where_sql=kwargs["dataset_where_sql"],
            log_level=logging.DEBUG,
        )
        insert_from_path(
            dataset_path, insert_dataset_path=temp_output_path, log_level=logging.DEBUG
        )
    dataset.delete(temp_output_path, log_level=logging.DEBUG)
    LOG.log(level, "End: Dissolve.")
    return dataset_path


def eliminate_interior_rings(
    dataset_path, max_area=None, max_percent_total_area=None, **kwargs
):
    """Eliminate interior rings of polygon features.

    Note:
        If no value if provided for either max_area or max_percent_total_area, (nearly)
        all interior rings will be removed. Technically, max_percent_total_area will be
        set to 99.9999.

    Args:
        dataset_path (pathlib.Path, str): Path of the dataset.
        max_area (float, str): Maximum area which parts smaller than are eliminated.
            Numeric area will be in dataset's units. String area will be formatted as
            '{number} {unit}'.
        max_percent_total_area (float): Maximum percent of total area which parts
            smaller than are eliminated.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        pathlib.Path: Path of the dataset updated.
    """
    dataset_path = Path(dataset_path)
    # Only set max_percent_total_area default if neither it or area defined.
    if all([max_area is None, max_percent_total_area is None]):
        max_percent_total_area = 99.9999
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("use_edit_session", False)
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(level, "Start: Eliminate interior rings in `%s`.", dataset_path)
    if all([max_area is not None, max_percent_total_area is not None]):
        condition = "AREA_OR_PERCENT"
    elif max_area is not None:
        condition = "AREA"
    else:
        condition = "PERCENT"
    view = arcobj.DatasetView(dataset_path, kwargs["dataset_where_sql"])
    temp_output_path = unique_path("output")
    with view:
        arcpy.management.EliminatePolygonPart(
            in_features=view.name,
            # ArcPy2.8.0: Convert to str.
            out_feature_class=str(temp_output_path),
            condition=condition,
            part_area=max_area,
            part_area_percent=max_percent_total_area,
            part_option="CONTAINED_ONLY",
        )
    session = arcobj.Editor(
        arcobj.dataset_metadata(dataset_path)["workspace_path"],
        kwargs["use_edit_session"],
    )
    with session:
        delete(
            dataset_path,
            dataset_where_sql=kwargs["dataset_where_sql"],
            log_level=logging.DEBUG,
        )
        insert_from_path(
            dataset_path, insert_dataset_path=temp_output_path, log_level=logging.DEBUG
        )
    dataset.delete(temp_output_path, log_level=logging.DEBUG)
    LOG.log(level, "End: Eliminate.")
    return dataset_path


def erase(dataset_path, erase_dataset_path, **kwargs):
    """Erase feature geometry where it overlaps erase-dataset geometry.

    Args:
        dataset_path (pathlib.Path, str): Path of the dataset.
        erase_dataset_path (pathlib.Path, str): Path of the dataset defining the erase-
            area.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        erase_where_sql (str): SQL where-clause for erase-dataset subselection.
        tolerance (float): Tolerance for coincidence, in dataset's units.
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        pathlib.Path: Path of the dataset updated.
    """
    dataset_path = Path(dataset_path)
    erase_dataset_path = Path(erase_dataset_path)
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("erase_where_sql")
    kwargs.setdefault("tolerance")
    kwargs.setdefault("use_edit_session", False)
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level,
        "Start: Erase features in `%s` where overlapping `%s`.",
        dataset_path,
        erase_dataset_path,
    )
    view = {
        "dataset": arcobj.DatasetView(dataset_path, kwargs["dataset_where_sql"]),
        "erase": arcobj.DatasetView(erase_dataset_path, kwargs["erase_where_sql"]),
    }
    temp_output_path = unique_path("output")
    with view["dataset"], view["erase"]:
        arcpy.analysis.Erase(
            in_features=view["dataset"].name,
            erase_features=view["erase"].name,
            # ArcPy2.8.0: Convert to str.
            out_feature_class=str(temp_output_path),
            cluster_tolerance=kwargs["tolerance"],
        )
    session = arcobj.Editor(
        arcobj.dataset_metadata(dataset_path)["workspace_path"],
        kwargs["use_edit_session"],
    )
    with session:
        delete(
            dataset_path,
            dataset_where_sql=kwargs["dataset_where_sql"],
            log_level=logging.DEBUG,
        )
        insert_from_path(
            dataset_path, insert_dataset_path=temp_output_path, log_level=logging.DEBUG
        )
    dataset.delete(temp_output_path, log_level=logging.DEBUG)
    LOG.log(level, "End: Erase.")
    return dataset_path


def insert_from_dicts(dataset_path, insert_features, field_names, **kwargs):
    """Insert features into dataset from dictionaries.

    Args:
        dataset_path (pathlib.Path, str): Path of the dataset.
        insert_features (iter of dict): Collection of dictionaries representing
            features.
        field_names (iter): Collection of field names/keys to insert.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        collections.Counter: Counts of features for each insert-state.
    """
    dataset_path = Path(dataset_path)
    field_names = list(contain(field_names))
    kwargs.setdefault("use_edit_session", False)
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(level, "Start: Insert features into `%s` from dictionaries.", dataset_path)
    if inspect.isgeneratorfunction(insert_features):
        insert_features = insert_features()
    insert_features = (
        (feature[field_name] for field_name in field_names)
        for feature in insert_features
    )
    states = insert_from_iters(
        dataset_path,
        insert_features,
        field_names,
        use_edit_session=kwargs["use_edit_session"],
        log_level=logging.DEBUG,
    )
    log_entity_states("features", states, LOG, log_level=level)
    LOG.log(level, "End: Insert.")
    return states


def insert_from_iters(dataset_path, insert_features, field_names, **kwargs):
    """Insert features into dataset from iterables.

    Args:
        dataset_path (pathlib.Path, str): Path of the dataset.
        insert_features (iter of iter): Collection of iterables representing features.
        field_names (iter): Collection of field names to insert. These must match the
            order of their attributes in the insert_features items.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        collections.Counter: Counts of features for each insert-state.
    """
    dataset_path = Path(dataset_path)
    field_names = list(contain(field_names))
    kwargs.setdefault("use_edit_session", False)
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(level, "Start: Insert features into `%s` from iterables.", dataset_path)
    if inspect.isgeneratorfunction(insert_features):
        insert_features = insert_features()
    # ArcPy2.8.0: Convert to str.
    cursor = arcpy.da.InsertCursor(in_table=str(dataset_path), field_names=field_names)
    session = arcobj.Editor(
        arcobj.dataset_metadata(dataset_path)["workspace_path"],
        kwargs["use_edit_session"],
    )
    states = Counter()
    with session, cursor:
        for row in insert_features:
            cursor.insertRow(tuple(row))
            states["inserted"] += 1
    log_entity_states("features", states, LOG, log_level=level)
    LOG.log(level, "End: Insert.")
    return states


def insert_from_path(dataset_path, insert_dataset_path, field_names=None, **kwargs):
    """Insert features into dataset from another dataset.

    Args:
        dataset_path (pathlib.Path, str): Path of the dataset.
        insert_dataset_path (pathlib.Path, str): Path of dataset to insert features
            from.
        field_names (iter): Collection of field names to insert. Listed field must be
            present in both datasets. If field_names is None, all fields will be
            inserted.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        insert_where_sql (str): SQL where-clause for insert-dataset subselection.
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        collections.Counter: Counts of features for each insert-state.
    """
    dataset_path = Path(dataset_path)
    insert_dataset_path = Path(insert_dataset_path)
    kwargs.setdefault("insert_where_sql")
    kwargs.setdefault("use_edit_session", False)
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level,
        "Start: Insert features into `%s` from `%s`.",
        dataset_path,
        insert_dataset_path,
    )
    dataset_meta = {
        "dataset": arcobj.dataset_metadata(dataset_path),
        "insert_dataset": arcobj.dataset_metadata(insert_dataset_path),
    }
    if field_names is None:
        field_names = set.intersection(
            *(
                set(name.lower() for name in _meta["field_names_tokenized"])
                for _meta in dataset_meta.values()
            )
        )
    else:
        field_names = set(name.lower() for name in contain(field_names))
    # OIDs & area/length "fields" have no business being part of an insert.
    # Geometry itself is handled separately in append function.
    for _meta in dataset_meta.values():
        for field_name in chain(*_meta["field_token"].items()):
            field_names.discard(field_name)
            field_names.discard(field_name.lower())
            field_names.discard(field_name.upper())
    field_names = list(field_names)
    field_mapping = arcpy.FieldMappings()
    # Create field maps.
    # ArcGIS Pro's no-test append is case-sensitive (verified 1.0-1.1.1).
    # Avoid this problem by using field mapping.
    # BUG-000090970 - ArcGIS Pro 'No test' field mapping in Append tool does not auto-
    # map to the same field name if naming convention differs.
    for field_name in field_names:
        field_map = arcpy.FieldMap()
        field_map.addInputField(insert_dataset_path, field_name)
        field_mapping.addFieldMap(field_map)
    view = arcobj.DatasetView(
        insert_dataset_path,
        dataset_where_sql=kwargs["insert_where_sql"],
        view_name=unique_name("view"),
        # Must be nonspatial to append to nonspatial table.
        force_nonspatial=(not dataset_meta["dataset"]["is_spatial"]),
    )
    session = arcobj.Editor(
        dataset_meta["dataset"]["workspace_path"], kwargs["use_edit_session"]
    )
    with view, session:
        arcpy.management.Append(
            inputs=view.name,
            # ArcPy2.8.0: Convert to str.
            target=str(dataset_path),
            schema_type="NO_TEST",
            field_mapping=field_mapping,
        )
        states = Counter(inserted=view.count)
    log_entity_states("features", states, LOG, log_level=level)
    LOG.log(level, "End: Insert.")
    return states


def keep_by_location(dataset_path, location_dataset_path, **kwargs):
    """Keep features where geometry overlaps location-dataset geometry.

    Args:
        dataset_path (pathlib.Path, str): Path of the dataset.
        location_dataset_path (pathlib.Path, str): Path of location-dataset.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        location_where_sql (str): SQL where-clause for location-dataset subselection.
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        collections.Counter: Counts of features for each keep-state.
    """
    dataset_path = Path(dataset_path)
    location_dataset_path = Path(location_dataset_path)
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("location_where_sql")
    kwargs.setdefault("use_edit_session", False)
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level,
        "Start: Keep features in `%s` where location overlaps `%s`.",
        dataset_path,
        location_dataset_path,
    )
    session = arcobj.Editor(
        arcobj.dataset_metadata(dataset_path)["workspace_path"],
        kwargs["use_edit_session"],
    )
    view = {
        "dataset": arcobj.DatasetView(dataset_path, kwargs["dataset_where_sql"]),
        "location": arcobj.DatasetView(
            location_dataset_path, kwargs["location_where_sql"]
        ),
    }
    with session, view["dataset"], view["location"]:
        arcpy.management.SelectLayerByLocation(
            in_layer=view["dataset"].name,
            overlap_type="INTERSECT",
            select_features=view["location"].name,
            selection_type="NEW_SELECTION",
        )
        arcpy.management.SelectLayerByLocation(
            in_layer=view["dataset"].name, selection_type="SWITCH_SELECTION"
        )
        states = delete(view["dataset"].name, log_level=logging.DEBUG)
    log_entity_states("features", states, LOG, log_level=level)
    LOG.log(level, "End: Keep.")
    return states


def update_from_dicts(
    dataset_path, update_features, id_field_names, field_names, **kwargs
):
    """Update features in dataset from dictionaries.

    Note:
        There is no guarantee that the ID field(s) are unique.
        Use ArcPy cursor token names for object IDs and geometry objects/properties.

    Args:
        dataset_path (pathlib.Path, str): Path of the dataset.
        update_features (iter of dict): Collection of dictionaries representing
            features.
        id_field_names (iter, str): Name(s) of the ID field/key(s).
        field_names (iter): Collection of field names/keys to check & update.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        delete_missing_features (bool): True if update should delete features missing
            from update_features, False otherwise. Default is True.
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        collections.Counter: Counts of features for each update-state.
    """
    dataset_path = Path(dataset_path)
    id_field_names = list(contain(id_field_names))
    field_names = list(contain(field_names))
    kwargs.setdefault("delete_missing_features", True)
    kwargs.setdefault("use_edit_session", False)
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(level, "Start: Update features in `%s` from dictionaries.", dataset_path)
    if inspect.isgeneratorfunction(update_features):
        update_features = update_features()
    update_features = (
        (feature[field_name] for field_name in id_field_names + field_names)
        for feature in update_features
    )
    states = update_from_iters(
        dataset_path,
        update_features,
        id_field_names,
        field_names=id_field_names + field_names,
        delete_missing_features=kwargs["delete_missing_features"],
        use_edit_session=kwargs["use_edit_session"],
        log_level=logging.DEBUG,
    )
    log_entity_states("features", states, LOG, log_level=level)
    LOG.log(level, "End: Update.")
    return states


def update_from_iters(
    dataset_path, update_features, id_field_names, field_names, **kwargs
):
    """Update features in dataset from iterables.

    Note:
        There is no guarantee that the ID field(s) are unique.
        Use ArcPy cursor token names for object IDs and geometry objects/properties.

    Args:
        dataset_path (pathlib.Path, str): Path of the dataset.
        update_features (iter of dict): Collection of iterables representing features.
        id_field_names (iter, str): Name(s) of the ID field/key(s). *All* ID fields
            must also be in field_names.
        field_names (iter): Collection of field names/keys to check & update.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        delete_missing_features (bool): True if update should delete features missing
            from update_features, False otherwise. Default is True.
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        collections.Counter: Counts of features for each update-state.
    """
    dataset_path = Path(dataset_path)
    id_field_names = list(contain(id_field_names))
    field_names = list(contain(field_names))
    kwargs.setdefault("delete_missing_features", True)
    kwargs.setdefault("use_edit_session", False)
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(level, "Start: Update features in `%s` from iterables.", dataset_path)
    if not set(id_field_names).issubset(field_names):
        raise ValueError("id_field_names must be a subset of field_names")

    ids = {
        "dataset": {
            tuple(freeze_values(*_id))
            for _id in attributes.as_tuples(dataset_path, id_field_names)
        }
    }
    if inspect.isgeneratorfunction(update_features):
        update_features = update_features()
    feats = {"insert": [], "id_update": dict()}
    for feat in update_features:
        feat = list(freeze_values(*feat))
        _id = tuple(
            feat[field_names.index(field_name)] for field_name in id_field_names
        )
        if _id not in ids["dataset"]:
            feats["insert"].append(feat)
        else:
            feats["id_update"][_id] = feat
    if kwargs["delete_missing_features"]:
        ids["delete"] = {_id for _id in ids["dataset"] if _id not in feats["id_update"]}
    else:
        ids["delete"] = set()
    session = arcobj.Editor(
        arcobj.dataset_metadata(dataset_path)["workspace_path"],
        kwargs["use_edit_session"],
    )
    states = Counter()
    if ids["delete"] or feats["id_update"]:
        cursor = arcpy.da.UpdateCursor(
            # ArcPy2.8.0: Convert to str.
            in_table=str(dataset_path),
            field_names=field_names,
        )
        with session, cursor:
            for feat in cursor:
                _id = tuple(
                    freeze_values(
                        *(
                            feat[field_names.index(field_name)]
                            for field_name in id_field_names
                        )
                    )
                )
                if _id in ids["delete"]:
                    cursor.deleteRow()
                    states["deleted"] += 1
                elif _id in feats["id_update"]:
                    new_feat = feats["id_update"].pop(_id)
                    if not same_feature(feat, new_feat):
                        try:
                            cursor.updateRow(new_feat)
                        except RuntimeError:
                            LOG.error(
                                "Row failed to update. Offending row: `%r`)",
                                dict(zip(cursor.fields, new_feat)),
                            )
                            raise

                        states["altered"] += 1
                    else:
                        states["unchanged"] += 1
                else:
                    states["unchanged"] += 1
    if feats["insert"]:
        cursor = arcpy.da.InsertCursor(
            # ArcPy2.8.0: Convert to str.
            in_table=str(dataset_path),
            field_names=field_names,
        )
        with session, cursor:
            for new_feat in feats["insert"]:
                try:
                    cursor.insertRow(new_feat)
                except RuntimeError:
                    LOG.error("Feature failed to write to cursor. Offending row:")
                    for field_name, val in zip(field_names, new_feat):
                        LOG.error("%s: %s", field_name, val)
                    raise

                states["inserted"] += 1
    log_entity_states("features", states, LOG, log_level=level)
    LOG.log(level, "End: Update.")
    return states


def update_from_path(
    dataset_path, update_dataset_path, id_field_names, field_names=None, **kwargs
):
    """Update features in dataset from another dataset.

    Args:
        dataset_path (pathlib.Path, str): Path of the dataset.
        update_dataset_path (pathlib.Path, str): Path of dataset to update features
            from.
        id_field_names (iter, str): Name(s) of the ID field/key(s).
        field_names (iter): Collection of field names/keys to check & update. Listed
            field must be present in both datasets. If field_names is None, all fields
            will be inserted.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection. WARNING:
            defining this has major effects: filtered features will not be considered
            for updating or deletion, and duplicates in the update features will be
            inserted as if novel.
        update_where_sql (str): SQL where-clause for update-dataset subselection.
        delete_missing_features (bool): True if update should delete features missing
            from update_dataset_path, False otherwise. Default is True.
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        collections.Counter: Counts of features for each update-state.
    """
    dataset_path = Path(dataset_path)
    update_dataset_path = Path(update_dataset_path)
    id_field_names = list(contain(id_field_names))
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("update_where_sql")
    kwargs.setdefault("delete_missing_features", True)
    kwargs.setdefault("use_edit_session", False)
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level,
        "Start: Update features in `%s` from `%s`.",
        dataset_path,
        update_dataset_path,
    )
    dataset_meta = {
        "dataset": arcobj.dataset_metadata(dataset_path),
        "update_dataset": arcobj.dataset_metadata(update_dataset_path),
    }
    if field_names is None:
        field_names = set(
            name.lower() for name in dataset_meta["dataset"]["field_names_tokenized"]
        ) & set(
            name.lower()
            for name in dataset_meta["update_dataset"]["field_names_tokenized"]
        )
    else:
        field_names = set(name.lower() for name in list(contain(field_names)))
    # OIDs & area/length "fields" have no business being part of an update.
    for field_token in ["OID@", "SHAPE@AREA", "SHAPE@LENGTH"]:
        field_names.discard(field_token.lower())
    field_names = list(field_names)
    states = Counter()
    update_features = attributes.as_tuples(
        update_dataset_path,
        field_names=id_field_names + field_names,
        dataset_where_sql=kwargs["update_where_sql"],
    )
    view = arcobj.DatasetView(
        dataset_path,
        dataset_where_sql=kwargs["dataset_where_sql"],
        field_names=id_field_names + field_names,
    )
    with view:
        states.update(
            update_from_iters(
                dataset_path=view.name,
                update_features=update_features,
                id_field_names=id_field_names,
                field_names=id_field_names + field_names,
                delete_missing_features=kwargs["delete_missing_features"],
                use_edit_session=kwargs["use_edit_session"],
                log_level=logging.DEBUG,
            )
        )
    log_entity_states("features", states, LOG, log_level=level)
    LOG.log(level, "End: Update.")
    return states
