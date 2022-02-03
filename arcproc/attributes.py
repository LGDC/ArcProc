"""Attribute operations."""
from collections import Counter, defaultdict
from functools import partial
import logging
from operator import itemgetter
from pathlib import Path
from types import BuiltinFunctionType, BuiltinMethodType, FunctionType, MethodType
from typing import Any, Iterable, Iterator, Optional, Union

import arcpy

from arcproc.arcobj import (
    DatasetView,
    Editor,
    python_type,
)
from arcproc import dataset
from arcproc.helpers import (
    contain,
    log_entity_states,
    same_value,
    unique_ids,
    unique_path,
)
from arcproc.metadata import Dataset, Domain, Field, SpatialReference


LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""

arcpy.SetLogHistory(False)


EXEC_TYPES = [BuiltinFunctionType, BuiltinMethodType, FunctionType, MethodType, partial]
"""list: Executable object types. Useful for determining if an object can execute."""


class FeatureMatcher:
    """Tracks features that share ID values.

    Attributes:
        assigned (collections.Counter): Running count of features that have been
            assigned each ID value. Useful for stepping through in a specific order.
        matched (collections.Counter): Count of how many features match each ID value.
    """

    def __init__(self, dataset_path, id_field_names, dataset_where_sql=None):
        """Initialize instance.

        Args:
            dataset_path (pathlib.Path, str): Path of the dataset.
            id_field_names (iter): Field names used to identify a feature.
            dataset_where_sql (str): SQL where-clause for dataset subselection. Default
                is None.
        """
        self.assigned = Counter()
        self.matched = Counter(
            as_tuples(dataset_path, id_field_names, dataset_where_sql=dataset_where_sql)
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


def as_dicts(dataset_path, field_names=None, **kwargs):
    """Generate mappings of feature attribute name to value.

    Notes:
        Use ArcPy cursor token names for object IDs and geometry objects/properties.

    Args:
        dataset_path (pathlib.Path, str): Path of the dataset.
        field_names (iter): Collection of field names. Names will be the keys in the
            dictionary mapping to their values. If value is None, all attributes fields
            will be used.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        spatial_reference_item: Item from which the spatial reference of the output
            geometry will be derived.

    Yields:
        dict
    """
    dataset_path = Path(dataset_path)
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("spatial_reference_item")
    if field_names is None:
        keys = {"feature": Dataset(dataset_path).field_names_tokenized}
    else:
        keys = {"feature": list(contain(field_names))}
    cursor = arcpy.da.SearchCursor(
        # ArcPy2.8.0: Convert to str.
        in_table=str(dataset_path),
        field_names=keys["feature"],
        where_clause=kwargs["dataset_where_sql"],
        spatial_reference=SpatialReference(kwargs["spatial_reference_item"]).object,
    )
    with cursor:
        for feature in cursor:
            yield dict(zip(cursor.fields, feature))


def as_tuples(
    dataset_path: Union[Path, str],
    field_names: Iterable[str],
    *,
    dataset_where_sql: Optional[str] = None,
    spatial_reference_item: Optional[Any] = None,
) -> Iterator[tuple]:
    """Generate tuples of feature attribute values.

    Notes:
        Use ArcPy cursor token names for object IDs and geometry objects/properties.

    Args:
        dataset_path: Path to the dataset.
        field_names: Collection of field names. The order of the names in the collection
            will determine where its value will fall in the generated item.
        dataset_where_sql: SQL where-clause for dataset subselection.
        spatial_reference_item: Item from which the spatial reference of the output
            geometry will be derived.
    """
    field_names = list(field_names)
    dataset_path = Path(dataset_path)
    cursor = arcpy.da.SearchCursor(
        # ArcPy2.8.0: Convert to str.
        in_table=str(dataset_path),
        field_names=field_names,
        where_clause=dataset_where_sql,
        spatial_reference=SpatialReference(spatial_reference_item).object,
    )
    with cursor:
        yield from cursor


def as_values(dataset_path, field_names, **kwargs):
    """Generate feature attribute values.

    Values of all given field names will be yielded as they are uncovered.

    Notes:
        Use ArcPy cursor token names for object IDs and geometry objects/properties.

    Args:
        dataset_path (pathlib.Path, str): Path of the dataset.
        field_names (iter): Collection of field names.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        spatial_reference_item: Item from which the spatial reference of the output
            geometry will be derived.

    Yields:
        object
    """
    dataset_path = Path(dataset_path)
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("spatial_reference_item")
    keys = {"feature": list(contain(field_names))}
    cursor = arcpy.da.SearchCursor(
        # ArcPy2.8.0: Convert to str.
        in_table=str(dataset_path),
        field_names=keys["feature"],
        where_clause=kwargs["dataset_where_sql"],
        spatial_reference=SpatialReference(kwargs["spatial_reference_item"]).object,
    )
    with cursor:
        for feature in cursor:
            for value in feature:
                yield value


def as_value_count(dataset_path, field_names, **kwargs):
    """Return counter of attribute values.

    Args:
        dataset_path (pathlib.Path, str): Path of the dataset.
        field_name (str): Name of field.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        spatial_reference_item: Item from which the spatial reference of the output
            geometry will be derived.

    Returns:
        collections.Counter
    """
    dataset_path = Path(dataset_path)
    return Counter(as_values(dataset_path, field_names, **kwargs))


def update_by_central_overlay(
    dataset_path, field_name, overlay_dataset_path, overlay_field_name, **kwargs
):
    """Update attribute values by finding the central overlay feature value.

    Note:
        Since only one value will be selected in the overlay, operations with multiple
        overlaying features will respect the geoprocessing environment merge rule. This
        rule generally defaults to the value of the "first" feature.

    Args:
        dataset_path (pathlib.Path, str): Path of the dataset.
        field_name (str): Name of the field.
        overlay_dataset_path (pathlib.Path, str): Path of the overlay-dataset.
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
    dataset_path = Path(dataset_path)
    overlay_dataset_path = Path(overlay_dataset_path)
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level,
        "Start: Update attributes in `%s.%s` by central-overlay value in `%s.%s`.",
        dataset_path,
        field_name,
        overlay_dataset_path,
        overlay_field_name,
    )
    original_tolerance = arcpy.env.XYTolerance
    view = {
        "dataset": DatasetView(
            dataset_path,
            kwargs.get("dataset_where_sql"),
            # Do *not* include any fields here (avoids name collisions in temp output).
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
            # ArcPy2.8.0: Convert to str.
            out_feature_class=str(temp_output_path),
            join_operation="JOIN_ONE_TO_ONE",
            join_type="KEEP_ALL",
            match_option="HAVE_THEIR_CENTER_IN",
        )
        if "tolerance" in kwargs:
            arcpy.env.XYTolerance = original_tolerance
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
        key_field_names=["OID@"],
        join_dataset_path=temp_output_path,
        join_field_name=overlay_field_name,
        join_key_field_names=["TARGET_FID"],
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
        dataset_path (pathlib.Path, str): Path of the dataset.
        field_name (str): Name of the field.
        overlay_dataset_path (pathlib.Path, str): Path of the overlay-dataset.
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
    dataset_path = Path(dataset_path)
    overlay_dataset_path = Path(overlay_dataset_path)
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level,
        "Start: Update attributes in `%s.%s` by dominant overlay value in `%s.%s`.",
        dataset_path,
        field_name,
        overlay_dataset_path,
        overlay_field_name,
    )
    original_tolerance = arcpy.env.XYTolerance
    view = {
        "dataset": DatasetView(
            dataset_path,
            kwargs.get("dataset_where_sql"),
            # Do *not* include any fields here (avoids name collisions in temp output).
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
            # ArcPy2.8.0: Convert to str.
            out_feature_class=str(temp_output_path),
            join_attributes="ALL",
        )
        if "tolerance" in kwargs:
            arcpy.env.XYTolerance = original_tolerance
    # Identity makes custom FID field names - in_features FID field comes first.
    fid_keys = [
        key for key in Dataset(temp_output_path).field_names if key.startswith("FID_")
    ]
    coverage = {}
    for oid, overlay_oid, value, area in as_tuples(
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
    **kwargs,
):
    """Update attribute values using a coded-values domain.

    Args:
        dataset_path (pathlib.Path, str): Path of the dataset.
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
    dataset_path = Path(dataset_path)
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
    states = update_by_mapping(
        dataset_path,
        field_name,
        mapping=Domain(domain_workspace_path, domain_name).code_description,
        key_field_names=[code_field_name],
        dataset_where_sql=kwargs["dataset_where_sql"],
        use_edit_session=kwargs["use_edit_session"],
        log_level=logging.DEBUG,
    )
    log_entity_states("attributes", states, LOG, log_level=level)
    LOG.log(level, "End: Update.")
    return states


def update_by_expression(
    dataset_path: Union[Path, str],
    field_name: str,
    expression: str,
    expression_type: str = "Python",
    *,
    dataset_where_sql: Optional[str] = None,
    use_edit_session: bool = False,
    log_level: int = logging.INFO,
) -> str:
    """Update attribute values using a (single) code-expression.

    Wraps arcpy.management.CalculateField.

    Args:
        dataset_path: Path to the dataset.
        field_name: Name of the field.
        expression: String expression to evaluate values from.
        expression_type: Type of code expression represents. Allowed values include:
            "Arcade", "Python", "Python3", and "SQL". Case-insensitive.
        dataset_where_sql: SQL where-clause for dataset subselection.
        use_edit_session: Updates are done in an edit session if True.
        log_level: Level to log the function at.

    Returns:
        Name of the field updated.
    """
    dataset_path = Path(dataset_path)
    if expression_type.upper() not in ["ARCADE", "PYTHON", "PYTHON3", "SQL"]:
        raise AttributeError("Invalid expression_type")

    LOG.log(
        log_level,
        "Start: Update attributes in `%s.%s` using %s expression `%s`.",
        dataset_path,
        field_name,
        expression_type,
        expression,
    )
    if expression_type.upper() == "PYTHON":
        expression_type = "PYTHON3"
    dataset_view = DatasetView(dataset_path, dataset_where_sql=dataset_where_sql)
    session = Editor(Dataset(dataset_path).workspace_path, use_edit_session)
    with session, dataset_view:
        arcpy.management.CalculateField(
            in_table=dataset_view.name,
            field=field_name,
            expression=expression,
            expression_type=expression_type,
        )
    LOG.log(log_level, "End: Update.")
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
        dataset_path (pathlib.Path, str): Path of the dataset.
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
    dataset_path = Path(dataset_path)
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

    keys = {
        "id": list(contain(id_field_names)),
        "sort": list(contain(kwargs.get("sort_field_names", []))),
    }
    keys["feature"] = keys["id"] + [field_name]
    matcher = FeatureMatcher(dataset_path, keys["id"], kwargs["dataset_where_sql"])
    session = Editor(Dataset(dataset_path).workspace_path, kwargs["use_edit_session"])
    cursor = arcpy.da.UpdateCursor(
        # ArcPy2.8.0: Convert to str.
        in_table=str(dataset_path),
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
        dataset_path (pathlib.Path, str): Path of the dataset.
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
    dataset_path = Path(dataset_path)
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
    session = Editor(Dataset(dataset_path).workspace_path, kwargs["use_edit_session"])
    cursor = arcpy.da.UpdateCursor(
        # ArcPy2.8.0: Convert to str.
        in_table=str(dataset_path),
        field_names=[field_name, source_field_name],
        where_clause=kwargs["dataset_where_sql"],
        spatial_reference=SpatialReference(kwargs["spatial_reference_item"]).object,
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
        dataset_path (pathlib.Path, str): Path of the dataset.
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
    dataset_path = Path(dataset_path)
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
    keys = {
        "args": list(contain(kwargs["arg_field_names"])),
        "kwargs": list(contain(kwargs["kwarg_field_names"])),
    }
    keys["feature"] = keys["args"] + keys["kwargs"] + [field_name]
    session = Editor(Dataset(dataset_path).workspace_path, kwargs["use_edit_session"])
    cursor = arcpy.da.UpdateCursor(
        # ArcPy2.8.0: Convert to str.
        in_table=str(dataset_path),
        field_names=keys["feature"],
        where_clause=kwargs["dataset_where_sql"],
        spatial_reference=SpatialReference(kwargs["spatial_reference_item"]).object,
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


def update_by_joined_value(
    dataset_path: Union[Path, str],
    field_name: str,
    key_field_names: Iterable[str],
    join_dataset_path: Union[Path, str],
    join_field_name: str,
    join_key_field_names: Iterable[str],
    *,
    dataset_where_sql: Optional[str] = None,
    join_dataset_where_sql: Optional[str] = None,
    use_edit_session: bool = False,
    log_level: int = logging.INFO,
) -> Counter:
    """Update attribute values by referencing a joinable field in another dataset.

    key_field_names & join_key_field_names must be the same length & same order.

    Args:
        dataset_path: Path to the dataset.
        field_name: Name of the field.
        key_field_names: Names of the relationship key fields.
        join_dataset_path: Path to the join-dataset.
        join_field_name: Name of the join-field.
        join_key_field_names: Names of the relationship key fields on join-dataset.
        dataset_where_sql: SQL where-clause for dataset subselection.
        join_dataset_where_sql: SQL where-clause for join-dataset subselection.
        use_edit_session: Updates are done in an edit session if True.
        log_level: Level to log the function at.

    Returns:
        Counts of features for each update-state.
    """
    dataset_path = Path(dataset_path)
    join_dataset_path = Path(join_dataset_path)
    key_field_names = list(key_field_names)
    join_key_field_names = list(join_key_field_names)
    LOG.log(
        log_level,
        "Start: Update attributes in `%s.%s` by joined values in `%s.%s`.",
        dataset_path,
        field_name,
        join_dataset_path,
        join_field_name,
    )
    if len(key_field_names) != len(join_key_field_names):
        raise AttributeError("id_field_names & join_id_field_names not same length.")

    cursor = arcpy.da.UpdateCursor(
        # ArcPy2.8.0: Convert to str.
        in_table=str(dataset_path),
        field_names=key_field_names + [field_name],
        where_clause=dataset_where_sql,
    )
    id_join_value = {
        feature[:-1]: feature[-1]
        for feature in as_tuples(
            join_dataset_path,
            field_names=join_key_field_names + [join_field_name],
            dataset_where_sql=join_dataset_where_sql,
        )
    }
    session = Editor(Dataset(dataset_path).workspace_path, use_edit_session)
    states = Counter()
    with session, cursor:
        for feature in cursor:
            value = feature[-1]
            new_value = id_join_value.get(tuple(feature[:-1]))
            if same_value(value, new_value):
                states["unchanged"] += 1
            else:
                try:
                    cursor.updateRow(feature[:-1] + [new_value])
                    states["altered"] += 1
                except RuntimeError as error:
                    raise RuntimeError(
                        f"Update cursor failed: Offending value: `{new_value}`"
                    ) from error

    log_entity_states("attributes", states, LOG, log_level=log_level)
    LOG.log(log_level, "End: Update.")
    return states


def update_by_mapping(dataset_path, field_name, mapping, key_field_names, **kwargs):
    """Update attribute values by finding them in a mapping.

    Note: Mapping key must be a tuple if an iterable.

    Args:
        dataset_path (pathlib.Path, str): Path of the dataset.
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
    dataset_path = Path(dataset_path)
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
    keys = {"map": list(contain(key_field_names))}
    keys["feature"] = keys["map"] + [field_name]
    if isinstance(mapping, tuple(EXEC_TYPES)):
        mapping = mapping()
    session = Editor(Dataset(dataset_path).workspace_path, kwargs["use_edit_session"])
    cursor = arcpy.da.UpdateCursor(
        # ArcPy2.8.0: Convert to str.
        in_table=str(dataset_path),
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


def update_by_overlay_count(dataset_path, field_name, overlay_dataset_path, **kwargs):
    """Update attribute values by finding overlay feature value.

    Args:
        dataset_path (pathlib.Path, str): Path of the dataset.
        field_name (str): Name of the field.
        overlay_dataset_path (pathlib.Path, str): Path of the overlay-dataset.
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
    dataset_path = Path(dataset_path)
    overlay_dataset_path = Path(overlay_dataset_path)
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level,
        "Start: Update attributes in `%s.%s` by overlay counts from `%s`.",
        dataset_path,
        field_name,
        overlay_dataset_path,
    )
    original_tolerance = arcpy.env.XYTolerance
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
            # ArcPy2.8.0: Convert to str.
            out_feature_class=str(temp_output_path),
            join_operation="join_one_to_one",
            join_type="keep_common",
            match_option="intersect",
        )
        if "tolerance" in kwargs:
            arcpy.env.XYTolerance = original_tolerance
    oid_overlay_count = dict(
        as_tuples(temp_output_path, field_names=["TARGET_FID", "Join_Count"])
    )
    dataset.delete(temp_output_path, log_level=logging.DEBUG)
    session = Editor(
        Dataset(dataset_path).workspace_path, kwargs.get("use_edit_session", False),
    )
    cursor = arcpy.da.UpdateCursor(
        # ArcPy2.8.0: Convert to str.
        in_table=str(dataset_path),
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
        dataset_path (pathlib.Path, str): Path of the dataset.
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
    dataset_path = Path(dataset_path)
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
    session = Editor(Dataset(dataset_path).workspace_path, kwargs["use_edit_session"])
    cursor = arcpy.da.UpdateCursor(
        # ArcPy2.8.0: Convert to str.
        in_table=str(dataset_path),
        field_names=[field_name],
        where_clause=kwargs["dataset_where_sql"],
    )
    # First run will clear duplicate IDs & gather used IDs.
    used_ids = set()
    # BUG-UNFILED: Use separate edit sessions (not a fan of this intermediate state).
    with session:
        with cursor:
            for [id_value] in cursor:
                if id_value in used_ids:
                    cursor.updateRow([None])
                else:
                    used_ids.add(id_value)
            _field = Field(dataset_path, field_name)
            id_pool = unique_ids(
                data_type=python_type(_field.type),
                string_length=_field.length,
                initial_number=(
                    max(used_ids) + 1
                    if kwargs["start_after_max_number"]
                    else kwargs["initial_number"]
                ),
            )
    # Second run will fill in missing IDs.
    with session:
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
        dataset_path (pathlib.Path, str): Path of the dataset.
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
    dataset_path = Path(dataset_path)
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("use_edit_session", False)
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level,
        "Start: Update attributes in `%s.%s` by given value.",
        dataset_path,
        field_name,
    )
    session = Editor(Dataset(dataset_path).workspace_path, kwargs["use_edit_session"])
    cursor = arcpy.da.UpdateCursor(
        # ArcPy2.8.0: Convert to str.
        in_table=str(dataset_path),
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
