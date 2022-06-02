"""Attribute operations."""
from collections import Counter, defaultdict
from functools import partial
from logging import DEBUG, INFO, Logger, getLogger
from operator import itemgetter
from pathlib import Path
from types import FunctionType
from typing import Any, Iterable, Iterator, Mapping, Optional, Union

from arcpy import SetLogHistory
from arcpy.analysis import Identity, SpatialJoin
from arcpy.da import SearchCursor, UpdateCursor
from arcpy.management import CalculateField, Delete

from arcproc.dataset import DatasetView, unique_dataset_path
from arcproc.helpers import (
    EXECUTABLE_TYPES,
    log_entity_states,
    python_type_constructor,
    same_value,
    unique_ids,
)
from arcproc.metadata import (
    Dataset,
    Domain,
    Field,
    SpatialReference,
    SpatialReferenceSourceItem,
)
from arcproc.workspace import Session


LOG: Logger = getLogger(__name__)
"""Module-level logger."""

SetLogHistory(False)


def field_value_count(
    dataset_path: Union[Path, str],
    field_name: str,
    *,
    dataset_where_sql: Optional[str] = None,
    spatial_reference_item: SpatialReferenceSourceItem = None,
) -> Counter:
    """Return counter of field attribute values.

    Notes:
        Use ArcPy cursor token names for object IDs and geometry objects/properties.

    Args:
        dataset_path: Path to dataset.
        field_name: Name of field.
        dataset_where_sql: SQL where-clause for dataset subselection.
        spatial_reference_item: Item from which the spatial reference for any geometry
            properties will be set to. If set to None, will use spatial reference of
            the dataset.
    """
    dataset_path = Path(dataset_path)
    return Counter(
        field_values(
            dataset_path,
            field_name,
            dataset_where_sql=dataset_where_sql,
            spatial_reference_item=spatial_reference_item,
        )
    )


def field_values(
    dataset_path: Union[Path, str],
    field_name: str,
    *,
    dataset_where_sql: Optional[str] = None,
    spatial_reference_item: SpatialReferenceSourceItem = None,
) -> Iterator[Any]:
    """Generate field attribute values.

    Notes:
        Use ArcPy cursor token names for object IDs and geometry objects/properties.

    Args:
        dataset_path: Path to dataset.
        field_name: Name of field.
        dataset_where_sql: SQL where-clause for dataset subselection.
        spatial_reference_item: Item from which the spatial reference for any geometry
            properties will be set to. If set to None, will use spatial reference of
            the dataset.
    """
    dataset_path = Path(dataset_path)
    cursor = SearchCursor(
        # ArcPy2.8.0: Convert to str.
        in_table=str(dataset_path),
        field_names=[field_name],
        where_clause=dataset_where_sql,
        spatial_reference=SpatialReference(spatial_reference_item).object,
    )
    with cursor:
        for (value,) in cursor:
            yield value


def update_field_with_central_overlay(  # pylint: disable=invalid-name
    dataset_path: Union[Path, str],
    field_name: str,
    *,
    overlay_dataset_path: Union[Path, str],
    overlay_field_name: str,
    dataset_where_sql: Optional[str] = None,
    overlay_where_sql: Optional[str] = None,
    replacement_value: Optional[Any] = None,
    use_edit_session: bool = False,
    log_level: int = INFO,
) -> Counter:
    """Update field attribute values with the central overlay feature value.

    Notes:
        Since only one value will be selected in the overlay, operations with multiple
        overlaying features will respect the geoprocessing environment merge rule. This
        rule generally defaults to the value of the "first" feature.

    Args:
        dataset_path: Path to dataset.
        field_name: Name of field.
        overlay_dataset_path: Path to overlay-dataset.
        overlay_field_name: Name of overlay-field.
        dataset_where_sql: SQL where-clause for dataset subselection.
        overlay_where_sql: SQL where-clause for overlay-dataset subselection.
        replacement_value: Value to replace a present overlay-field value with. If set
            to None, no replacement will occur.
        use_edit_session: True if edits are to be made in an edit session.
        log_level: Level to log the function at.

    Returns:
        Attribute counts for each update-state.
    """
    dataset_path = Path(dataset_path)
    overlay_dataset_path = Path(overlay_dataset_path)
    LOG.log(
        log_level,
        "Start: Update field `%s.%s` with central-overlay value in `%s.%s`.",
        dataset_path,
        field_name,
        overlay_dataset_path,
        overlay_field_name,
    )
    # Do *not* include any fields here (avoids name collisions in temporary output).
    view = DatasetView(
        dataset_path, field_names=[], dataset_where_sql=dataset_where_sql
    )
    overlay_view = DatasetView(
        overlay_dataset_path,
        field_names=[overlay_field_name],
        dataset_where_sql=overlay_where_sql,
    )
    with view, overlay_view:
        temp_output_path = unique_dataset_path("output")
        SpatialJoin(
            target_features=view.name,
            join_features=overlay_view.name,
            # ArcPy2.8.0: Convert to str.
            out_feature_class=str(temp_output_path),
            join_operation="JOIN_ONE_TO_ONE",
            join_type="KEEP_ALL",
            match_option="HAVE_THEIR_CENTER_IN",
        )
    if replacement_value is not None:
        update_field_with_function(
            temp_output_path,
            overlay_field_name,
            function=lambda x: replacement_value if x else None,
            log_level=DEBUG,
        )
    states = update_field_with_join(
        dataset_path,
        field_name,
        key_field_names=["OID@"],
        join_dataset_path=temp_output_path,
        join_field_name=overlay_field_name,
        join_key_field_names=["TARGET_FID"],
        dataset_where_sql=dataset_where_sql,
        use_edit_session=use_edit_session,
        log_level=DEBUG,
    )
    # ArcPy2.8.0: Convert to str.
    Delete(str(temp_output_path))
    log_entity_states("attributes", states, logger=LOG, log_level=log_level)
    LOG.log(log_level, "End: Update.")
    return states


def update_field_with_dominant_overlay(  # pylint: disable=invalid-name
    dataset_path: Union[Path, str],
    field_name: str,
    *,
    overlay_dataset_path: Union[Path, str],
    overlay_field_name: str,
    dataset_where_sql: Optional[str] = None,
    overlay_where_sql: Optional[str] = None,
    include_missing_area: bool = False,
    use_edit_session: bool = False,
    log_level: int = INFO,
) -> Counter:
    """Update field attribute values with the dominant overlay feature value.
    Args:
        dataset_path: Path to dataset.
        field_name: Name of field.
        overlay_dataset_path: Path to overlay-dataset.
        overlay_field_name: Name of overlay-field.
        dataset_where_sql: SQL where-clause for dataset subselection.
        overlay_where_sql: SQL where-clause for overlay-dataset subselection.
        include_missing_area: If True, the collective area where no
            overlay value exists (i.e. no overlay geometry + overlay of NoneType value)
            is considered a valid candidate for the dominant overlay.
        use_edit_session: True if edits are to be made in an edit session.
        log_level: Level to log the function at.

    Returns:
        Attribute counts for each update-state.
    """
    dataset_path = Path(dataset_path)
    overlay_dataset_path = Path(overlay_dataset_path)
    LOG.log(
        log_level,
        "Start: Update field `%s.%s` with dominant overlay value in `%s.%s`.",
        dataset_path,
        field_name,
        overlay_dataset_path,
        overlay_field_name,
    )
    # Do *not* include any fields here (avoids name collisions in temporary output).
    view = DatasetView(
        dataset_path, field_names=[], dataset_where_sql=dataset_where_sql
    )
    overlay_view = DatasetView(
        overlay_dataset_path,
        field_names=[overlay_field_name],
        dataset_where_sql=overlay_where_sql,
    )
    with view, overlay_view:
        temp_output_path = unique_dataset_path("output")
        Identity(
            in_features=view.name,
            identity_features=overlay_view.name,
            # ArcPy2.8.0: Convert to str.
            out_feature_class=str(temp_output_path),
            join_attributes="ALL",
        )
    # Identity makes custom OID field names - in_features OID field comes first.
    oid_field_names = [
        field_name
        for field_name in Dataset(temp_output_path).field_names
        if field_name.startswith("FID_")
    ]
    oid_value_area = {}
    cursor = SearchCursor(
        # ArcPy2.8.0: Convert to str.
        in_table=str(temp_output_path),
        field_names=oid_field_names + [overlay_field_name, "SHAPE@AREA"],
    )
    with cursor:
        for oid, overlay_oid, value, area in cursor:
            # Def check for -1 OID (no overlay feature): identity does not set to None.
            if overlay_oid == -1:
                value = None
            if value is None and not include_missing_area:
                continue

            if oid not in oid_value_area:
                oid_value_area[oid] = defaultdict(float)
            oid_value_area[oid][value] += area
    # ArcPy2.8.0: Convert to str.
    Delete(str(temp_output_path))
    oid_dominant_value = {
        oid: max(value_area.items(), key=itemgetter(1))[0]
        for oid, value_area in oid_value_area.items()
    }
    states = update_field_with_mapping(
        dataset_path,
        field_name,
        mapping=oid_dominant_value,
        key_field_names=["OID@"],
        dataset_where_sql=dataset_where_sql,
        use_edit_session=use_edit_session,
        log_level=DEBUG,
    )
    log_entity_states("attributes", states, logger=LOG, log_level=log_level)
    LOG.log(log_level, "End: Update.")
    return states


def update_field_with_domain(
    dataset_path: Union[Path, str],
    field_name: str,
    *,
    code_field_name: str,
    domain_name: str,
    domain_workspace_path: Union[Path, str],
    dataset_where_sql: Optional[str] = None,
    use_edit_session: bool = False,
    log_level: int = INFO,
) -> Counter:
    """Update field attribute values with coded-values domain descriptions.

    Args:
        dataset_path: Path to dataset
        field_name: Name of field.
        code_field_name: Name of field with related domain code.
        domain_name: Name of domain.
        domain_workspace_path: Path of the workspace the domain is in.
        dataset_where_sql: SQL where-clause for dataset subselection.
        use_edit_session: True if edits are to be made in an edit session.
        log_level: Level to log the function at.

    Returns:
        Attribute counts for each update-state.
    """
    dataset_path = Path(dataset_path)
    LOG.log(
        log_level,
        "Start: Update field `%s.%s` from domain `%s` using code in field `%s`.",
        dataset_path,
        field_name,
        domain_name,
        code_field_name,
    )
    states = update_field_with_mapping(
        dataset_path,
        field_name,
        mapping=Domain(domain_workspace_path, domain_name).code_description,
        key_field_names=[code_field_name],
        dataset_where_sql=dataset_where_sql,
        use_edit_session=use_edit_session,
        log_level=DEBUG,
    )
    log_entity_states("attributes", states, logger=LOG, log_level=log_level)
    LOG.log(log_level, "End: Update.")
    return states


def update_field_with_expression(
    dataset_path: Union[Path, str],
    field_name: str,
    *,
    expression: str,
    expression_type: str = "Python",
    dataset_where_sql: Optional[str] = None,
    use_edit_session: bool = False,
    log_level: int = INFO,
) -> Field:
    """Update field attribute values with a (single) code-expression.

    Wraps CalculateField from ArcPy.

    Args:
        dataset_path: Path to dataset.
        field_name: Name of field.
        expression: String expression to evaluate values from.
        expression_type: Type of code expression represents. Allowed values include:
            "Arcade", "Python", "Python3", and "SQL". Case-insensitive.
        dataset_where_sql: SQL where-clause for dataset subselection.
        use_edit_session: True if edits are to be made in an edit session.
        log_level: Level to log the function at.

    Returns:
        Field metadata instance for field with updated attributes.

    Raises:
        AttributeError: If given expression type invalid.
    """
    dataset_path = Path(dataset_path)
    if expression_type.upper() not in ["ARCADE", "PYTHON", "PYTHON3", "SQL"]:
        raise AttributeError("Invalid expression_type")

    LOG.log(
        log_level,
        "Start: Update field `%s.%s` with %s expression `%s`.",
        dataset_path,
        field_name,
        expression_type,
        expression,
    )
    if expression_type.upper() == "PYTHON":
        expression_type = "PYTHON3"
    session = Session(Dataset(dataset_path).workspace_path, use_edit_session)
    view = DatasetView(dataset_path, dataset_where_sql=dataset_where_sql)
    with session, view:
        CalculateField(
            in_table=view.name,
            field=field_name,
            expression=expression,
            expression_type=expression_type,
        )
    LOG.log(log_level, "End: Update.")
    return Field(dataset_path, field_name)


def update_field_with_field(
    dataset_path: Union[Path, str],
    field_name: str,
    *,
    source_field_name: str,
    dataset_where_sql: Optional[str] = None,
    spatial_reference_item: SpatialReferenceSourceItem = None,
    use_edit_session: bool = False,
    log_level: int = INFO,
) -> Counter:
    """Update field attribute values with values from another field.

    Args:
        dataset_path: Path to dataset.
        field_name: Name of field.
        source_field_name: Name of field to get values from.
        dataset_where_sql: SQL where-clause for dataset subselection.
        spatial_reference_item: Item from which the spatial reference for any geometry
            properties will be set to. If set to None, will use spatial reference of
            the dataset.
        use_edit_session: True if edits are to be made in an edit session.
        log_level: Level to log the function at.

    Returns:
        Attribute counts for each update-state.

    Raises:
        RuntimeError: If attribute cannot be updated.
    """
    dataset_path = Path(dataset_path)
    LOG.log(
        log_level,
        "Start: Update field `%s.%s` with field `%s`.",
        dataset_path,
        field_name,
        source_field_name,
    )
    cursor = UpdateCursor(
        # ArcPy2.8.0: Convert to str.
        in_table=str(dataset_path),
        field_names=[field_name, source_field_name],
        where_clause=dataset_where_sql,
        spatial_reference=SpatialReference(spatial_reference_item).object,
    )
    session = Session(Dataset(dataset_path).workspace_path, use_edit_session)
    states = Counter()
    with session, cursor:
        for old_value, new_value in cursor:
            if same_value(old_value, new_value):
                states["unchanged"] += 1
            else:
                try:
                    cursor.updateRow([new_value, new_value])
                    states["altered"] += 1
                except RuntimeError as error:
                    raise RuntimeError(
                        f"Update cursor failed: Offending value: `{new_value}`"
                    ) from error

    log_entity_states("attributes", states, logger=LOG, log_level=log_level)
    LOG.log(log_level, "End: Update.")
    return states


def update_field_with_function(
    dataset_path: Union[Path, str],
    field_name: str,
    *,
    function: FunctionType,
    field_as_first_arg: bool = True,
    arg_field_names: Iterable[str] = (),
    kwarg_field_names: Iterable[str] = (),
    dataset_where_sql: Optional[str] = None,
    spatial_reference_item: SpatialReferenceSourceItem = None,
    use_edit_session: bool = False,
    log_level: int = INFO,
) -> Counter:
    """Update field attribute values with a function.

    Args:
        dataset_path: Path to dataset.
        field_name: Name of field.
        function: Function to return values from.
        field_as_first_arg: True if field value will be the first positional argument.
        arg_field_names: Field names whose values will be the function positional
            arguments (not including primary field).
        kwarg_field_names: Field names whose names & values will be the function keyword
            arguments.
        dataset_where_sql: SQL where-clause for dataset subselection.
        spatial_reference_item: Item from which the spatial reference for any geometry
            properties will be set to. If set to None, will use spatial reference of
            the dataset.
        use_edit_session: True if edits are to be made in an edit session.
        log_level: Level to log the function at.

    Returns:
        Attribute counts for each update-state.

    Raises:
        RuntimeError: If attribute cannot be updated.
    """
    dataset_path = Path(dataset_path)
    LOG.log(
        log_level,
        "Start: Update field `%s.%s` with `%s`.",
        dataset_path,
        field_name,
        # Partials show all the pre-loaded arg & kwarg values, which is cumbersome.
        f"partial version of function `{function.func}`"
        if isinstance(function, partial)
        else f"function `{function}`",
    )
    arg_field_names = list(arg_field_names)
    kwarg_field_names = list(kwarg_field_names)
    cursor = UpdateCursor(
        # ArcPy2.8.0: Convert to str.
        in_table=str(dataset_path),
        field_names=arg_field_names + kwarg_field_names + [field_name],
        where_clause=dataset_where_sql,
        spatial_reference=SpatialReference(spatial_reference_item).object,
    )
    session = Session(Dataset(dataset_path).workspace_path, use_edit_session)
    states = Counter()
    with session, cursor:
        for feature in cursor:
            old_value = feature[-1]
            args = feature[: len(arg_field_names)]
            if field_as_first_arg:
                args = [old_value] + args
            kwargs = dict(zip(kwarg_field_names, feature[len(arg_field_names) : -1]))
            new_value = function(*args, **kwargs)
            if same_value(old_value, new_value):
                states["unchanged"] += 1
            else:
                try:
                    cursor.updateRow(feature[:-1] + [new_value])
                    states["altered"] += 1
                except RuntimeError as error:
                    raise RuntimeError(
                        f"Update cursor failed: Offending value: `{new_value}`"
                    ) from error

    log_entity_states("attributes", states, logger=LOG, log_level=log_level)
    LOG.log(log_level, "End: Update.")
    return states


def update_field_with_join(
    dataset_path: Union[Path, str],
    field_name: str,
    *,
    key_field_names: Iterable[str],
    join_dataset_path: Union[Path, str],
    join_field_name: str,
    join_key_field_names: Iterable[str],
    dataset_where_sql: Optional[str] = None,
    join_dataset_where_sql: Optional[str] = None,
    use_edit_session: bool = False,
    log_level: int = INFO,
) -> Counter:
    """Update field attribute values with a join to a field in another dataset.

    key_field_names & join_key_field_names must be the same length & same order.

    Args:
        dataset_path: Path to dataset.
        field_name: Name of field.
        key_field_names: Names of relationship key fields.
        join_dataset_path: Path to join-dataset.
        join_field_name: Name of join-field.
        join_key_field_names: Names of relationship key fields on join-dataset.
        dataset_where_sql: SQL where-clause for dataset subselection.
        join_dataset_where_sql: SQL where-clause for join-dataset subselection.
        use_edit_session: Updates are done in an edit session if True.
        use_edit_session: True if edits are to be made in an edit session.
        log_level: Level to log the function at.

    Returns:
        Attribute counts for each update-state.

    Raises:
        AttributeError: If key_field_names & join_key_field_names have different length.
        RuntimeError: If attribute cannot be updated.
    """
    dataset_path = Path(dataset_path)
    join_dataset_path = Path(join_dataset_path)
    LOG.log(
        log_level,
        "Start: Update field `%s.%s` with join to `%s.%s`.",
        dataset_path,
        field_name,
        join_dataset_path,
        join_field_name,
    )
    key_field_names = list(key_field_names)
    join_key_field_names = list(join_key_field_names)
    if len(key_field_names) != len(join_key_field_names):
        raise AttributeError("key_field_names & join_key_field_names not same length.")

    cursor = SearchCursor(
        # ArcPy2.8.0: Convert to str.
        in_table=str(join_dataset_path),
        field_names=join_key_field_names + [join_field_name],
        where_clause=join_dataset_where_sql,
    )
    with cursor:
        id_join_value = {feature[:-1]: feature[-1] for feature in cursor}
    cursor = UpdateCursor(
        # ArcPy2.8.0: Convert to str.
        in_table=str(dataset_path),
        field_names=key_field_names + [field_name],
        where_clause=dataset_where_sql,
    )
    session = Session(Dataset(dataset_path).workspace_path, use_edit_session)
    states = Counter()
    with session, cursor:
        for feature in cursor:
            old_value = feature[-1]
            new_value = id_join_value.get(tuple(feature[:-1]))
            if same_value(old_value, new_value):
                states["unchanged"] += 1
            else:
                try:
                    cursor.updateRow(feature[:-1] + [new_value])
                    states["altered"] += 1
                except RuntimeError as error:
                    raise RuntimeError(
                        f"Update cursor failed: Offending value: `{new_value}`"
                    ) from error

    log_entity_states("attributes", states, logger=LOG, log_level=log_level)
    LOG.log(log_level, "End: Update.")
    return states


def update_field_with_mapping(
    dataset_path: Union[Path, str],
    field_name: str,
    *,
    mapping: Union[Mapping, FunctionType],
    key_field_names: Iterable[str],
    dataset_where_sql: Optional[str] = None,
    default_value: Any = None,
    use_edit_session: bool = False,
    log_level: int = INFO,
) -> Counter:
    """Update field attribute values with a mapping.

    Notes:
        Mapping key must be a tuple if an iterable.

    Args:
        dataset_path: Path to dataset.
        field_name: Name of field.
        mapping: Mapping to get values from.
        key_field_names: Names of mapping key fields.
        dataset_where_sql: SQL where-clause for dataset subselection.
        default_value: Value to assign mapping if key value not in mapping.
        use_edit_session: True if edits are to be made in an edit session.
        log_level: Level to log the function at.

    Returns:
        Attribute counts for each update-state.

    Raises:
        RuntimeError: If attribute cannot be updated.
    """
    dataset_path = Path(dataset_path)
    key_field_names = list(key_field_names)
    LOG.log(
        log_level, "Start: Update field `%s.%s` with mapping", dataset_path, field_name
    )
    if isinstance(mapping, EXECUTABLE_TYPES):
        mapping = mapping()
    cursor = UpdateCursor(
        # ArcPy2.8.0: Convert to str.
        in_table=str(dataset_path),
        field_names=key_field_names + [field_name],
        where_clause=dataset_where_sql,
    )
    session = Session(Dataset(dataset_path).workspace_path, use_edit_session)
    states = Counter()
    with session, cursor:
        for feature in cursor:
            key = feature[0] if len(key_field_names) == 1 else tuple(feature[:-1])
            old_value = feature[-1]
            new_value = mapping.get(key, default_value)
            if same_value(old_value, new_value):
                states["unchanged"] += 1
            else:
                try:
                    cursor.updateRow(feature[:-1] + [new_value])
                    states["altered"] += 1
                except RuntimeError as error:
                    raise RuntimeError(
                        f"Update cursor failed: Offending value: `{new_value}`"
                    ) from error

    log_entity_states("attributes", states, logger=LOG, log_level=log_level)
    LOG.log(log_level, "End: Update.")
    return states


def update_field_with_overlay_count(
    dataset_path: Union[Path, str],
    field_name: str,
    *,
    overlay_dataset_path: Union[Path, str],
    dataset_where_sql: Optional[str] = None,
    overlay_where_sql: Optional[str] = None,
    use_edit_session: bool = False,
    log_level: int = INFO,
) -> Counter:
    """Update field attribute values with count of overlay features.

    Args:
        dataset_path: Path to dataset.
        field_name: Name of field.
        overlay_dataset_path: Path to overlay-dataset.

    Keyword Args:
        dataset_where_sql: SQL where-clause for dataset subselection.
        overlay_where_sql: SQL where-clause for overlay-dataset subselection.
        use_edit_session: True if edits are to be made in an edit session.
        log_level: Level to log the function at.

    Returns:
        Attribute counts for each update-state.

    Raises:
        RuntimeError: If attribute cannot be updated.
    """
    dataset_path = Path(dataset_path)
    overlay_dataset_path = Path(overlay_dataset_path)
    LOG.log(
        log_level,
        "Start: Update field `%s.%s` with overlay feature counts from `%s`.",
        dataset_path,
        field_name,
        overlay_dataset_path,
    )
    view = DatasetView(
        dataset_path, field_names=[], dataset_where_sql=dataset_where_sql
    )
    overlay_view = DatasetView(
        overlay_dataset_path, field_names=[], dataset_where_sql=overlay_where_sql
    )
    with view, overlay_view:
        temp_output_path = unique_dataset_path("output")
        SpatialJoin(
            target_features=view.name,
            join_features=overlay_view.name,
            # ArcPy2.8.0: Convert to str.
            out_feature_class=str(temp_output_path),
            join_operation="JOIN_ONE_TO_ONE",
            join_type="KEEP_COMMON",
            match_option="INTERSECT",
        )
    cursor = SearchCursor(
        # ArcPy2.8.0: Convert to str.
        in_table=str(temp_output_path),
        field_names=["TARGET_FID", "Join_Count"],
    )
    with cursor:
        oid_overlay_count = dict(cursor)
    # ArcPy2.8.0: Convert to str.
    Delete(str(temp_output_path))
    cursor = UpdateCursor(
        # ArcPy2.8.0: Convert to str.
        in_table=str(dataset_path),
        field_names=["OID@", field_name],
        where_clause=dataset_where_sql,
    )
    session = Session(Dataset(dataset_path).workspace_path, use_edit_session)
    states = Counter()
    with session, cursor:
        for feature in cursor:
            oid = feature[0]
            old_value = feature[1]
            new_value = oid_overlay_count.get(oid, 0)
            if same_value(old_value, new_value):
                states["unchanged"] += 1
            else:
                try:
                    cursor.updateRow([oid, new_value])
                    states["altered"] += 1
                except RuntimeError as error:
                    raise RuntimeError(
                        f"Update cursor failed: Offending value: `{new_value}`"
                    ) from error

    log_entity_states("attributes", states, logger=LOG, log_level=log_level)
    LOG.log(log_level, "End: Update.")
    return states


def update_field_with_unique_id(
    dataset_path: Union[Path, str],
    field_name: str,
    *,
    dataset_where_sql: Optional[str] = None,
    initial_number: int = 1,
    start_after_max_number: bool = False,
    use_edit_session: bool = False,
    log_level: int = INFO,
) -> Counter:
    """Update field attribute values with a unique ID.

    Existing IDs are preserved, if unique.

    Args:
        dataset_path: Path to dataset.
        field_name: Name of field.
        dataset_where_sql: SQL where-clause for dataset subselection.
        initial_number: Initial number for a proposed ID, if using a numeric data type.
            Superseded by `start_after_max_number`.
        start_after_max_number: Initial number will be one greater than the
            maximum existing ID number if True, if using a numeric data type.
        use_edit_session: True if edits are to be made in an edit session.
        log_level: Level to log the function at.

    Returns:
        Attribute counts for each update-state.

    Raises:
        RuntimeError: If attribute cannot be updated.
    """
    dataset_path = Path(dataset_path)
    LOG.log(
        log_level,
        "Start: Update field `%s.%s` with unique ID.",
        dataset_path,
        field_name,
    )
    cursor = UpdateCursor(
        # ArcPy2.8.0: Convert to str.
        in_table=str(dataset_path),
        field_names=[field_name],
        where_clause=dataset_where_sql,
    )
    session = Session(Dataset(dataset_path).workspace_path, use_edit_session)
    # First run will clear duplicate IDs & gather used IDs.
    used_ids = set()
    # BUG-UNFILED: Use separate edit sessions (not a fan of this intermediate state).
    with session, cursor:
        for (id_value,) in cursor:
            if id_value in used_ids:
                cursor.updateRow([None])
            else:
                used_ids.add(id_value)
        _field = Field(dataset_path, field_name)
        id_pool = unique_ids(
            data_type=python_type_constructor(_field.type),
            string_length=_field.length,
            initial_number=(
                max(used_ids) + 1 if start_after_max_number else initial_number
            ),
        )
    states = Counter()
    # Second run will fill in missing IDs.
    with session, cursor:
        for (id_value,) in cursor:
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
                except RuntimeError as error:
                    raise RuntimeError(
                        f"Update cursor failed: Offending value: `{id_value}`"
                    ) from error

    log_entity_states("attributes", states, logger=LOG, log_level=log_level)
    LOG.log(log_level, "End: Update.")
    return states


def update_field_with_value(
    dataset_path: Union[Path, str],
    field_name: str,
    *,
    value: Any,
    dataset_where_sql: Optional[str] = None,
    use_edit_session: bool = False,
    log_level: int = INFO,
) -> Counter:
    """Update field attribute values with a given value.

    Args:
        dataset_path: Path to dataset.
        field_name: Name of field.
        value: Value to assign.
        dataset_where_sql: SQL where-clause for dataset subselection.
        use_edit_session: True if edits are to be made in an edit session.
        log_level: Level to log the function at.

    Returns:
        Attribute counts for each update-state.

    Raises:
        RuntimeError: If attribute cannot be updated.
    """
    dataset_path = Path(dataset_path)
    LOG.log(
        log_level,
        "Start: Update field `%s.%s` with given value.",
        dataset_path,
        field_name,
    )
    cursor = UpdateCursor(
        # ArcPy2.8.0: Convert to str.
        in_table=str(dataset_path),
        field_names=[field_name],
        where_clause=dataset_where_sql,
    )
    session = Session(Dataset(dataset_path).workspace_path, use_edit_session)
    states = Counter()
    with session, cursor:
        for (old_value,) in cursor:
            if same_value(old_value, value):
                states["unchanged"] += 1
            else:
                try:
                    cursor.updateRow([value])
                    states["altered"] += 1
                except RuntimeError as error:
                    raise RuntimeError(
                        f"Update cursor failed: Offending value: `{value}`"
                    ) from error

    log_entity_states("attributes", states, logger=LOG, log_level=log_level)
    LOG.log(log_level, "End: Update.")
    return states
