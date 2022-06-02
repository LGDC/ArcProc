"""Tracking operations."""
from collections import Counter, defaultdict
from datetime import date
from datetime import datetime as _datetime
from itertools import chain
from logging import DEBUG, INFO, Logger, getLogger
from operator import itemgetter
from pathlib import Path
from typing import Iterable, Optional, Union

from arcpy import SetLogHistory
from arcpy.da import UpdateCursor

from arcproc.features import (
    features_as_dicts,
    features_as_tuples,
    insert_features_from_sequences,
    update_features_from_mappings,
)
from arcproc.helpers import log_entity_states, same_value
from arcproc.metadata import Dataset
from arcproc.workspace import Editing


LOG: Logger = getLogger(__name__)
"""Module-level logger."""

SetLogHistory(False)


def consolidate_tracking_rows(
    dataset_path: Union[Path, str],
    *,
    field_name: str,
    id_field_names: Iterable[str],
    date_initiated_field_name: str = "date_initiated",
    date_expired_field_name: str = "date_expired",
    use_edit_session: bool = False,
    log_level: int = INFO,
) -> Counter:
    """Consolidate tracking rows where the value does not actually change.

    Useful for quick-loaded point-in-time values, or for processing hand-altered rows.

    Args:
        dataset_path: Path to tracking dataset.
        field_name: Name of field with tracked attribute.
        id_field_names: Names of the feature ID fields.
        date_initiated_field_name: Name of tracking-row-inititated date field.
        date_expired_field_name: Name of tracking-row-expired date field.
        use_edit_session: True if edits are to be made in an edit session.
        log_level: Level to log the function at.

    Returns:
        Feature counts for each update-state.
    """
    dataset_path = Path(dataset_path)
    LOG.log(log_level, "Start: Consolidate tracking rows in `%s`.", dataset_path)
    id_field_names = list(id_field_names)
    field_names = id_field_names + [
        date_initiated_field_name,
        date_expired_field_name,
        field_name,
    ]
    id_rows = defaultdict(list)
    for row in features_as_dicts(dataset_path, field_names=field_names):
        _id = tuple(row[name] for name in id_field_names)
        id_rows[_id].append(row)
    for _id in list(id_rows):
        rows = sorted(id_rows[_id], key=itemgetter(date_initiated_field_name))
        for i, row in enumerate(rows):
            if i == 0 or row[date_initiated_field_name] is None:
                continue

            date_initiated = row[date_initiated_field_name]
            value = row[field_name]
            previous_row = rows[i - 1]
            previous_value = previous_row[field_name]
            previous_date_expired = previous_row[date_expired_field_name]
            if same_value(value, previous_value) and same_value(
                date_initiated, previous_date_expired
            ):
                # Move previous row date initiated to current row & clear from previous.
                row[date_initiated_field_name] = previous_row[date_initiated_field_name]
                previous_row[date_initiated_field_name] = None
        id_rows[_id] = [
            row for row in rows if row[date_initiated_field_name] is not None
        ]
    states = update_features_from_mappings(
        dataset_path,
        field_names=field_names,
        # In tracking dataset, ID is ID + date_initiated.
        id_field_names=id_field_names + [date_initiated_field_name],
        source_features=chain(*id_rows.values()),
        use_edit_session=use_edit_session,
        log_level=DEBUG,
    )
    log_entity_states("tracking rows", states, logger=LOG, log_level=log_level)
    LOG.log(log_level, "End: Consolidate.")
    return states


def update_tracking_rows(
    dataset_path: Union[Path, str],
    *,
    field_name: str,
    id_field_names: Iterable[str],
    cmp_dataset_path: Union[Path, str],
    cmp_field_name: Optional[str] = None,
    cmp_id_field_names: Optional[Iterable[str]] = None,
    cmp_date: Optional[Union[date, _datetime]] = None,
    date_initiated_field_name: str = "date_initiated",
    date_expired_field_name: str = "date_expired",
    use_edit_session: bool = False,
    log_level: int = INFO,
) -> Counter:
    """Update tracking rows from comparison dataset.

    Args:
        dataset_path: Path to tracking dataset.
        field_name: Name of field with tracked attribute.
        id_field_names: Names of the feature ID fields.
        cmp_dataset_path: Path to comparison dataset.
        cmp_field_name: Name of field with tracked attribute in comparison dataset. If
            set to None, will assume same as field_name.
        cmp_id_field_names: Names of the feature ID fields in comparison dataset. If set
            to None, will assume same as field_name.
        cmp_date: Date to mark comparison change. If set to None, will set to the date
            of execution.
        date_initiated_field_name: Name of tracking-row-inititated date field.
        date_expired_field_name: Name of tracking-row-expired date field.
        use_edit_session: True if edits are to be made in an edit session.
        log_level: Level to log the function at.

    Returns:
        Feature counts for each update-state.
    """
    dataset_path = Path(dataset_path)
    cmp_dataset_path = Path(cmp_dataset_path)
    LOG.log(
        log_level,
        "Start: Update tracking rows in `%s` from `%s`.",
        dataset_path,
        cmp_dataset_path,
    )
    id_field_names = list(id_field_names)
    if cmp_field_name is None:
        cmp_field_name = field_name
    cmp_id_field_names = (
        id_field_names if cmp_id_field_names is None else list(cmp_id_field_names)
    )
    if cmp_date is None:
        cmp_date = date.today()
    current_where_sql = f"{date_expired_field_name} IS NULL"
    id_current_value = {
        row[:-1]: row[-1]
        for row in features_as_tuples(
            dataset_path,
            field_names=id_field_names + [field_name],
            dataset_where_sql=current_where_sql,
        )
    }
    id_cmp_value = {
        row[:-1]: row[-1]
        for row in features_as_tuples(
            cmp_dataset_path, field_names=cmp_id_field_names + [cmp_field_name]
        )
    }
    changed_ids = set()
    expired_ids = {_id for _id in id_current_value if _id not in id_cmp_value}
    new_rows = []
    for _id, value in id_cmp_value.items():
        if _id not in id_current_value:
            new_rows.append(_id + (value, cmp_date))
        elif not same_value(value, id_current_value[_id]):
            changed_ids.add(_id)
            new_rows.append(_id + (value, cmp_date))
    # ArcPy2.8.0: Convert Path to str.
    cursor = UpdateCursor(
        in_table=str(dataset_path),
        field_names=id_field_names + [field_name, date_expired_field_name],
        where_clause=current_where_sql,
    )
    session = Editing(Dataset(dataset_path).workspace_path, use_edit_session)
    states = Counter()
    with session, cursor:
        for row in cursor:
            _id = tuple(row[: len(id_field_names)])
            if _id in changed_ids or _id in expired_ids:
                cursor.updateRow(_id + (row[-2], cmp_date))
            else:
                states["unchanged"] += 1
    insert_features_from_sequences(
        dataset_path,
        field_names=id_field_names + [field_name, date_initiated_field_name],
        source_features=new_rows,
        use_edit_session=use_edit_session,
        log_level=DEBUG,
    )
    states["changed"] = len(changed_ids)
    states["expired"] = len(expired_ids)
    log_entity_states("features", states, logger=LOG, log_level=log_level)
    LOG.log(log_level, "End: Update.")
    return states
