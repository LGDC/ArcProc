"""Tracking operations."""
from collections import Counter, defaultdict
import datetime
from itertools import chain
import logging
from operator import itemgetter

import arcpy

from arcproc import arcobj
from arcproc import attributes
from arcproc import features
from arcproc.helpers import log_entity_states, same_value


LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""


def consolidate_rows(dataset_path, field_name, id_field_name, **kwargs):
    """Consolidate tracking dataset rows where the value did not actually change.

    Useful for quick-loaded point-in-time values, or for processing hand-altered rows.

    Args:
        dataset_path (str): Path of tracking dataset.
        field_name (str): Name of tracked field.
        id_field_name (str): Name of ID field.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        date_initiated_field_name (str): Name of tracking-row-inititated date field.
            Default is "date_initiated".
        date_expired_field_name (str): Name of tracking-row-expired date field. Default
            is "date_expired".
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        collections.Counter: Counts of features for each update-state.
    """
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(level, "Start: Consolidate tracking rows in `%s`.", dataset_path)
    for key in ["date_initiated", "date_expired"]:
        if not kwargs.get(key + "_field_name"):
            kwargs[key + "_field_name"] = key
    field_names = [
        id_field_name,
        kwargs["date_initiated_field_name"],
        kwargs["date_expired_field_name"],
        field_name,
    ]
    id_rows = defaultdict(list)
    for row in attributes.as_dicts(dataset_path, field_names):
        id_rows[row[id_field_name]].append(row)
    for id_ in list(id_rows):
        rows = sorted(id_rows[id_], key=itemgetter(kwargs["date_initiated_field_name"]))
        for i, row in enumerate(rows):
            if i == 0 or row[kwargs["date_initiated_field_name"]] is None:
                continue

            previous_row = rows[i - 1]
            if same_value(row[field_name], previous_row[field_name]):
                # Move previous row date initiated to current row & clear from previous.
                row[kwargs["date_initiated_field_name"]] = previous_row[
                    kwargs["date_initiated_field_name"]
                ]
                previous_row[kwargs["date_initiated_field_name"]] = None
        id_rows[id_] = [
            row for row in rows if row[kwargs["date_initiated_field_name"]] is not None
        ]
    states = features.update_from_dicts(
        dataset_path,
        update_features=chain(*id_rows.values()),
        id_field_names=field_names[:2],
        field_names=field_names,
        use_edit_session=kwargs.get("use_edit_session", False),
        log_level=logging.DEBUG,
    )
    log_entity_states("tracking rows", states, LOG, log_level=level)
    LOG.log(level, "End: Consolidate.")
    return states


def update_rows(dataset_path, field_name, id_field_name, cmp_dataset_path, **kwargs):
    """Add field value changes to tracking dataset from comparison dataset.

    Args:
        dataset_path (str): Path of tracking dataset.
        field_name (str): Name of tracked field.
        id_field_name (str): Name of ID field.
        cmp_dataset_path (str): Path of comparison dataset.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        cmp_date (datetime.date, datetime.datetime): Date to mark comparison change.
            Default is date of execution.
        cmp_field_name (str): Name of tracked field in comparison dataset, if different
            from field_name.
        cmp_id_field_name (str): Name of ID field in comparison dataset, if different
            from id_field_name.
        date_initiated_field_name (str): Name of tracking-row-inititated date field.
            Default is "date_initiated".
        date_expired_field_name (str): Name of tracking-row-expired date field. Default
            is "date_expired".
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        collections.Counter: Counts of features for each update-state.
    """
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level,
        "Start: Update tracking rows in `%s` from `%s`.",
        dataset_path,
        cmp_dataset_path,
    )
    if not kwargs.get("cmp_date"):
        kwargs["cmp_date"] = datetime.date.today()
    for key in ["date_initiated", "date_expired"]:
        if not kwargs.get(key + "_field_name"):
            kwargs[key + "_field_name"] = key
    current_where_sql = "{date_expired_field_name} IS NULL".format(**kwargs)
    meta = {"dataset": arcobj.dataset_metadata(dataset_path)}
    id_value = {}
    id_value["current"] = attributes.id_values_map(
        dataset_path,
        id_field_names=[id_field_name],
        field_names=[field_name],
        dataset_where_clause=current_where_sql,
    )
    id_value["cmp"] = attributes.id_values_map(
        cmp_dataset_path,
        id_field_names=[kwargs.get("cmp_id_field_name", id_field_name)],
        field_names=[kwargs.get("cmp_field_name", field_name)],
    )
    ids = {
        "changing": set(),
        "expiring": {id_ for id_ in id_value["current"] if id_ not in id_value["cmp"]},
    }
    new_rows = []
    for id_, value in id_value["cmp"].items():
        if id_ not in id_value["current"]:
            new_rows.append((id_, value, kwargs["cmp_date"]))
        elif not same_value(value, id_value["current"][id_]):
            ids["changing"].add(id_)
            new_rows.append((id_, value, kwargs["cmp_date"]))
    states = Counter()
    # Could replace cursor with features.update_from_iters if that function adds a
    # dataset_where_sql keyword argument.
    session = arcobj.Editor(
        meta["dataset"]["workspace_path"], kwargs.get("use_edit_session", False)
    )
    cursor = arcpy.da.UpdateCursor(
        dataset_path,
        field_names=[id_field_name, field_name, kwargs["date_expired_field_name"]],
    )
    with session, cursor:
        for id_, value, _ in cursor:
            if id_ in ids["changing"] or id_ in ids["expiring"]:
                cursor.updateRow((id_, value, kwargs["cmp_date"]))
                states["altered"] += 1
            else:
                states["unchanged"] += 1
    states.update(
        features.insert_from_iters(
            dataset_path,
            insert_features=new_rows,
            field_names=[
                id_field_name,
                field_name,
                kwargs["date_initiated_field_name"],
            ],
            use_edit_session=kwargs.get("use_edit_session", False),
            log_level=logging.DEBUG,
        )
    )
    log_entity_states("tracking rows", states, LOG, log_level=level)
    LOG.log(level, "End: Update.")
    return states
