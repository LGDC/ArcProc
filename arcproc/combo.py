"""Combination operations."""
import datetime
import logging
from pathlib import Path

import arcpy

from arcproc.arcobj import Editor, dataset_metadata


LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""

arcpy.SetLogHistory(False)


def adjust_for_shapefile(dataset_path, **kwargs):
    """Adjust features to meet shapefile requirements.

    Note:
        Shapefiles cannot have null-values. Nulls will be replaced with the values
            provided in the null replacement keyword arguments.
        Shapefiles only have dates in the date/datetime field. Times will be truncated
            in the adjustment.

    Args:
        dataset_path (pathlib.Path, str): Path of the dataset.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        date_null_replacement (datetime.date): Replacement value for null-values in
            date fields. Default is datetime.date.min.
        integer_null_replacement (int): Replacement value for null-values in integer
            fields. Default is 0.
        numeric_null_replacement (float): Replacement value for null-values in numeric
            fields. Default is 0.0.
        string_null_replacement (str): Replacement value for null-values in string
            fields. Default is "".
        use_edit_session (bool): Updates are done in an edit session if True. Default is
            False.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        pathlib.Path: Path of the adjusted dataset.
    """
    dataset_path = Path(dataset_path)
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("use_edit_session", False)
    level = kwargs.get("log_level", logging.INFO)
    replacement_value = {
        "date": kwargs.setdefault("date_null_replacement", datetime.date.min),
        "double": kwargs.setdefault("numeric_null_replacement", 0.0),
        "single": kwargs.setdefault("numeric_null_replacement", 0.0),
        "integer": kwargs.setdefault("integer_null_replacement", 0),
        "smallinteger": kwargs.setdefault("integer_null_replacement", 0),
        "string": kwargs.setdefault("string_null_replacement", ""),
        # Shapefile loader handles non-user fields seperately.
        # "geometry", "oid",
    }
    LOG.log(level, "Start: Adjust features for shapefile output in `%s`.", dataset_path)
    dataset_meta = dataset_metadata(dataset_path)
    session = Editor(dataset_meta["workspace_path"], kwargs["use_edit_session"])
    with session:
        for field in dataset_meta["user_fields"]:
            if field["type"].lower() not in replacement_value:
                LOG.log(
                    level,
                    "Skipping `%s`: field type cannot transfer to shapefile.",
                    field["name"],
                )
                continue

            else:
                LOG.log(level, "Adjusting values in `%s`.", field["name"])
            cursor = arcpy.da.UpdateCursor(
                # ArcPy2.8.0: Convert to str.
                in_table=str(dataset_path),
                field_names=[field["name"]],
                where_clause=kwargs["dataset_where_sql"],
            )
            with cursor:
                for (old_value,) in cursor:
                    if old_value is None:
                        new_value = replacement_value[field["type"].lower()]
                        try:
                            cursor.updateRow([new_value])
                        except RuntimeError:
                            LOG.error("Offending value is `%s`.", new_value)
                            raise RuntimeError

    LOG.log(level, "End: Adjust.")
    return dataset_path
