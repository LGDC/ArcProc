"""Combination operations."""
import datetime
import logging

import arcpy

from arcetl import arcobj
from arcetl.helpers import leveled_logger


LOG = logging.getLogger(__name__)
"""logging.Logger: Toolbox-level logger."""


def adjust_for_shapefile(dataset_path, **kwargs):
    """Adjust features to meet shapefile requirements.

    Note:
        Shapefiles cannot have null-values. Nulls will be replaced with the values
            provided in the null replacement keyword arguments.
        Shapefiles only have dates in the date/datetime field. Times will be truncated
            in the adjustment.

    Args:
        dataset_path (str): Path of the dataset.
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
            fields. Default is ''.
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        str: Path of the adjusted dataset.

    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('use_edit_session', False)
    shp_type_replace_val = {
        'date': kwargs.setdefault('date_null_replacement', datetime.date.min),
        'double': kwargs.setdefault('numeric_null_replacement', 0.0),
        'single': kwargs.setdefault('numeric_null_replacement', 0.0),
        'integer': kwargs.setdefault('integer_null_replacement', 0),
        'smallinteger': kwargs.setdefault('integer_null_replacement', 0),
        'string': kwargs.setdefault('string_null_replacement', ''),
        # Shapefile loader handles these types.
        # 'geometry', 'oid',
    }
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    log("Start: Adjust features for shapefile output in %s.", dataset_path)
    meta = {'dataset': arcobj.dataset_metadata(dataset_path)}
    session = arcobj.Editor(
        meta['dataset']['workspace_path'], kwargs['use_edit_session']
    )
    cursor_kwargs = {
        'in_table': dataset_path, 'where_clause': kwargs['dataset_where_sql']
    }
    with session:
        for field in meta['dataset']['user_fields']:
            if field['type'].lower() not in shp_type_replace_val:
                log("Skipping %s field: type cannot transfer to shapefile.")
                continue

            else:
                log("Adjusting values in %s field.", field['name'])
            cursor = arcpy.da.UpdateCursor(field_names=[field['name']], **cursor_kwargs)
            with cursor:
                for old_val, in cursor:
                    if old_val is None:
                        new_val = shp_type_replace_val[field['type'].lower()]
                    else:
                        new_val = old_val
                    if old_val != new_val:
                        try:
                            cursor.updateRow([new_val])
                        except RuntimeError:
                            LOG.error("Offending value is %s", new_val)
                            raise RuntimeError

    log("End: Adjust.")
    return dataset_path
