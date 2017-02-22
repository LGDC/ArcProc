"""Combination operations."""
import datetime
import logging

from arcetl import arcobj
from arcetl import attributes
from arcetl import helpers


LOG = logging.getLogger(__name__)


def adjust_for_shapefile(dataset_path, **kwargs):
    """Adjust features to meet shapefile requirements.

    Note:
        Shapefiles cannot have null-values. Nulls will be replaced with the
            values provided in the null replacement keyword arguments.
        Shapefiles only have dates in the date/datetime field. Times will be
            truncated in the adjustment.

    Args:
        dataset_path (str): Path of the dataset.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        date_null_replacement (datetime.date): Replacement value for
            null-values in date fields. Defaults to datetime.date.min.
        integer_null_replacement (int): Replacement value for null-values in
            integer fields. Defaults to 0.
        log_level (str): Level to log the function at. Defaults to 'info'.
        numeric_null_replacement (float): Replacement value for null-values
            in numeric fields. Defaults to 0.0.
        string_null_replacement (str): Replacement value for null-values in
            string fields. Defaults to ''.

    Returns:
        str: Path of the adjusted dataset.
    """
    log_level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(log_level, "Start: Adjust features for shapefile output in %s.",
            dataset_path)
    shp_field_convert_types = (
        'Date', 'Double', 'Single', 'Integer', 'SmallInteger', 'String',
        # Shapefile loader handles these types.
        # 'Geometry', 'OID',
        # Invalid shapefile types.
        # 'Blob', 'GlobalID', 'Guid', 'Raster',
        )
    def shp_value_convertor_factory(field_type):
        """Return value-convertor function for shapefile-valid values."""
        def _date_convertor(value):
            if value is None:
                value = kwargs.get('date_null_replacement', datetime.date.min)
            else:
                value = value.date()
            return value
        def _integer_convertor(value):
            if value is None:
                value = kwargs.get('integer_null_replacement', 0)
            return value
        def _numeric_convertor(value):
            if value is None:
                value = kwargs.get('numeric_null_replacement', 0.0)
            return value
        def _string_convertor(value):
            if value is None:
                value = kwargs.get('string_null_replacement', '')
            return value
        type_convertor = {
            'Date': _date_convertor,
            'Double': _numeric_convertor,
            'Integer': _integer_convertor,
            'Single': _numeric_convertor,
            'SmallInteger': _integer_convertor,
            'String': _string_convertor,
            }
        return type_convertor[field_type.lower()]
    for field in arcobj.dataset_metadata(dataset_path)['fields']:
        # Ignore fields that conversion won't affect in copying to shapefiles.
        if field['type'] not in shp_field_convert_types:
            continue
        attributes.update_by_function(
            dataset_path, field['name'],
            function=shp_value_convertor_factory(field['type']),
            dataset_where_sql=kwargs.get('dataset_where_sql'),
            log_level=None
            )
    LOG.log(log_level, "End: Adjust.")
    return dataset_path
