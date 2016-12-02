"""Combination operations."""
import datetime as _datetime
import logging as _logging

from arcetl import attributes
from arcetl import dataset
from arcetl import helpers


LOG = _logging.getLogger(__name__)


def adjust_for_shapefile(dataset_path, **kwargs):
    """Adjust features to meet shapefile requirements.

    Adjustments currently made:
    * Convert datetime values to date or time based on
    preserve_time_not_date flag.
    * Convert nulls to replacement value.
    Args:
        dataset_path (str): Path of dataset.
    Kwargs:
        datetime_null_replacement (datetime.date): Replacement value for nulls
            in datetime fields.
        integer_null_replacement (int): Replacement value for nulls in integer
            fields.
        numeric_null_replacement (float): Replacement value for nulls in
            numeric fields.
        string_null_replacement (str): Replacement value for nulls in string
            fields.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [
            ('dataset_where_sql', None),
            ('datetime_null_replacement', _datetime.date.min),
            ('integer_null_replacement', 0), ('numeric_null_replacement', 0.0),
            ('string_null_replacement', ''), ('log_level', 'info')
        ]:
        kwargs.setdefault(*kwarg_default)
    log_level = helpers.log_level(kwargs['log_level'])
    LOG.log(log_level, "Start: Adjust features for shapefile output in %s.",
            dataset_path)
    type_function_map = {
        # Invalid shapefile field types: 'blob', 'raster'.
        # Shapefiles can only store dates, not times.
        'date': (lambda x: kwargs['datetime_null_replacement']
                 if x is None else x.date()),
        'double': (lambda x: kwargs['numeric_null_replacement']
                   if x is None else x),
        #'geometry',  # Passed-through: Shapefile loader handles this.
        #'guid': Not valid shapefile type.
        'integer': (lambda x: kwargs['integer_null_replacement']
                    if x is None else x),
        #'oid',  # Passed-through: Shapefile loader handles this.
        'single': (lambda x: kwargs['numeric_null_replacement']
                   if x is None else x),
        'smallinteger': (lambda x: kwargs['integer_null_replacement']
                         if x is None else x),
        'string': (lambda x: kwargs['string_null_replacement']
                   if x is None else x)
        }
    for field in dataset.metadata(dataset_path)['fields']:
        if field['type'].lower() in type_function_map:
            attributes.update_by_function(
                dataset_path, field['name'],
                function=type_function_map[field['type'].lower()],
                dataset_where_sql=kwargs['dataset_where_sql'],
                log_level=None
                )
    LOG.log(log_level, "End: Adjust.")
    return dataset_path


def view_chunks(dataset_path, chunk_size, **kwargs):
    """Generator for creating & referencing views of 'chunks' of a dataset.

    Wraps create_view.
    Yields view name as a string.

    Args:
        dataset_path (str): Path of dataset.
        chunk_size (int): Number of features in each chunk-view.
    Kwargs:
        force_nonspatial (bool): Flag ensure view is nonspatial.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level at which to log this function.
    Yields:
        str.
    """
    for kwarg_default in [('dataset_where_sql', None),
                          ('force_nonspatial', False), ('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    chunk_where_sql_template = "{field} >= {from_oid} and {field} <= {to_oid}"
    if kwargs['dataset_where_sql']:
        chunk_where_sql_template += " and ({})".format(
            kwargs['dataset_where_sql']
            )
    # Get iterable of all O in the dataset.
    # Sorting is important, allows views with ID range instead of list.
    oids = attributes.as_iters(dataset_path, ('oid@',),
                               dataset_where_sql=kwargs['dataset_where_sql'])
    oids = sorted(oid for oid, in oids)
    while oids:
        chunk = oids[:chunk_size]  # Get chunk OIDs
        oids = oids[chunk_size:]  # Remove them from set.
        # ArcPy where clauses cannot use 'between'.
        kwargs['dataset_where_sql'] = chunk_where_sql_template.format(
            field=dataset.metadata(dataset_path)['oid_field_name'],
            from_oid=chunk[0], to_oid=chunk[-1]
            )
        yield dataset.create_view(helpers.unique_name('chunk'), dataset_path,
                                  **kwargs)
