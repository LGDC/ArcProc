# -*- coding=utf-8 -*-
"""ETL framework library based on ArcGIS/ArcPy."""
#pylint: disable=unused-import
from arcetl.etl import ArcETL
from .geometry.sets import (
    clip_features, dissolve_features, erase_features, identity_features,
    keep_features_by_location, overlay_features, union_features
    )
from .geometry.transformations import (
    convert_dataset_to_spatial, convert_polygons_to_lines,
    eliminate_interior_rings, planarize_features, project
    )
from .helpers import (
    sexagesimal_angle_to_decimal, toggle_arc_extension, unique_ids,
    unique_name, unique_temp_dataset_path
    )


__version__ = '1.0'


##TODO: Find a home for these below.

import logging

LOG = logging.getLogger(__name__)

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
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    import datetime
    from arcetl import attributes, dataset
    from arcetl.helpers import LOG_LEVEL_MAP
    for kwarg_default in [
            ('datetime_null_replacement', datetime.date.min),
            ('integer_null_replacement', 0), ('numeric_null_replacement', 0.0),
            ('string_null_replacement', ''), ('log_level', 'info')
        ]:
        kwargs.setdefault(*kwarg_default)
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
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
    dataset_meta = dataset.metadata(dataset_path)
    for field in dataset_meta['fields']:
        if field['type'].lower() in type_function_map:
            attributes.update_by_function(
                dataset_path, field['name'],
                function=type_function_map[field['type'].lower()],
                log_level=None
                )
    LOG.log(log_level, "End: Adjust.")
    return dataset_path


def near_features_as_dicts(dataset_path, dataset_id_field_name,
                           near_dataset_path, near_id_field_name, **kwargs):
    """Generator for near-feature pairs.

    Yielded dictionary has the following keys:
    {id, near_id, distance, rank, angle, coordinates, x, y}.
    Setting max_near_distance to NoneType will generate every possible
    feature cross-reference.
    Setting only_closest to True will generate a cross reference only with
    the closest feature.
    Distance values will match the linear unit of the main dataset.
    Angle values are in decimal degrees.

    Args:
        dataset_path (str): Path of dataset.
        dataset_id_field_name (str): Name of ID field.
        near_dataset_path (str): Path of near-dataset.
        near_id_field_name (str): Name of near ID field.
    Kwargs:
        max_near_distance (float): Maximum distance to search for near-
            features, in units of the dataset's spatial reference.
        only_closest (bool): Flag indicating only closest feature  will be
            cross-referenced.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        near_where_sql (str): SQL where-clause for near dataset subselection.
    Yields:
        dict.
    """
    import arcpy
    from arcetl import attributes, dataset
    from helpers import unique_name, unique_temp_dataset_path
    for kwarg_default in [
            ('dataset_where_sql', None), ('max_near_distance', None),
            ('near_where_sql', None), ('only_closest', False)]:
        kwargs.setdefault(*kwarg_default)
    dataset_view_name = dataset.create_view(
        unique_name('view'), dataset_path,
        dataset_where_sql=kwargs['dataset_where_sql'], log_level=None)
    near_dataset_view_name = dataset.create_view(
        unique_name('view'), near_dataset_path,
        dataset_where_sql=kwargs['near_where_sql'], log_level=None)
    temp_near_path = unique_temp_dataset_path('near')
    arcpy.analysis.GenerateNearTable(
        in_features=dataset_view_name, near_features=near_dataset_view_name,
        out_table=temp_near_path, search_radius=kwargs['max_near_distance'],
        location=True, angle=True, closest=kwargs['only_closest'],
        method='geodesic')
    dataset_oid_id_map = attributes.id_map(dataset_view_name,
                                           dataset_id_field_name)
    dataset.delete(dataset_view_name, log_level=None)
    near_oid_id_map = attributes.id_map(near_dataset_view_name,
                                        near_id_field_name)
    dataset.delete(near_dataset_view_name, log_level=None)
    with arcpy.da.SearchCursor(
        in_table=temp_near_path,
        field_names=['in_fid', 'near_fid', 'near_dist', 'near_rank',
                     'near_angle', 'near_x', 'near_y']) as cursor:
        for row in cursor:
            row_info = dict(zip(cursor.fields, row))
            yield {'id': dataset_oid_id_map[row_info['in_fid']],
                   'near_id': near_oid_id_map[row_info['near_fid']],
                   'distance': row_info['near_dist'],
                   'rank': row_info['near_rank'],
                   'angle': row_info['near_angle'],
                   'coordinates': (row_info['near_x'], row_info['near_y']),
                   'x': row_info['near_x'], 'y': row_info['near_y']}
    dataset.delete(temp_near_path, log_level=None)


def write_rows_to_csvfile(rows, output_path, field_names, **kwargs):
    """Write collected of rows to a CSV-file.

    The rows can be represented by either a dictionary or iterable.
    Args:
        rows (iter): Iterable of obejcts representing rows (iterables or
            dictionaries).
        output_path (str): Path of output dataset.
        field_names (iter): Iterable of field names, in the desired order.
    Kwargs:
        header (bool): Flag indicating whether to write a header to the output.
        file_mode (str): Code indicating the file mode for writing.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    import collections
    import csv
    from arcetl.helpers import LOG_LEVEL_MAP
    for kwarg_default in [('file_mode', 'wb'), ('header', False),
                          ('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Write iterable of row objects to CSVfile %s.",
            output_path)
    with open(output_path, kwargs['file_mode']) as csvfile:
        for index, row in enumerate(rows):
            if index == 0:
                if isinstance(row, dict):
                    writer = csv.DictWriter(csvfile, field_names)
                    if kwargs['header']:
                        writer.writeheader()
                elif isinstance(row, collections.Sequence):
                    writer = csv.writer(csvfile)
                    if kwargs['header']:
                        writer.writerow(field_names)
                else:
                    raise TypeError("Rows must be dictionaries or sequences.")
            writer.writerow(row)
    LOG.log(log_level, "End: Write.")
    return output_path
