# -*- coding=utf-8 -*-
"""ETL framework library based on ArcGIS/ArcPy."""
#pylint: disable=unused-import
from arcetl.etl import ArcETL
from .helpers import (
    sexagesimal_angle_to_decimal, toggle_arc_extension, unique_ids,
    unique_name, unique_temp_dataset_path
    )


__version__ = '1.0'


##TODO: Find a home for these below.

import logging

LOG = logging.getLogger(__name__)


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
    from arcetl.helpers import unique_name, unique_temp_dataset_path
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
