# -*- coding=utf-8 -*-
"""Analysis result operations."""

import logging

import arcpy

from arcetl import attributes, dataset
from arcetl.helpers import unique_name, unique_temp_dataset_path


LOG = logging.getLogger(__name__)


def id_near_info_map(dataset_path, dataset_id_field_name, near_dataset_path,
                     near_id_field_name, **kwargs):
    """Return mapping dictionary of feature IDs/near-feature info.

    Mapping structure: {
        <feature_id>: {'id': <>, 'near_id': <>, 'rank': int(),
                       'distance': float(), 'angle': float(),
                       'near_x': int(), 'near_y': int()}
        }
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
    for kwarg_default in [
            ('dataset_where_sql', None), ('max_near_distance', None),
            ('near_where_sql', None), ('only_closest', False)
        ]:
        kwargs.setdefault(*kwarg_default)
    view_name = dataset.create_view(
        unique_name('view'), dataset_path,
        dataset_where_sql=kwargs['dataset_where_sql'], log_level=None
        )
    near_view_name = dataset.create_view(
        unique_name('view'), near_dataset_path,
        dataset_where_sql=kwargs['near_where_sql'], log_level=None
        )
    temp_near_path = unique_temp_dataset_path('near')
    arcpy.analysis.GenerateNearTable(
        in_features=view_name, near_features=near_view_name,
        out_table=temp_near_path, search_radius=kwargs['max_near_distance'],
        location=True, angle=True, closest=kwargs['only_closest'],
        )
    oid_id_map = attributes.id_map(view_name, dataset_id_field_name)
    dataset.delete(view_name, log_level=None)
    near_oid_id_map = attributes.id_map(near_view_name, near_id_field_name)
    dataset.delete(near_view_name, log_level=None)
    field_names = ['in_fid', 'near_fid', 'near_dist', 'near_angle',
                   'near_x', 'near_y']
    if not kwargs['only_closest']:
        field_names.append('near_rank')
    near_info_map = {}
    for near_info in attributes.as_dicts(temp_near_path, field_names):
        near_info['id'] = oid_id_map[near_info.pop('in_fid')]
        near_info['near_id'] = near_oid_id_map[near_info.pop('near_fid')]
        near_info['rank'] = (1 if kwargs['only_closest']
                             else near_info.pop('near_rank'))
        near_info['distance'] = near_info.pop('near_dist')
        near_info['angle'] = near_info.pop('near_angle')
        near_info_map[near_info['id']] = near_info
    dataset.delete(temp_near_path, log_level=None)
    return near_info_map
