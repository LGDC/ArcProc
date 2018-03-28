"""Analysis result operations."""
import logging

import arcpy

from arcetl import arcobj
from arcetl import attributes
from arcetl import dataset
from arcetl.helpers import unique_path


LOG = logging.getLogger(__name__)


def id_near_info_map(dataset_path, dataset_id_field_name, near_dataset_path,
                     near_id_field_name, max_near_distance=None, **kwargs):
    """Return mapping dictionary of feature IDs/near-feature info.

    Args:
        dataset_path (str): Path of the dataset.
        dataset_id_field_name (str): Name of ID field.
        near_dataset_path (str): Path of the near-dataset.
        near_id_field_name (str): Name of the near ID field.
        max_near_distance (float): Maximum distance to search for near-
            features, in units of the dataset's spatial reference.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        near_rank (int): Nearness rank of the feature to map info for.
            Defaults to 1.
        near_where_sql (str): SQL where-clause for near-dataset
            subselection.

    Returns:
        dict: Mapping of the dataset ID to a near-feature info dictionary.
            Info dictionary keys: 'id', 'near_id', 'rank', 'distance',
            'angle', 'near_x', 'near_y'.
            'distance' value (float) will match linear unit of the dataset's
            spatial reference.
            'angle' value (float) is in decimal degrees.
    """
    dataset_view = arcobj.DatasetView(dataset_path,
                                      kwargs.get('dataset_where_sql'))
    near_view = arcobj.DatasetView(near_dataset_path,
                                   kwargs.get('near_where_sql'))
    with dataset_view, near_view:
        temp_near_path = unique_path('near')
        arcpy.analysis.GenerateNearTable(
            in_features=dataset_view.name, near_features=near_view.name,
            out_table=temp_near_path, search_radius=max_near_distance,
            location=True, angle=True, closest=False,
            closest_count=kwargs.get('near_rank', 1),
            )
        oid_id_map = attributes.id_map(dataset_view.name, 'oid@',
                                       dataset_id_field_name)
        near_oid_id_map = attributes.id_map(near_view.name, 'oid@',
                                            near_id_field_name)
    field_names = ('in_fid', 'near_fid', 'near_dist', 'near_angle',
                   'near_x', 'near_y', 'near_rank')
    near_info_map = {}
    for near_info in attributes.as_dicts(temp_near_path, field_names):
        if near_info['near_rank'] == kwargs.get('near_rank', 1):
            _id = oid_id_map[near_info['in_fid']]
            near_info_map[_id] = {
                'id': _id,
                'near_id': near_oid_id_map[near_info['near_fid']],
                'rank': near_info['near_rank'],
                'distance': near_info['near_dist'],
                'angle': near_info['near_angle'],
                'near_x': near_info['near_x'], 'near_y': near_info['near_y'],
                }
    dataset.delete(temp_near_path, log_level=None)
    return near_info_map
