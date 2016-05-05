# -*- coding=utf-8 -*-
"""Dataset values objects."""
import logging

import arcpy

from . import arcwrap, helpers


LOG = logging.getLogger(__name__)


def features_as_iters(dataset_path, field_names=None, **kwargs):
    """Generator for iterables of feature attributes.

    Args:
        dataset_path (str): Path of dataset.
        field_names (iter): Iterable of field names.
    Kwargs:
        iter_type (object): Python iterable type to yield.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        spatial_reference_id (int): EPSG code indicating the spatial reference
            output geometry will be in.
    Yields:
        iter.
    """
    for kwarg_default in [('dataset_where_sql', None), ('iter_type', tuple),
                          ('spatial_reference_id', None)]:
        kwargs.setdefault(*kwarg_default)
    meta = {'spatial_reference': (
        arcpy.SpatialReference(kwargs['spatial_reference_id'])
        if kwargs.get('spatial_reference_id') else None)}
    #pylint: disable=no-member
    with arcpy.da.SearchCursor(
        #pylint: enable=no-member
        in_table=dataset_path, field_names=field_names if field_names else '*',
        where_clause=kwargs['dataset_where_sql'],
        spatial_reference=meta['spatial_reference']) as cursor:
        for feature in cursor:
            yield kwargs['iter_type'](feature)


def features_as_dicts(dataset_path, field_names=None, **kwargs):
    """Generator for dictionaries of feature attributes.

    Args:
        dataset_path (str): Path of dataset.
        field_names (iter): Iterable of field names.
    Kwargs:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        spatial_reference_id (int): EPSG code indicating the spatial reference
            output geometry will be in.
    Yields:
        dict.
    """
    for kwarg_default in [
            ('dataset_where_sql', None), ('spatial_reference_id', None)]:
        kwargs.setdefault(*kwarg_default)
    meta = {'spatial_reference': (
        arcpy.SpatialReference(kwargs['spatial_reference_id'])
        if kwargs.get('spatial_reference_id') else None)}
    #pylint: disable=no-member
    with arcpy.da.SearchCursor(
        #pylint: enable=no-member
        in_table=dataset_path, field_names=field_names if field_names else '*',
        where_clause=kwargs['dataset_where_sql'],
        spatial_reference=meta['spatial_reference']) as cursor:
        for feature in cursor:
            yield dict(zip(cursor.fields, feature))


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
    for kwarg_default in [
            ('dataset_where_sql', None), ('max_near_distance', None),
            ('near_where_sql', None), ('only_closest', False)]:
        kwargs.setdefault(*kwarg_default)
    meta = {
        'dataset_view_name': arcwrap.create_dataset_view(
            helpers.unique_name('view'), dataset_path,
            dataset_where_sql=kwargs['dataset_where_sql']),
        'near_dataset_view_name': arcwrap.create_dataset_view(
            helpers.unique_name('view'), near_dataset_path,
            dataset_where_sql=kwargs['near_where_sql']),
        'temp_near_path': helpers.unique_temp_dataset_path('near'),
        'near_field_names': ('in_fid', 'near_fid', 'near_dist', 'near_rank',
                             'near_angle', 'near_x', 'near_y')}
    try:
        arcpy.analysis.GenerateNearTable(
            in_features=meta['dataset_view_name'],
            near_features=meta['near_dataset_view_name'],
            out_table=meta['temp_near_path'],
            search_radius=kwargs['max_near_distance'],
            location=True,
            angle=True, closest=kwargs['only_closest'],
            method='geodesic')
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    dataset_oid_id_map = oid_field_value_map(
        meta['dataset_view_name'], dataset_id_field_name)
    near_oid_id_map = oid_field_value_map(
        meta['near_dataset_view_name'], near_id_field_name)
    #pylint: disable=no-member
    with arcpy.da.SearchCursor(
        in_table=meta['temp_near_path'],
        field_names=meta['near_field_names']) as cursor:
        #pylint: enable=no-member
        for row in cursor:
            row_info = dict(zip(cursor.fields, row))
            yield {'id': dataset_oid_id_map[row_info['in_fid']],
                   'near_id': near_oid_id_map[row_info['near_fid']],
                   'distance': row_info['near_dist'],
                   'rank': row_info['near_rank'],
                   'angle': row_info['near_angle'],
                   'coordinates': (row_info['near_x'], row_info['near_y']),
                   'x': row_info['near_x'], 'y': row_info['near_y']}
    arcwrap.delete_dataset(meta['dataset_view_name'])
    arcwrap.delete_dataset(meta['near_dataset_view_name'])
    arcwrap.delete_dataset(meta['temp_near_path'])


def oid_field_values(dataset_path, field_name, **kwargs):
    """Generator for tuples of (OID, field_value).

    Args:
        dataset_path (str): Path of dataset.
        field_name (iter): Name of field.
    Kwargs:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        spatial_reference_id (int): EPSG code indicating the spatial reference
            output geometry will be in.
    Yields:
        tuple.
    """
    for kwarg_default in [
            ('dataset_where_sql', None), ('spatial_reference_id', None)]:
        kwargs.setdefault(*kwarg_default)
    meta = {'spatial_reference': (
        arcpy.SpatialReference(kwargs['spatial_reference_id'])
        if kwargs.get('spatial_reference_id') else None)}
    #pylint: disable=no-member
    with arcpy.da.SearchCursor(
        #pylint: enable=no-member
        in_table=dataset_path, field_names=['oid@', field_name],
        where_clause=kwargs['dataset_where_sql'],
        spatial_reference=meta['spatial_reference']) as cursor:
        for oid, value in cursor:
            yield (oid, value)


def oid_field_value_map(dataset_path, field_name, **kwargs):
    """Return dictionary mapping of field value for the feature OID.

    Args:
        dataset_path (str): Path of dataset.
        field_name (str): Name of field.
    Kwargs:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        spatial_reference_id (int): EPSG code indicating the spatial reference
            output geometry will be in.
    Returns:
        dict.
    """
    for kwarg_default in [
            ('dataset_where_sql', None), ('spatial_reference_id', None)]:
        kwargs.setdefault(*kwarg_default)
    meta = {'spatial_reference': (
        arcpy.SpatialReference(kwargs['spatial_reference_id'])
        if kwargs.get('spatial_reference_id') else None)}
    #pylint: disable=no-member
    with arcpy.da.SearchCursor(
        #pylint: enable=no-member
        in_table=dataset_path, field_names=['oid@', field_name],
        where_clause=kwargs['dataset_where_sql'],
        spatial_reference=meta['spatial_reference']) as cursor:
        return {oid: value for oid, value in cursor}


def oid_geometries(dataset_path, **kwargs):
    """Generator for tuples of (OID, geometry).

    Args:
        dataset_path (str): Path of dataset.
    Kwargs:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        spatial_reference_id (int): EPSG code indicating the spatial reference
            output geometry will be in.
    Yields:
        tuple.
    """
    for kwarg_default in [
            ('dataset_where_sql', None), ('spatial_reference_id', None)]:
        kwargs.setdefault(*kwarg_default)
    meta = {'spatial_reference': (
        arcpy.SpatialReference(kwargs['spatial_reference_id'])
        if kwargs.get('spatial_reference_id') else None)}
    #pylint: disable=no-member
    with arcpy.da.SearchCursor(
        #pylint: enable=no-member
        in_table=dataset_path, field_names=['oid@', 'shape@'],
        where_clause=kwargs['dataset_where_sql'],
        spatial_reference=meta['spatial_reference']) as cursor:
        for oid, geom in cursor:
            yield (oid, geom)


def oid_geometry_map(dataset_path, **kwargs):
    """Return dictionary mapping of geometry for the feature OID.

    Args:
        dataset_path (str): Path of dataset.
    Kwargs:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        spatial_reference_id (int): EPSG code indicating the spatial reference
            output geometry will be in.
    Returns:
        dict.
    """
    for kwarg_default in [
            ('dataset_where_sql', None), ('spatial_reference_id', None)]:
        kwargs.setdefault(*kwarg_default)
    meta = {'spatial_reference': (
        arcpy.SpatialReference(kwargs['spatial_reference_id'])
        if kwargs.get('spatial_reference_id') else None)}
    #pylint: disable=no-member
    with arcpy.da.SearchCursor(
        #pylint: enable=no-member
        in_table=dataset_path, field_names=['oid@', 'shape@'],
        where_clause=kwargs['dataset_where_sql'],
        spatial_reference=meta['spatial_reference']) as cursor:
        return {oid: geom for oid, geom in cursor}

