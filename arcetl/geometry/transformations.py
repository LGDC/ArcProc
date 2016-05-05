# -*- coding=utf-8 -*-
"""Geometric transformation operations."""
import logging

import arcpy

from .. import arcwrap, fields, helpers, properties


LOG = logging.getLogger(__name__)


@helpers.log_function
def convert_dataset_to_spatial(dataset_path, output_path, x_field_name,
                               y_field_name, z_field_name=None, **kwargs):
    """Convert nonspatial coordinate table to a new spatial dataset.

    Args:
        dataset_path (str): Path of dataset.
        output_path (str): Path of output dataset.
    Kwargs:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        spatial_reference_id (int): EPSG code indicating the spatial reference
            the coordinates and output geometry are/will be in.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('dataset_where_sql', None), ('log_level', 'info'),
                          ('spatial_reference_id', 4326)]:
        kwargs.setdefault(*kwarg_default)
    meta = {
        'description': "Convert {} to spatial dataset.".format(dataset_path),
        'dataset_view_name': helpers.unique_name('view'),
        'spatial_reference': (
            arcpy.SpatialReference(kwargs['spatial_reference_id'])
            if kwargs.get('spatial_reference_id') else None)}
    helpers.log_line('start', meta['description'], kwargs['log_level'])
    try:
        arcpy.management.MakeXYEventLayer(
            table=dataset_path, out_layer=meta['dataset_view_name'],
            in_x_field=x_field_name, in_y_field=y_field_name,
            in_z_field=z_field_name,
            spatial_reference=kwargs['spatial_reference'])
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    arcwrap.copy_dataset(meta['dataset_view_name'], output_path,
                         dataset_where_sql=kwargs['dataset_where_sql'])
    arcwrap.delete_dataset(meta['dataset_view_name'])
    helpers.log_line('end', meta['description'], kwargs['log_level'])
    return output_path


@helpers.log_function
def convert_polygons_to_lines(dataset_path, output_path, **kwargs):
    """Convert geometry from polygons to lines.

    If topological is set to True, shared outlines will be a single,
    separate feature. Note that one cannot pass attributes to a
    topological transformation (as the values would not apply to all
    adjacent features).

    If an id field name is specified, the output dataset will identify the
    input features that defined the line feature with the name & values
    from the provided field. This option will be ignored if the output is
    non-topological lines, as the field will pass over with the rest of
    the attributes.

    Args:
        dataset_path (str): Path of dataset.
        output_path (str): Path of output dataset.
    Kwargs:
        topological (bool): Flag indicating if lines should be topological, or
            merge overlapping lines.
        id_field_name (str): Name of field to apply ID to lines from.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('topological', False), ('id_field_name', None),
                          ('dataset_where_sql', None), ('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    meta = {
        'description': "Convert polygon features in {} to lines.".format(
            dataset_path),
        'dataset': properties.dataset_metadata(dataset_path),
        'dataset_view_name': arcwrap.create_dataset_view(
            helpers.unique_name('view'), dataset_path,
            dataset_where_sql=kwargs['dataset_where_sql'])}
    helpers.log_line('start', meta['description'], kwargs['log_level'])
    try:
        arcpy.management.PolygonToLine(
            in_features=meta['dataset_view_name'],
            out_feature_class=output_path,
            neighbor_option=kwargs['topological'])
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    arcwrap.delete_dataset(meta['dataset_view_name'])
    if kwargs['topological']:
        meta['id_field_metadata'] = properties.field_metadata(
            dataset_path, kwargs['id_field_name'])
        for side in ('left', 'right'):
            meta[side] = dict()
            meta[side]['oid_field_name'] = '{}_FID'.format(side.upper())
            if kwargs['id_field_name']:
                meta[side]['id_field'] = meta['id_field_metadata'].copy()
                meta[side]['id_field']['name'] = '{}_{}'.format(
                    side, kwargs['id_field_name'])
                # Cannot create an OID-type field, so force to long.
                if meta[side]['id_field']['type'].lower() == 'oid':
                    meta[side]['id_field']['type'] = 'long'
                fields.add_fields_from_metadata_list(
                    output_path, metadata_list=[meta[side]['id_field']],
                    log_level=None)
                fields.update_field_by_joined_value(
                    dataset_path=output_path,
                    field_name=meta[side]['id_field']['name'],
                    join_dataset_path=dataset_path,
                    join_field_name=kwargs['id_field_name'],
                    on_field_pairs=[(meta[side]['oid_field_name'],
                                     meta['dataset']['oid_field_name'])],
                    log_level=None)
            fields.delete_field(
                output_path, meta[side]['oid_field_name'], log_level=None)
    else:
        fields.delete_field(output_path, 'ORIG_FID', log_level=None)
    helpers.log_line('end', meta['description'], kwargs['log_level'])
    return output_path


@helpers.log_function
def planarize_features(dataset_path, output_path, **kwargs):
    """Convert feature geometry to lines - planarizing them.

    This method does not make topological linework. However it does carry
    all attributes with it, rather than just an ID attribute.

    Since this method breaks the new line geometry at intersections, it
    can be useful to break line geometry features at them.

    Args:
        dataset_path (str): Path of dataset.
        output_path (str): Path of output dataset.
    Kwargs:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('dataset_where_sql', None), ('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    meta = {
        'description': "Planarize features in {}.".format(dataset_path),
        'dataset_view_name': arcwrap.create_dataset_view(
            helpers.unique_name('view'), dataset_path,
            dataset_where_sql=kwargs['dataset_where_sql'])}
    helpers.log_line('start', meta['description'], kwargs['log_level'])
    try:
        arcpy.management.FeatureToLine(
            in_features=meta['dataset_view_name'],
            out_feature_class=output_path, ##cluster_tolerance,
            attributes=True)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    arcwrap.delete_dataset(meta['dataset_view_name'])
    helpers.log_line('end', meta['description'], kwargs['log_level'])
    return output_path


@helpers.log_function
def project(dataset_path, output_path, spatial_reference_id=4326, **kwargs):
    """Project dataset features to a new dataset.

    Not supplying a spatial reference ID defaults to unprojected WGS84.

    Args:
        dataset_path (str): Path of dataset.
        output_path (str): Path of output dataset.
        spatial_reference_id (int): EPSG code indicating the spatial reference
            output geometry will be in.
    Kwargs:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('dataset_where_sql', None), ('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    meta = {
        'description': "Project {} to EPSG={}.".format(
            dataset_path, spatial_reference_id),
        'dataset': properties.dataset_metadata(dataset_path),
        'spatial_reference': (arcpy.SpatialReference(spatial_reference_id)
                              if spatial_reference_id else None)}
    helpers.log_line('start', meta['description'], kwargs['log_level'])
    # Project tool cannot output to an in-memory workspace (will throw error
    # 000944). Not a bug. Esri's Project documentation (as of v10.4)
    # specifically states: "The in_memory workspace is not supported as a
    # location to write the output dataset." To avoid all this ado, using
    # create a clone dataset & copy features.
    arcwrap.create_dataset(
        output_path,
        field_metadata_list=[
            field for field in meta['dataset']['fields']
            if field['type'].lower() not in ('geometry ', 'oid')],
        geometry_type=meta['dataset']['geometry_type'],
        spatial_reference_id=spatial_reference_id)
    arcwrap.copy_dataset(dataset_path, output_path,
                         dataset_where_sql=kwargs['dataset_where_sql'])
    helpers.log_line('end', meta['description'], kwargs['log_level'])
    return output_path
