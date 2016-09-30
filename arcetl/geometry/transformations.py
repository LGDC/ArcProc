# -*- coding=utf-8 -*-
"""Geometric transformation operations."""
import logging

import arcpy

from .. import helpers, metadata
from arcetl import attributes, dataset, features


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
    log_level = helpers.LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Convert %s to spatial dataset %s.",
            dataset_path, output_path)
    LOG.log(log_level, "%s features.", metadata.feature_count(dataset_path))
    dataset_view_name = helpers.unique_name('view')
    arcpy.management.MakeXYEventLayer(
        table=dataset_path, out_layer=dataset_view_name,
        in_x_field=x_field_name, in_y_field=y_field_name,
        in_z_field=z_field_name,
        spatial_reference=(
            arcpy.SpatialReference(kwargs['spatial_reference_id'])
            if kwargs.get('spatial_reference_id') else None))
    dataset.copy(dataset_view_name, output_path,
                         dataset_where_sql=kwargs['dataset_where_sql'])
    dataset.delete(dataset_view_name)
    LOG.log(log_level, "End: Convert.")
    LOG.log(log_level, "%s features.", metadata.feature_count(output_path))
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
        tolerance (float): Tolerance for coincidence, in dataset's units.
        id_field_name (str): Name of field to apply ID to lines from.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [
            ('dataset_where_sql', None), ('id_field_name', None),
            ('log_level', 'info'), ('tolerance', None),
            ('topological', False)]:
        kwargs.setdefault(*kwarg_default)
    if not kwargs['topological']:
        # Tolerance only applies to topological conversions.
        kwargs['tolerance'] = None
    log_level = helpers.LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(
        log_level,
        "Start: Convert polgyon features in %s to line features in %s.",
        dataset_path, output_path)
    LOG.log(log_level, "%s features.", metadata.feature_count(dataset_path))
    dataset_meta = metadata.dataset_metadata(dataset_path)
    dataset_view_name = dataset.create_view(
        helpers.unique_name('view'), dataset_path,
        dataset_where_sql=kwargs['dataset_where_sql'])
    if kwargs['tolerance']:
        old_tolerance = arcpy.env.XYTolerance
        arcpy.env.XYTolerance = kwargs['tolerance']
    arcpy.management.PolygonToLine(
        in_features=dataset_view_name, out_feature_class=output_path,
        neighbor_option=kwargs['topological'])
    if kwargs['tolerance']:
        arcpy.env.XYTolerance = old_tolerance
    dataset.delete(dataset_view_name)
    if kwargs['topological']:
        for side in ('left', 'right'):
            side_meta = {'oid_field_name': '{}_FID'.format(side.upper())}
            if kwargs['id_field_name']:
                side_meta['id_field'] = metadata.field_metadata(
                    dataset_path, kwargs['id_field_name'])
                side_meta['id_field']['name'] = '{}_{}'.format(
                    side, kwargs['id_field_name'])
                # Cannot create an OID-type field, so force to long.
                if side_meta['id_field']['type'].lower() == 'oid':
                    side_meta['id_field']['type'] = 'long'
                dataset.add_field_from_metadata(
                    output_path, side_meta['id_field'], log_level=None
                    )
                attributes.update_by_joined_value(
                    dataset_path=output_path,
                    field_name=side_meta['id_field']['name'],
                    join_dataset_path=dataset_path,
                    join_field_name=kwargs['id_field_name'],
                    on_field_pairs=[(side_meta['oid_field_name'],
                                     dataset_meta['oid_field_name'])],
                    log_level=None)
            dataset.delete_field(output_path, side_meta['oid_field_name'],
                                 log_level=None)
    else:
        dataset.delete_field(output_path, 'ORIG_FID', log_level=None)
    LOG.log(log_level, "End: Convert.")
    LOG.log(log_level, "%s features.", metadata.feature_count(output_path))
    return output_path


def eliminate_interior_rings(dataset_path, **kwargs):
    """Merge features that share values in given fields.

    Args:
        dataset_path (str): Path of dataset.
    Kwargs:
        max_area (float, str): Maximum area under which parts are eliminated.
            Numeric area will be in dataset's units. String area will be
            formatted as '{number} {unit}'.
        max_percent_total_area (float): Maximum percent of total area under
            which parts are eliminated. Default is 100.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('dataset_where_sql', None), ('log_level', 'info'),
                          ('max_area', None), ('max_percent_total_area', None)]:
        kwargs.setdefault(*kwarg_default)
    # Only set max_percent_total_area default if neither it or area defined.
    if all([kwargs['max_area'] is None,
            kwargs['max_percent_total_area'] is None]):
        kwargs['max_percent_total_area'] = 99.9999
        kwargs['condition'] = 'percent'
    elif all([kwargs['max_area'] is not None,
              kwargs['max_percent_total_area'] is not None]):
        kwargs['condition'] = 'area_or_percent'
    elif kwargs['max_area'] is not None:
        kwargs['condition'] = 'area'
    else:
        kwargs['condition'] = 'percent'
    log_level = helpers.LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Eliminate interior rungs in %s.", dataset_path)
    dataset_view_name = dataset.create_view(
        helpers.unique_name('view'), dataset_path,
        dataset_where_sql=kwargs['dataset_where_sql'])
    temp_output_path = helpers.unique_temp_dataset_path('output')
    arcpy.management.EliminatePolygonPart(
        in_features=dataset_view_name, out_feature_class=temp_output_path,
        condition=kwargs['condition'], part_area=kwargs['max_area'],
        part_area_percent=kwargs['max_percent_total_area'],
        part_option='contained_only')
    # Delete un-eliminated features that are now eliminated (in the temp).
    features.delete(dataset_view_name)
    dataset.delete(dataset_view_name)
    # Copy the dissolved features (in the temp) to the dataset.
    features.insert_from_path(dataset_path, temp_output_path)
    dataset.delete(temp_output_path)
    LOG.log(log_level, "End: Eliminate.")
    return dataset_path


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
        tolerance (float): Tolerance for coincidence, in dataset's units.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('dataset_where_sql', None), ('log_level', 'info'),
                          ('tolerance', None)]:
        kwargs.setdefault(*kwarg_default)
    log_level = helpers.LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(
        log_level, "Start: Planarize features in %s to line features in %s.",
        dataset_path, output_path)
    LOG.log(log_level, "%s features.", metadata.feature_count(dataset_path))
    dataset_view_name = dataset.create_view(
        helpers.unique_name('view'), dataset_path,
        dataset_where_sql=kwargs['dataset_where_sql'])
    arcpy.management.FeatureToLine(
        in_features=dataset_view_name, out_feature_class=output_path,
        cluster_tolerance=kwargs['tolerance'], attributes=True)
    dataset.delete(dataset_view_name)
    LOG.log(log_level, "End: Planarize.")
    LOG.log(log_level, "%s features.", metadata.feature_count(output_path))
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
    log_level = helpers.LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Project %s to EPSG=%s in %s.",
            dataset_path, spatial_reference_id, output_path)
    dataset_meta = metadata.dataset_metadata(dataset_path)
    # Project tool cannot output to an in-memory workspace (will throw error
    # 000944). Not a bug. Esri's Project documentation (as of v10.4)
    # specifically states: "The in_memory workspace is not supported as a
    # location to write the output dataset." To avoid all this ado, using
    # create a clone dataset & copy features.
    dataset.create(
        output_path,
        field_metadata_list=[
            field for field in dataset_meta['fields']
            if field['type'].lower() not in ('geometry ', 'oid')],
        geometry_type=dataset_meta['geometry_type'],
        spatial_reference_id=spatial_reference_id)
    dataset.copy(dataset_path, output_path,
                         dataset_where_sql=kwargs['dataset_where_sql'])
    LOG.log(log_level, "End: Project.")
    return output_path
