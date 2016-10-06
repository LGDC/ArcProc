# -*- coding=utf-8 -*-
"""Conversion operations."""

import collections
import csv
import logging

import arcpy

from arcetl import arcobj, attributes, dataset
from arcetl.helpers import LOG_LEVEL_MAP, unique_name


LOG = logging.getLogger(__name__)


def planarize(dataset_path, output_path, **kwargs):
    """Planarize feature geometry into lines.

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
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Planarize geometry in %s to lines in %s.",
            dataset_path, output_path)
    view_name = dataset.create_view(
        unique_name(), dataset_path,
        dataset_where_sql=kwargs['dataset_where_sql'], log_level=None
        )
    arcpy.management.FeatureToLine(
        in_features=view_name, out_feature_class=output_path,
        cluster_tolerance=kwargs['tolerance'], attributes=True
        )
    dataset.delete(view_name, log_level=None)
    LOG.log(log_level, "End: Planarize.")
    return output_path


def polygons_to_lines(dataset_path, output_path, **kwargs):
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
    for kwarg_default in [('dataset_where_sql', None), ('id_field_name', None),
                          ('log_level', 'info'), ('tolerance', None),
                          ('topological', False)]:
        kwargs.setdefault(*kwarg_default)
    if not kwargs['topological']:
        # Tolerance only applies to topological conversions.
        kwargs['tolerance'] = None
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Convert polgyons in %s to lines in %s.",
            dataset_path, output_path)
    dataset_meta = dataset.metadata(dataset_path)
    view_name = dataset.create_view(
        unique_name(), dataset_path,
        dataset_where_sql=kwargs['dataset_where_sql'], log_level=None
        )
    if kwargs['tolerance']:
        old_tolerance = arcpy.env.XYTolerance
        arcpy.env.XYTolerance = kwargs['tolerance']
    arcpy.management.PolygonToLine(in_features=view_name,
                                   out_feature_class=output_path,
                                   neighbor_option=kwargs['topological'])
    if kwargs['tolerance']:
        arcpy.env.XYTolerance = old_tolerance
    dataset.delete(view_name, log_level=None)
    if kwargs['topological']:
        for side in ('left', 'right'):
            side_meta = {'oid_field_name': '{}_FID'.format(side.upper())}
            if kwargs['id_field_name']:
                side_meta['id_field'] = dataset.field_metadata(
                    dataset_path, kwargs['id_field_name']
                    )
                side_meta['id_field']['name'] = '{}_{}'.format(
                    side, kwargs['id_field_name']
                    )
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
                    log_level=None
                    )
            dataset.delete_field(output_path, side_meta['oid_field_name'],
                                 log_level=None)
    else:
        dataset.delete_field(output_path, 'ORIG_FID', log_level=None)
    LOG.log(log_level, "End: Convert.")
    return output_path


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
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Project %s to srid=%s in %s.",
            dataset_path, spatial_reference_id, output_path)
    dataset_meta = dataset.metadata(dataset_path)
    # Project tool cannot output to an in-memory workspace (will throw error
    # 000944). Not a bug. Esri's Project documentation (as of v10.4)
    # specifically states: "The in_memory workspace is not supported as a
    # location to write the output dataset." To avoid all this ado, using
    # create a clone dataset & copy features.
    dataset.create(
        output_path,
        field_metadata_list=[
            field for field in dataset_meta['fields']
            if field['type'].lower() not in ('geometry ', 'oid')
            ],
        geometry_type=dataset_meta['geometry_type'],
        spatial_reference_id=spatial_reference_id, log_level=None
        )
    dataset.copy(dataset_path, output_path,
                 dataset_where_sql=kwargs['dataset_where_sql'], log_level=None)
    LOG.log(log_level, "End: Project.")
    return output_path


def rows_to_csvfile(rows, output_path, field_names, **kwargs):
    """Write collection of rows to a CSV-file.

    Rows can be represented by either dictionaries or sequences.

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
    for kwarg_default in [('file_mode', 'wb'), ('header', False),
                          ('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Convert rows to CSVfile %s.", output_path)
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


def table_to_points(dataset_path, output_path, x_field_name, y_field_name,
                    z_field_name=None, spatial_reference_id=4326, **kwargs):
    """Convert coordinate table to a new point dataset.

    Not supplying a spatial reference ID defaults to unprojected WGS84.

    Args:
        dataset_path (str): Path of dataset.
        output_path (str): Path of output dataset.
        x_field_name (str): Name of field with x-coordinate.
        y_field_name (str): Name of field with y-coordinate.
        z_field_name (str): Name of field with z-coordinate.
        spatial_reference_id (int): EPSG code indicating the spatial reference
            the coordinates and output geometry are/will be in.
    Kwargs:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('dataset_where_sql', None), ('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Convert %s to spatial dataset %s.",
            dataset_path, output_path)
    view_name = unique_name()
    arcpy.management.MakeXYEventLayer(
        table=dataset_path, out_layer=view_name, in_x_field=x_field_name,
        in_y_field=y_field_name, in_z_field=z_field_name,
        spatial_reference=arcobj.spatial_reference_as_arc(spatial_reference_id)
        )
    dataset.copy(view_name, output_path,
                 dataset_where_sql=kwargs['dataset_where_sql'], log_level=None)
    dataset.delete(view_name, log_level=None)
    LOG.log(log_level, "End: Convert.")
    return output_path
