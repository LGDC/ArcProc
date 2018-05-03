"""Conversion operations."""
from collections import Sequence
import csv
import logging

import arcpy

from arcetl import arcobj
from arcetl import attributes
from arcetl import dataset
from arcetl.helpers import leveled_logger, unique_name


LOG = logging.getLogger(__name__)


def planarize(dataset_path, output_path, **kwargs):
    """Planarize feature geometry into lines.

    Note:
        This method does not make topological linework. However it does carry
        all attributes with it, rather than just an ID attribute.

        Since this method breaks the new line geometry at intersections, it
        can be useful to break line geometry features at them.

    Args:
        dataset_path (str): Path of the dataset.
        output_path (str): Path of the output dataset.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        tolerance (float): Tolerance for coincidence, in dataset's units.
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        str: Path of the converted dataset.
    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('tolerance')
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    log("Start: Planarize geometry in %s to lines in %s.", dataset_path, output_path)
    with arcobj.DatasetView(dataset_path,
                            kwargs.get('dataset_where_sql')) as dataset_view:
        arcpy.management.FeatureToLine(
            in_features=dataset_view.name, out_feature_class=output_path,
            cluster_tolerance=kwargs.get('tolerance'), attributes=True
            )
    log("End: Planarize.")
    return output_path


def polygons_to_lines(dataset_path, output_path, **kwargs):
    """Convert geometry from polygons to lines.

    Note:
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
        dataset_path (str): Path of the dataset.
        output_path (str): Path of the output dataset.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        id_field_name (str): Name of the field to apply ID to lines from.
        topological (bool): Flag to indicate lines should be topological, or merged
            where lines overlap. Default is False.
        tolerance (float): Tolerance for coincidence, in dataset's units.
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        str: Path of the converted dataset.
    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('id_field_name')
    kwargs.setdefault('topological', False)
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    log("Start: Convert polgyons in %s to lines in %s.", dataset_path, output_path)
    # Tolerance only applies to topological conversions.
    if not kwargs.get('topological', False):
        kwargs['tolerance'] = None
    dataset_meta = arcobj.dataset_metadata(dataset_path)
    with arcobj.DatasetView(dataset_path,
                            kwargs.get('dataset_where_sql')) as dataset_view:
        if kwargs.get('tolerance') is not None:
            old_tolerance = arcpy.env.XYTolerance
            arcpy.env.XYTolerance = kwargs['tolerance']
        arcpy.management.PolygonToLine(
            in_features=dataset_view.name, out_feature_class=output_path,
            neighbor_option=kwargs.get('topological', False)
            )
        if kwargs.get('tolerance') is not None:
            arcpy.env.XYTolerance = old_tolerance
    if kwargs.get('topological', False):
        for side in ('left', 'right'):
            side_meta = {'oid_field_name': '{}_FID'.format(side.upper())}
            if kwargs.get('id_field_name'):
                side_meta['id_field'] = arcobj.field_metadata(
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
    log("End: Convert.")
    return output_path


def project(dataset_path, output_path, spatial_reference_item=4326, **kwargs):
    """Project dataset features to a new dataset.

    Args:
        dataset_path (str): Path of the dataset.
        output_path (str): Path of the output dataset.
        spatial_reference_item: Item from which the output geometry's spatial reference
            will be derived. Default is 4326 (EPSG code for unprojected WGS84).
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        str: Path of the converted dataset.
    """
    kwargs.setdefault('dataset_where_sql')
    log = leveled_logger(LOG, kwargs.get('log_level', 'info'))
    sref = arcobj.spatial_reference(spatial_reference_item)
    log("Start: Project %s to srid=%s in %s.",
        dataset_path, sref.factoryCode, output_path)
    dataset_meta = arcobj.dataset_metadata(dataset_path)
    # Project tool cannot output to an in-memory workspace (will throw error
    # 000944). Not a bug. Esri's Project documentation (as of v10.4)
    # specifically states: "The in_memory workspace is not supported as a
    # location to write the output dataset." To avoid all this ado, using
    # create a clone dataset & copy features.
    field_metas = (field for field in dataset_meta['fields']
                   if field['type'].lower() not in ('geometry ', 'oid'))
    dataset.create(output_path, field_metas,
                   geometry_type=dataset_meta['geometry_type'],
                   spatial_reference_item=sref, log_level=None)
    dataset.copy(dataset_path, output_path,
                 dataset_where_sql=kwargs.get('dataset_where_sql'),
                 log_level=None)
    log("End: Project.")
    return output_path


def rows_to_csvfile(rows, output_path, field_names, **kwargs):
    """Write collection of rows to a CSV-file.

    Note:
        Rows can be represented by either dictionaries or sequences.

    Args:
        rows (iter): Collection of dictionaries or sequences representing
            rows.
        output_path (str): Path of the output dataset.
        field_names (iter): Collection of the field names, in the desired
            order or output.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        header (bool): Flag to indicate whether to write a header in the output.
            Default is False.
        file_mode (str): Code indicating the file mode for writing. Default is 'wb'.
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        str: Path of the CSV-file.

    """
    kwargs.setdefault('header', False)
    kwargs.setdefault('file_mode', 'wb')
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    log("Start: Convert rows to CSVfile %s.", output_path)
    field_names = tuple(field_names)
    with open(output_path, kwargs.get('file_mode', 'wb')) as csvfile:
        for index, row in enumerate(rows):
            if index == 0:
                if isinstance(row, dict):
                    writer = csv.DictWriter(csvfile, field_names)
                    if kwargs['header']:
                        writer.writeheader()
                elif isinstance(row, Sequence):
                    writer = csv.writer(csvfile)
                    if kwargs['header']:
                        writer.writerow(field_names)
                else:
                    raise TypeError("Rows must be dictionaries or sequences.")
            writer.writerow(row)
    log("End: Write.")
    return output_path


def table_to_points(dataset_path, output_path, x_field_name, y_field_name,
                    spatial_reference_item=4326, **kwargs):
    """Convert coordinate table to a new point dataset.

    Args:
        dataset_path (str): Path of the dataset.
        output_path (str): Path of the output dataset.
        x_field_name (str): Name of the field with x-coordinate.
        y_field_name (str): Name of the field with y-coordinate.
        spatial_reference_item: Item from which the output geometry's spatial
            reference will be derived. Defaults to 4326 (EPSG code for
            unprojected WGS84).
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        z_field_name (str): Name of the field with z-coordinate.
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        str: Path of the converted dataset.

    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('z_field_name')
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    log("Start: Convert %s to spatial dataset %s.", dataset_path, output_path)
    view_name = unique_name()
    sref = arcobj.spatial_reference(spatial_reference_item)
    arcpy.management.MakeXYEventLayer(
        table=dataset_path, out_layer=view_name, in_x_field=x_field_name,
        in_y_field=y_field_name, in_z_field=kwargs.get('z_field_name'),
        spatial_reference=sref
        )
    dataset.copy(view_name, output_path,
                 dataset_where_sql=kwargs.get('dataset_where_sql'),
                 log_level=None)
    dataset.delete(view_name, log_level=None)
    log("End: Convert.")
    return output_path
