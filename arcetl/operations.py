# -*- coding=utf-8 -*-
"""ETL operations collected for ArcETL instance transformation use."""
import collections
import csv
import logging

from . import helpers
#pylint: disable=unused-import
from .features import (
    adjust_features_for_shapefile, delete_features, insert_features_from_dicts,
    insert_features_from_iters, insert_features_from_path)
from .fields import (
    add_field, add_fields_from_metadata_list, add_index, delete_field,
    duplicate_field, join_field, rename_field, update_field_by_domain_code,
    update_field_by_expression, update_field_by_feature_match,
    update_field_by_function, update_field_by_geometry,
    update_field_by_instance_method, update_field_by_joined_value,
    update_field_by_near_feature, update_field_by_overlay,
    update_field_by_unique_id, update_geometry_node_id_fields)
from .geometry.constructs import generate_service_areas, generate_service_rings
from .geometry.sets import (
    clip_features, dissolve_features, erase_features, identity_features,
    keep_features_by_location, overlay_features, union_features)
from .geometry.transformations import (
    convert_dataset_to_spatial, convert_polygons_to_lines, planarize_features,
    project)
from .workspace import (
    build_network, compress_geodatabase, copy_dataset, create_dataset,
    create_file_geodatabase, create_geodatabase_xml_backup, delete_dataset,
    execute_sql_statement, set_dataset_privileges)


LOG = logging.getLogger(__name__)


@helpers.log_function
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
    for kwarg_default in [('file_mode', 'wb'), ('header', False),
                          ('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    log_level = helpers.LOG_LEVEL_MAP[kwargs['log_level']]
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
                    raise TypeError(
                        "Row objects must be dictionaries or sequences.")
            writer.writerow(row)
    LOG.log(log_level, "End: Write.")
    LOG.log(log_level, "%s rows.", len(rows))
    return output_path
