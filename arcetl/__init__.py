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
from arcetl.metadata import (
    domain_metadata, workspace_dataset_names
    )
from .values import (
    features_as_dicts, features_as_iters, near_features_as_dicts,
    oid_field_value_map, oid_field_values, oid_geometries, oid_geometry_map,
    sorted_feature_dicts, sorted_feature_iters
    )
from .workspace import (
    build_locator, build_network, compress_geodatabase, create_file_geodatabase,
    create_geodatabase_xml_backup, execute_sql_statement, write_rows_to_csvfile
    )


__version__ = '1.0'


##TODO: Find a home for this.
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
