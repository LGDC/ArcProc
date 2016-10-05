# -*- coding=utf-8 -*-
"""Feature operations."""
import inspect
import logging

import arcpy

from arcetl import dataset
from arcetl.helpers import LOG_LEVEL_MAP, unique_name, unique_temp_dataset_path


LOG = logging.getLogger(__name__)


def clip(dataset_path, clip_dataset_path, **kwargs):
    """Clip feature geometry where overlapping clip-geometry.

    Args:
        dataset_path (str): Path of dataset.
        clip_dataset_path (str): Path of dataset defining clip area.
    Kwargs:
        tolerance (float): Tolerance for coincidence, in dataset's units.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        clip_where_sql (str): SQL where-clause for clip dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('clip_where_sql', None), ('dataset_where_sql', None),
                          ('log_level', 'info'), ('tolerance', None)]:
        kwargs.setdefault(*kwarg_default)
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Clip features in %s where overlapping %s.",
            dataset_path, clip_dataset_path)
    dataset_view_name = dataset.create_view(
        unique_name('view'), dataset_path,
        dataset_where_sql=kwargs['dataset_where_sql'], log_level=None
        )
    clip_dataset_view_name = dataset.create_view(
        unique_name('view'), clip_dataset_path,
        dataset_where_sql=kwargs['clip_where_sql'], log_level=None
        )
    temp_output_path = unique_temp_dataset_path('output')
    arcpy.analysis.Clip(
        in_features=dataset_view_name, clip_features=clip_dataset_view_name,
        out_feature_class=temp_output_path,
        cluster_tolerance=kwargs['tolerance']
        )
    dataset.delete(clip_dataset_view_name, log_level=None)
    delete(dataset_view_name, log_level=None)
    dataset.delete(dataset_view_name, log_level=None)
    insert_from_path(dataset_path, temp_output_path, log_level=None)
    dataset.delete(temp_output_path, log_level=None)
    LOG.log(log_level, "End: Clip.")
    return dataset_path


def count(dataset_path, **kwargs):
    """Return number of features in dataset.

    Wraps dataset.feature_count.

    Args:
        dataset_path (str): Path of dataset.
    Kwargs:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
    Returns:
        int.
    """
    return dataset.feature_count(dataset_path, **kwargs)


def delete(dataset_path, **kwargs):
    """Delete features.

    Args:
        dataset_path (str): Path of dataset.
    Kwargs:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('dataset_where_sql', None), ('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Delete features from %s.", dataset_path)
    truncate_type_error_codes = (
        # "Only supports Geodatabase tables and feature classes."
        'ERROR 000187',
        # "Operation not supported on a versioned table."
        'ERROR 001259',
        # "Operation not supported on table {table name}."
        'ERROR 001260',
        # Operation not supported on a feature class in a controller
        # dataset.
        'ERROR 001395'
        )
    dataset_view_name = dataset.create_view(
        unique_name('view'), dataset_path,
        dataset_where_sql=kwargs['dataset_where_sql'], log_level=None
        )
    # Can use (faster) truncate when no sub-selection
    run_truncate = kwargs.get('dataset_where_sql') is None
    if run_truncate:
        try:
            arcpy.management.TruncateTable(in_table=dataset_view_name)
        except arcpy.ExecuteError:
            # Avoid arcpy.GetReturnCode(); error code position inconsistent.
            # Search messages for 'ERROR ######' instead.
            if any(code in arcpy.GetMessages()
                   for code in truncate_type_error_codes):
                LOG.debug("Truncate unsupported; will try deleting rows.")
                run_truncate = False
            else:
                raise
    if not run_truncate:
        arcpy.management.DeleteRows(in_rows=dataset_view_name)
    dataset.delete(dataset_view_name, log_level=None)
    LOG.log(log_level, "End: Delete.")
    return dataset_path


def dissolve(dataset_path, dissolve_field_names=None, **kwargs):
    """Merge features that share values in given fields.

    Args:
        dataset_path (str): Path of dataset.
        dissolve_field_names (iter): Iterable of field names to dissolve on.
    Kwargs:
        multipart (bool): Flag indicating if dissolve should create multipart
            features.
        unsplit_lines (bool): Flag indicating if dissolving lines should merge
            features when endpoints meet without a crossing feature.
        tolerance (float): Tolerance for coincidence, in dataset's units.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [
            ('dataset_where_sql', None), ('log_level', 'info'),
            ('multipart', True), ('tolerance', 0.001), ('unsplit_lines', False)
        ]:
        kwargs.setdefault(*kwarg_default)
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Dissolve features in %s on fields: %s.",
            dataset_path, dissolve_field_names)
    if kwargs['tolerance']:
        old_tolerance = arcpy.env.XYTolerance
        arcpy.env.XYTolerance = kwargs['tolerance']
    dataset_view_name = dataset.create_view(
        unique_name('view'), dataset_path,
        dataset_where_sql=kwargs['dataset_where_sql'], log_level=None
        )
    temp_output_path = unique_temp_dataset_path('output')
    arcpy.management.Dissolve(
        in_features=dataset_view_name, out_feature_class=temp_output_path,
        dissolve_field=dissolve_field_names, multi_part=kwargs['multipart'],
        unsplit_lines=kwargs['unsplit_lines']
        )
    if kwargs['tolerance']:
        arcpy.env.XYTolerance = old_tolerance
    delete(dataset_view_name, log_level=None)
    dataset.delete(dataset_view_name, log_level=None)
    insert_from_path(dataset_path, temp_output_path, log_level=None)
    dataset.delete(temp_output_path, log_level=None)
    LOG.log(log_level, "End: Dissolve.")
    return dataset_path


def eliminate_interior_rings(dataset_path, **kwargs):
    """Eliminate interior rings in polygon features.

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
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Eliminate interior rings in %s.", dataset_path)
    dataset_view_name = dataset.create_view(
        unique_name('view'), dataset_path,
        dataset_where_sql=kwargs['dataset_where_sql'], log_level=None
        )
    temp_output_path = unique_temp_dataset_path('output')
    arcpy.management.EliminatePolygonPart(
        in_features=dataset_view_name, out_feature_class=temp_output_path,
        condition=kwargs['condition'], part_area=kwargs['max_area'],
        part_area_percent=kwargs['max_percent_total_area'],
        part_option='contained_only'
        )
    delete(dataset_view_name, log_level=None)
    dataset.delete(dataset_view_name, log_level=None)
    insert_from_path(dataset_path, temp_output_path, log_level=None)
    dataset.delete(temp_output_path, log_level=None)
    LOG.log(log_level, "End: Eliminate.")
    return dataset_path


def erase(dataset_path, erase_dataset_path, **kwargs):
    """Erase feature geometry where overlaps erase dataset geometry.

    Args:
        dataset_path (str): Path of dataset.
        erase_dataset_path (str): Path of erase-dataset.
    Kwargs:
        tolerance (float): Tolerance for coincidence, in dataset's units.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        erase_where_sql (str): SQL where-clause for erase-dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [
            ('dataset_where_sql', None), ('erase_where_sql', None),
            ('log_level', 'info'), ('tolerance', None)
        ]:
        kwargs.setdefault(*kwarg_default)
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Erase features in %s where overlapping %s.",
            dataset_path, erase_dataset_path)
    dataset_view_name = dataset.create_view(
        unique_name('view'), dataset_path,
        dataset_where_sql=kwargs['dataset_where_sql'], log_level=None
        )
    erase_dataset_view_name = dataset.create_view(
        unique_name('view'), erase_dataset_path,
        dataset_where_sql=kwargs['erase_where_sql'], log_level=None
        )
    temp_output_path = unique_temp_dataset_path('output')
    arcpy.analysis.Erase(
        in_features=dataset_view_name, erase_features=erase_dataset_view_name,
        out_feature_class=temp_output_path,
        cluster_tolerance=kwargs['tolerance']
        )
    dataset.delete(erase_dataset_view_name, log_level=None)
    delete(dataset_view_name, log_level=None)
    dataset.delete(dataset_view_name, log_level=None)
    insert_from_path(dataset_path, temp_output_path, log_level=None)
    dataset.delete(temp_output_path, log_level=None)
    LOG.log(log_level, "End: Erase.")
    return dataset_path


def insert_from_dicts(dataset_path, insert_features, field_names, **kwargs):
    """Insert features from a collection of dictionaries.

    Args:
        dataset_path (str): Path of dataset.
        insert_features (iter): Iterable containing dictionaries representing
            features.
        field_names (iter): Iterable of field names to insert.
    Kwargs:
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Insert features from dictionaries into %s.",
            dataset_path)
    if inspect.isgeneratorfunction(insert_features):
        insert_features = insert_features()
    with arcpy.da.InsertCursor(dataset_path, field_names) as cursor:
        for feature in insert_features:
            cursor.insertRow([feature[name] for name in field_names])
    LOG.log(log_level, "End: Insert.")
    return dataset_path


def insert_from_iters(dataset_path, insert_features, field_names, **kwargs):
    """Insert features from a collection of iterables.

    Args:
        dataset_path (str): Path of dataset.
        insert_features (iter): Iterable containing iterables representing
            features.
        field_names (iter): Iterable of field names to insert.
    Kwargs:
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Insert features from iterables into %s.",
            dataset_path)
    if inspect.isgeneratorfunction(insert_features):
        insert_features = insert_features()
    with arcpy.da.InsertCursor(dataset_path, field_names) as cursor:
        for row in insert_features:
            cursor.insertRow(row)
    LOG.log(log_level, "End: Insert.")
    return dataset_path


def insert_from_path(dataset_path, insert_dataset_path, field_names=None,
                     **kwargs):
    """Insert features from a dataset referred to by a system path.

    Args:
        dataset_path (str): Path of dataset.
        insert_dataset_path (str): Path of insert-dataset.
        field_names (iter): Iterable of field names to insert.
    Kwargs:
        insert_where_sql (str): SQL where-clause for insert-dataset
            subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('insert_where_sql', None), ('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Insert features from dataset path %s into %s.",
            insert_dataset_path, dataset_path)
    dataset_meta = dataset.metadata(dataset_path)
    insert_dataset_meta = dataset.metadata(insert_dataset_path)
    insert_dataset_view_name = dataset.create_view(
        unique_name('view'), insert_dataset_path,
        dataset_where_sql=kwargs['insert_where_sql'],
        # Insert view must be nonspatial to append to nonspatial table.
        force_nonspatial=(not dataset_meta['is_spatial']), log_level=None
        )
    # Create field maps.
    # Added because ArcGIS Pro's no-test append is case-sensitive (verified
    # 1.0-1.1.1). BUG-000090970 - ArcGIS Pro 'No test' field mapping in
    # Append tool does not auto-map to the same field name if naming
    # convention differs.
    if field_names:
        field_names = [name.lower() for name in field_names]
    else:
        field_names = [field['name'].lower()
                       for field in dataset_meta['fields']]
    insert_field_names = [field['name'].lower()
                          for field in insert_dataset_meta['fields']]
    # Append takes care of geometry & OIDs independent of the field maps.
    for field_name_type in ['geometry_field_name', 'oid_field_name']:
        if dataset_meta.get(field_name_type):
            field_names.remove(dataset_meta[field_name_type].lower())
            insert_field_names.remove(
                insert_dataset_meta[field_name_type].lower()
                )
    field_maps = arcpy.FieldMappings()
    for field_name in field_names:
        if field_name in insert_field_names:
            field_map = arcpy.FieldMap()
            field_map.addInputField(insert_dataset_path, field_name)
            field_maps.addFieldMap(field_map)
    arcpy.management.Append(
        inputs=insert_dataset_view_name, target=dataset_path,
        schema_type='no_test', field_mapping=field_maps
        )
    dataset.delete(insert_dataset_view_name, log_level=None)
    LOG.log(log_level, "End: Insert.")
    return dataset_path


def keep_by_location(dataset_path, location_dataset_path, **kwargs):
    """Keep features where geometry overlaps location feature geometry.

    Args:
        dataset_path (str): Path of dataset.
        location_dataset_path (str): Path of location-dataset.
    Kwargs:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        location_where_sql (str): SQL where-clause for location-dataset
            subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('dataset_where_sql', None),
                          ('location_where_sql', None), ('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Keep features in %s where overlapping %s.",
            dataset_path, location_dataset_path)
    dataset_view_name = dataset.create_view(
        unique_name('view'), dataset_path,
        dataset_where_sql=kwargs['dataset_where_sql'], log_level=None
        )
    location_dataset_view_name = dataset.create_view(
        unique_name('view'), location_dataset_path,
        dataset_where_sql=kwargs['location_where_sql'], log_level=None
        )
    arcpy.management.SelectLayerByLocation(
        in_layer=dataset_view_name, overlap_type='intersect',
        select_features=location_dataset_view_name,
        selection_type='new_selection'
        )
    arcpy.management.SelectLayerByLocation(in_layer=dataset_view_name,
                                           selection_type='switch_selection')
    dataset.delete(location_dataset_view_name, log_level=None)
    delete(dataset_view_name, log_level=None)
    dataset.delete(dataset_view_name, log_level=None)
    LOG.log(log_level, "End: Keep.")
    return dataset_path
