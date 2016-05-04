# -*- coding=utf-8 -*-
"""Low-level wrappers for ArcPy functions.

Defining these wrappers allows the module to avoid cyclical imports.
"""
import logging
import os

import arcpy

from . import arcobj, helpers

LOG = logging.getLogger(__name__)


def add_field(dataset_path, field_name, field_type, **kwargs):
    """Add field to dataset.

    Args:
        dataset_path (str): Path of dataset.
        field_name (str): Name of field.
        field_type (str): Type of field.
    Kwargs:
        field_length (int): Length of field.
        field_precision (int): Precision of field.
        field_scale (int): Scale of field.
        field_is_nullable (bool): Flag indicating if field will be nullable.
        field_is_required (bool): Flag indicating if field will be required.
    Returns:
        str.
    """
    for kwarg_default in [
            ('field_is_nullable', True), ('field_is_required', False),
            ('field_length', 64), ('field_precision', None),
            ('field_scale', None)]:
        kwargs.setdefault(*kwarg_default)
    try:
        arcpy.management.AddField(
            in_table=dataset_path, field_name=field_name,
            field_type=arcobj.FIELD_TYPE_AS_ARC.get(
                field_type.lower(), field_type),
            field_length=kwargs['field_length'],
            field_precision=kwargs['field_precision'],
            field_scale=kwargs['field_scale'],
            field_is_nullable=kwargs['field_is_nullable'],
            field_is_required=kwargs['field_is_required'])
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    return field_name


def copy_dataset(dataset_path, output_path, **kwargs):
    """Copy features into a new dataset.

    Args:
        dataset_path (str): Path of dataset.
        output_path (str): Path of output dataset.
    Kwargs:
        schema_only (bool): Flag to copy only the schema, omitting the data.
        overwrite (bool): Flag to overwrite an existing dataset at the path.
        sort_field_names (iter): Iterable of field names to sort on, in order.
        sort_reversed_field_names (iter): Iterable of field names (present in
            sort_field_names) to sort values in reverse-order.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
    Returns:
        str.
    """
    for kwarg_default in [
            ('dataset_where_sql', None), ('log_level', 'info'),
            ('overwrite', False), ('schema_only', False),
            ('sort_field_names', []), ('sort_reversed_field_names', [])]:
        kwargs.setdefault(*kwarg_default)
    meta = {
        'dataset': arcobj.dataset_as_metadata(arcpy.Describe(dataset_path)),
        'dataset_view_name': create_dataset_view(
            helpers.unique_name('view'), dataset_path,
            dataset_where_sql=("0=1" if kwargs['schema_only']
                               else kwargs['dataset_where_sql']))}
    if kwargs['sort_field_names']:
        copy_function = arcpy.management.Sort
        copy_kwargs = {
            'in_dataset': meta['dataset_view_name'],
            'out_dataset': output_path,
            'sort_field': [
                (name, 'descending')
                if name in kwargs['sort_reversed_field_names']
                else (name, 'ascending')
                for name in kwargs['sort_field_names']],
            'spatial_sort_method': 'UR'}
    elif meta['dataset']['is_spatial']:
        copy_function = arcpy.management.CopyFeatures
        copy_kwargs = {'in_features': meta['dataset_view_name'],
                       'out_feature_class': output_path}
    elif meta['dataset']['is_table']:
        copy_function = arcpy.management.CopyRows
        copy_kwargs = {'in_rows': meta['dataset_view_name'],
                       'out_table': output_path}
    else:
        raise ValueError("{} unsupported dataset type.".format(dataset_path))
    if kwargs['overwrite'] and arcpy.Exists(output_path):
        delete_dataset(output_path)
    try:
        copy_function(**copy_kwargs)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    delete_dataset(meta['dataset_view_name'])
    return output_path


def create_dataset(dataset_path, field_metadata_list=None, **kwargs):
    """Create new dataset.

    Args:
        dataset_path (str): Path of dataset to create.
        field_metadata_list (iter): Iterable of field metadata dicts.
    Kwargs:
        geometry_type (str): Type of geometry, if a spatial dataset.
        spatial_reference_id (int): EPSG code for spatial reference, if a
            spatial dataset.
    Returns:
        str.
    """
    for kwarg_default in [
            ('geometry_type', None), ('spatial_reference_id', 4326)]:
        kwargs.setdefault(*kwarg_default)
    create_kwargs = {'out_path': os.path.dirname(dataset_path),
                     'out_name': os.path.basename(dataset_path)}
    if kwargs['geometry_type']:
        create_function = arcpy.management.CreateFeatureclass
        create_kwargs['geometry_type'] = kwargs['geometry_type']
        # Default to EPSG 4326 (unprojected WGS 84).
        create_kwargs['spatial_reference'] = arcpy.SpatialReference(
            kwargs['spatial_reference_id'])
    else:
        create_function = arcpy.management.CreateTable
    try:
        create_function(**create_kwargs)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    if field_metadata_list:
        for metadata in field_metadata_list:
            add_field(**metadata)
    return dataset_path


def create_dataset_view(view_name, dataset_path, **kwargs):
    """Create new view of dataset.

    Args:
        view_name (str): Name of view to create.
        dataset_path (str): Path of dataset.
    Kwargs:
        force_nonspatial (bool): Flag ensure view is nonspatial.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
    Returns:
        str.
    """
    kwargs.setdefault('dataset_where_sql', None)
    kwargs.setdefault('force_nonspatial', False)
    meta = {
        'dataset': arcobj.dataset_as_metadata(arcpy.Describe(dataset_path))}
    create_kwargs = {'where_clause': kwargs['dataset_where_sql'],
                     'workspace':  meta['dataset']['workspace_path']}
    if meta['dataset']['is_spatial'] and not kwargs['force_nonspatial']:
        create_function = arcpy.management.MakeFeatureLayer
        create_kwargs['in_features'] = dataset_path
        create_kwargs['out_layer'] = view_name
    elif meta['dataset']['is_table']:
        create_function = arcpy.management.MakeTableView
        create_kwargs['in_table'] = dataset_path
        create_kwargs['out_view'] = view_name
    else:
        raise ValueError("{} unsupported dataset type.".format(dataset_path))
    try:
        create_function(**create_kwargs)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    return view_name


def delete_dataset(dataset_path):
    """Delete dataset.

    Args:
        dataset_path (str): Path of dataset.
    Returns:
        str.
    """
    try:
        arcpy.management.Delete(in_data=dataset_path)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    return dataset_path


def delete_features(dataset_path, **kwargs):
    """Delete select features.

    Args:
        dataset_path (str): Path of dataset.
    Kwargs:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
    Returns:
        str.
    """
    kwargs.setdefault('dataset_where_sql', None)
    meta = {
        'truncate_type_error_codes': (
            # "Only supports Geodatabase tables and feature classes."
            'ERROR 000187',
            # "Operation not supported on table {table name}."
            'ERROR 001260',
            # Operation not supported on a feature class in a controller
            # dataset.
            'ERROR 001395'),
        'dataset_view_name': create_dataset_view(
            helpers.unique_name('view'), dataset_path,
            dataset_where_sql=kwargs['dataset_where_sql'])}
    # Can use (faster) truncate when no sub-selection
    run_truncate = kwargs.get('dataset_where_sql') is None
    if run_truncate:
        try:
            arcpy.management.TruncateTable(in_table=meta['dataset_view_name'])
        except arcpy.ExecuteError:
            # Avoid arcpy.GetReturnCode(); error code position inconsistent.
            # Search messages for 'ERROR ######' instead.
            if any(code in arcpy.GetMessages()
                   for code in meta['truncate_type_error_codes']):
                LOG.debug("Truncate unsupported; will try deleting rows.")
                run_truncate = False
            else:
                LOG.exception("ArcPy execution.")
                raise
    if not run_truncate:
        try:
            arcpy.management.DeleteRows(in_rows=meta['dataset_view_name'])
        except:
            LOG.exception("ArcPy execution.")
            raise
    delete_dataset(meta['dataset_view_name'])
    return dataset_path
