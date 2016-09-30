# -*- coding=utf-8 -*-
"""Low-level wrappers for ArcPy functions.

Defining these wrappers allows the module to avoid cyclical imports.
"""
import logging
import os

import arcpy

from . import arcobj, helpers


__all__ = []
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
    arcpy.management.AddField(
        in_table=dataset_path, field_name=field_name,
        field_type=arcobj.FIELD_TYPE_AS_ARC.get(field_type.lower(), field_type),
        field_length=kwargs['field_length'],
        field_precision=kwargs['field_precision'],
        field_scale=kwargs['field_scale'],
        field_is_nullable=kwargs['field_is_nullable'],
        field_is_required=kwargs['field_is_required'])
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
            ('dataset_where_sql', None), ('overwrite', False),
            ('schema_only', False), ('sort_field_names', []),
            ('sort_reversed_field_names', [])]:
        kwargs.setdefault(*kwarg_default)
    dataset_meta = arcobj.dataset_as_metadata(arcpy.Describe(dataset_path))
    dataset_view_name = create_dataset_view(
        helpers.unique_name('view'), dataset_path,
        dataset_where_sql=(
            "0=1" if kwargs['schema_only'] else kwargs['dataset_where_sql']))
    if kwargs['sort_field_names']:
        copy_function = arcpy.management.Sort
        copy_kwargs = {
            'in_dataset': dataset_view_name,
            'out_dataset': output_path,
            'sort_field': [
                (name, 'descending')
                if name in kwargs['sort_reversed_field_names']
                else (name, 'ascending')
                for name in kwargs['sort_field_names']],
            'spatial_sort_method': 'UR'}
    elif dataset_meta['is_spatial']:
        copy_function = arcpy.management.CopyFeatures
        copy_kwargs = {'in_features': dataset_view_name,
                       'out_feature_class': output_path}
    elif dataset_meta['is_table']:
        copy_function = arcpy.management.CopyRows
        copy_kwargs = {'in_rows': dataset_view_name,
                       'out_table': output_path}
    else:
        raise ValueError("{} unsupported dataset type.".format(dataset_path))
    if kwargs['overwrite'] and arcpy.Exists(output_path):
        delete_dataset(output_path)
    copy_function(**copy_kwargs)
    delete_dataset(dataset_view_name)
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
    create_function(**create_kwargs)
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
    for kwarg_default in [
            ('dataset_where_sql', None), ('force_nonspatial', False)]:
        kwargs.setdefault(*kwarg_default)
    dataset_meta = arcobj.dataset_as_metadata(arcpy.Describe(dataset_path))
    create_kwargs = {'where_clause': kwargs['dataset_where_sql'],
                     'workspace':  dataset_meta['workspace_path']}
    if dataset_meta['is_spatial'] and not kwargs['force_nonspatial']:
        create_function = arcpy.management.MakeFeatureLayer
        create_kwargs['in_features'] = dataset_path
        create_kwargs['out_layer'] = view_name
    elif dataset_meta['is_table']:
        create_function = arcpy.management.MakeTableView
        create_kwargs['in_table'] = dataset_path
        create_kwargs['out_view'] = view_name
    else:
        raise ValueError("{} unsupported dataset type.".format(dataset_path))
    create_function(**create_kwargs)
    return view_name


def delete_dataset(dataset_path):
    """Delete dataset.

    Args:
        dataset_path (str): Path of dataset.
    Returns:
        str.
    """
    arcpy.management.Delete(in_data=dataset_path)
    return dataset_path

