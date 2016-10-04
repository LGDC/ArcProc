# -*- coding=utf-8 -*-
"""Metadata objects."""
import logging

import arcpy

from arcetl.arcobj import (dataset_as_metadata, domain_as_metadata,
                           field_as_metadata, spatial_reference_as_metadata)


LOG = logging.getLogger(__name__)


def dataset_metadata(dataset_path):
    """Return dictionary of dataset metadata.

    Args:
        dataset_path (str): Path of dataset.
    Returns:
        dict.
    """
    metadata = dataset_as_metadata(arcpy.Describe(dataset_path))
    return metadata


def domain_metadata(domain_name, workspace_path):
    """Return dictionary of dataset metadata.

    Args:
        dataset_path (str): Path of dataset.
    Returns:
        dict.
    """
    metadata = domain_as_metadata(
        next(domain for domain in arcpy.da.ListDomains(workspace_path)
             if domain.name.lower() == domain_name.lower())
        )
    return metadata


def feature_count(dataset_path, **kwargs):
    """Return number of features in dataset.

    Args:
        dataset_path (str): Path of dataset.
    Kwargs:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
    Returns:
        int.
    """
    kwargs.setdefault('dataset_where_sql', None)
    with arcpy.da.SearchCursor(
        in_table=dataset_path, field_names=['oid@'],
        where_clause=kwargs['dataset_where_sql']) as cursor:
        count = len([None for _ in cursor])
    return count


def field_metadata(dataset_path, field_name):
    """Return dictionary of field metadata.

    Field name is case-insensitive.

    Args:
        dataset_path (str): Path of dataset.
        field_name (str): Name of field.
    Returns:
        dict.
    """
    try:
        metadata = field_as_metadata(
            arcpy.ListFields(dataset=dataset_path, wild_card=field_name)[0]
            )
    except IndexError:
        raise AttributeError(
            "Field {} not present on {}".format(field_name, dataset_path)
            )
    return metadata


def is_valid_dataset(dataset_path):
    """Check whether dataset exists/is valid.

    Args:
        dataset_path (str): Path of dataset.
    Returns:
        bool.
    """
    is_valid = all([dataset_path is not None, arcpy.Exists(dataset_path),
                    dataset_metadata(dataset_path)['is_table']])
    return is_valid


def linear_unit_as_string(measure, spatial_reference):
    """Return unit of measure as a linear unit string."""
    linear_unit = spatial_reference_metadata(spatial_reference)['linear_unit']
    # if measure != 1 and linear_unit == 'Foot':
    #     linear_unit = 'Feet'
    return '{} {}'.format(measure, linear_unit)


def spatial_reference_metadata(spatial_reference):
    """Return dictionary of spatial reference metadata.

    Args:
        spatial_reference (str): Path of dataset, or spatial reference ID.
    Returns:
        dict.
    """
    if isinstance(spatial_reference, int):
        reference_object = arcpy.SpatialReference(spatial_reference)
    elif is_valid_dataset(spatial_reference):
        reference_object = arcpy.Describe(spatial_reference).spatialReference
    metadata = spatial_reference_as_metadata(reference_object)
    return metadata


def workspace_dataset_names(workspace_path, **kwargs):
    """Generator for workspace's dataset names.

    wildcard requires an * to indicate where open; case insensitive.

    Args:
        dataset_path (str): Path of dataset.
        field_name (str): Name of field.
    Kwargs:
        wildcard (str): String to indicate wildcard search.
        include_feature_classes (bool): Flag to include feature class datasets.
        include_rasters (bool): Flag to include raster datasets.
        include_tables (bool): Flag to include nonspatial tables.
        include_feature_datasets (bool): Flag to include contents of feature
            datasets.
    Yields:
        str.
    """
    for kwarg_default in [
            ('include_feature_classes', True),
            ('include_feature_datasets', True), ('include_rasters', True),
            ('include_tables', True), ('wildcard', None)]:
        kwargs.setdefault(*kwarg_default)
    old_workspace_path = arcpy.env.workspace
    arcpy.env.workspace = workspace_path
    dataset_names = []
    if kwargs['include_feature_classes']:
        # None-value represents the root level.
        feature_dataset_names = [None]
        if kwargs['include_feature_datasets']:
            feature_dataset_names.extend(arcpy.ListDatasets())
        for name in feature_dataset_names:
            dataset_names.extend(
                arcpy.ListFeatureClasses(
                    kwargs['wildcard'], feature_dataset=name))
    if kwargs['include_rasters']:
        dataset_names.extend(arcpy.ListRasters(kwargs['wildcard']))
    if kwargs['include_tables']:
        dataset_names.extend(arcpy.ListTables(kwargs['wildcard']))
    arcpy.env.workspace = old_workspace_path
    return dataset_names
