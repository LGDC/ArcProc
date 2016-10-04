# -*- coding=utf-8 -*-
"""Metadata objects."""
import logging

import arcpy

from arcetl import arcobj, dataset


LOG = logging.getLogger(__name__)


def domain_metadata(domain_name, workspace_path):
    """Return dictionary of dataset metadata.

    Args:
        dataset_path (str): Path of dataset.
    Returns:
        dict.
    """
    meta = arcobj.domain_as_metadata(
        next(domain for domain in arcpy.da.ListDomains(workspace_path)
             if domain.name.lower() == domain_name.lower())
        )
    return meta


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
    elif dataset.is_valid(spatial_reference):
        reference_object = arcpy.Describe(spatial_reference).spatialReference
    meta = arcobj.spatial_reference_as_metadata(reference_object)
    return meta


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
