# -*- coding=utf-8 -*-
"""Data property objects."""
import logging

import arcpy

from . import arcobj

LOG = logging.getLogger(__name__)


def dataset_metadata(dataset_path):
    """Return dictionary of dataset metadata.

    Args:
        dataset_path (str): Path of dataset.
    Returns:
        dict.
    """
    return arcobj.dataset_as_metadata(arcpy.Describe(dataset_path))


def field_metadata(dataset_path, field_name):
    """Return dictionary of field metadata.

    Field name is case-insensitive.
    """
    try:
        return arcobj.field_as_metadata(
            arcpy.ListFields(dataset=dataset_path, wild_card=field_name)[0])
    except IndexError:
        raise AttributeError(
            "Field {} not present on {}".format(field_name, dataset_path))


def is_valid_dataset(dataset_path):
    """Check whether dataset exists/is valid."""
    return (dataset_path is not None and arcpy.Exists(dataset_path)
            and dataset_metadata(dataset_path)['is_table'])


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
