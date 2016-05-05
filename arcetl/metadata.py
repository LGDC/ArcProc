# -*- coding=utf-8 -*-
"""Metadata objects."""
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


def domain_metadata(domain_name, workspace_path):
    """Return dictionary of dataset metadata.

    Args:
        dataset_path (str): Path of dataset.
    Returns:
        dict.
    """
    return arcobj.domain_as_metadata(next(
        #pylint: disable=no-member
        domain for domain in arcpy.da.ListDomains(workspace_path)
        #pylint: enable=no-member
        if domain.name.lower() == domain_name.lower()))


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
    #pylint: disable=no-member
    with arcpy.da.SearchCursor(
        #pylint: enable=no-member
        in_table=dataset_path, field_names=['oid@'],
        where_clause=kwargs['dataset_where_sql']) as cursor:
        return len([None for _ in cursor])


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
        return arcobj.field_as_metadata(
            arcpy.ListFields(dataset=dataset_path, wild_card=field_name)[0])
    except IndexError:
        raise AttributeError(
            "Field {} not present on {}".format(field_name, dataset_path))


def is_valid_dataset(dataset_path):
    """Check whether dataset exists/is valid.

    Args:
        dataset_path (str): Path of dataset.
    Returns:
        bool.
    """
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
