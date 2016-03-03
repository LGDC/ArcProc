# -*- coding=utf-8 -*-
"""Objects for workspace operations."""
import logging

import arcpy

import arcetl.helpers as helpers
import arcetl.properties as properties


LOG = logging.getLogger(__name__)


#pylint: disable=too-many-arguments
@helpers.log_function
def copy_dataset(dataset_path, output_path, dataset_where_sql=None,
                 schema_only=False, overwrite=False, log_level='info'):
    """Copy dataset."""
    logline = "Copy {} to {}.".format(dataset_path, output_path)
    helpers.log_line('start', logline, log_level)
    dataset_view_name = create_dataset_view(
        helpers.unique_name('dataset_view'), dataset_path,
        dataset_where_sql="0=1" if schema_only else dataset_where_sql,
        log_level=None)
    dataset_metadata = properties.dataset_metadata(dataset_path)
    if dataset_metadata['is_spatial']:
        _copy = arcpy.management.CopyFeatures
        _copy_kwargs = {'in_features': dataset_view_name,
                        'out_feature_class': output_path}
    elif dataset_metadata['is_table']:
        _copy = arcpy.management.CopyRows
        _copy_kwargs = {'in_rows': dataset_view_name,
                        'out_table': output_path}
    else:
        raise ValueError("{} unsupported dataset type.".format(dataset_path))
    if overwrite and properties.is_valid_dataset(output_path):
        delete_dataset(output_path, log_level=None)
    try:
        _copy(**_copy_kwargs)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    delete_dataset(dataset_view_name, log_level=None)
    helpers.log_line('end', logline, log_level)
    return output_path
#pylint: enable=too-many-arguments


@helpers.log_function
def create_dataset_view(view_name, dataset_path, dataset_where_sql=None,
                        force_nonspatial=False, log_level='info'):
    """Create new view of dataset."""
    logline = "Create dataset view of {}.".format(dataset_path)
    helpers.log_line('start', logline, log_level)
    dataset_metadata = properties.dataset_metadata(dataset_path)
    _create_kwargs = {'where_clause': dataset_where_sql,
                      'workspace': dataset_metadata['workspace_path']}
    if dataset_metadata['is_spatial'] and not force_nonspatial:
        _create = arcpy.management.MakeFeatureLayer
        _create_kwargs['in_features'] = dataset_path
        _create_kwargs['out_layer'] = view_name
    elif dataset_metadata['is_table']:
        _create = arcpy.management.MakeTableView
        _create_kwargs['in_table'] = dataset_path
        _create_kwargs['out_view'] = view_name
    else:
        raise ValueError("{} unsupported dataset type.".format(dataset_path))
    try:
        _create(**_create_kwargs)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    helpers.log_line('end', logline, log_level)
    return view_name


@helpers.log_function
def delete_dataset(dataset_path, log_level='info'):
    """Delete dataset."""
    logline = "Delete {}.".format(dataset_path)
    helpers.log_line('start', logline, log_level)
    try:
        arcpy.management.Delete(dataset_path)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    helpers.log_line('end', logline, log_level)
    return dataset_path
