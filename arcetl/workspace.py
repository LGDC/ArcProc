# -*- coding=utf-8 -*-
"""Objects for workspace operations."""
import logging
import os

import arcpy

from . import dataset
from . import helpers


LOG = logging.getLogger(__name__)


@helpers.log_function
def compress_geodatabase(geodatabase_path, disconnect_users=False,
                         log_level='info'):
    """Compress geodatabase."""
    logline = "Compress {}.".format(geodatabase_path)
    helpers.log_line('start', logline, log_level)
    try:
        workspace_type = arcpy.Describe(geodatabase_path).workspaceType
    except AttributeError:
        raise ValueError(
            "{} not a valid geodatabase.".format(geodatabase_path))
    if workspace_type == 'LocalDatabase':
        _compress = arcpy.management.CompressFileGeodatabaseData
        _compress_kwargs = {'in_data': geodatabase_path}
        # Local databases cannot disconnect users (connections managed
        # by file access).
        disconnect_users = False
    elif workspace_type == 'RemoteDatabase':
        _compress = arcpy.management.Compress
        _compress_kwargs = {'in_workspace': geodatabase_path}
    else:
        raise ValueError(
            "{} not a valid geodatabase type.".format(workspace_type))
    if disconnect_users:
        arcpy.AcceptConnections(sde_workspace=geodatabase_path,
                                accept_connections=False)
        arcpy.DisconnectUser(sde_workspace=geodatabase_path, users='all')
    try:
        _compress(**_compress_kwargs)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    if disconnect_users:
        arcpy.AcceptConnections(sde_workspace=geodatabase_path,
                                accept_connections=True)
    helpers.log_line('end', logline, log_level)
    return geodatabase_path


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
    dataset_metadata = dataset.dataset_metadata(dataset_path)
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
    if overwrite and dataset.is_valid_dataset(output_path):
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
def create_dataset(dataset_path, field_metadata=None, geometry_type=None,
                   spatial_reference_id=None, log_level='info'):
    """Create new dataset."""
    logline = "Create dataset {}.".format(dataset_path)
    helpers.log_line('start', logline, log_level)
    _create_kwargs = {'out_path': os.path.dirname(dataset_path),
                      'out_name': os.path.basename(dataset_path)}
    if geometry_type:
        _create = arcpy.management.CreateFeatureclass
        _create_kwargs['geometry_type'] = geometry_type
        # Default to EPSG 4326 (unprojected WGS 84).
        _create_kwargs['spatial_reference'] = arcpy.SpatialReference(
            spatial_reference_id if spatial_reference_id else 4326)
    else:
        _create = arcpy.management.CreateTable
    print('_create', _create)
    print('_create_kwargs', _create_kwargs)
    try:
        _create(**_create_kwargs)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    if field_metadata:
        if isinstance(field_metadata, dict):
            field_metadata = [field_metadata]
        for a_field_metadata in field_metadata:
            print(a_field_metadata)
            _add_kwargs = a_field_metadata.copy()
            _add_kwargs['log_level'] = None
            dataset.add_field(*_add_kwargs)
    helpers.log_line('end', logline, log_level)
    return dataset_path


@helpers.log_function
def create_dataset_view(view_name, dataset_path, dataset_where_sql=None,
                        force_nonspatial=False, log_level='info'):
    """Create new view of dataset."""
    logline = "Create dataset view of {}.".format(dataset_path)
    helpers.log_line('start', logline, log_level)
    dataset_metadata = dataset.dataset_metadata(dataset_path)
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
def create_file_geodatabase(geodatabase_path, xml_workspace_path=None,
                            include_xml_data=False, log_level='info'):
    """Create new file geodatabase."""
    logline = "Create file geodatabase at {}.".format(geodatabase_path)
    helpers.log_line('start', logline, log_level)
    if os.path.exists(geodatabase_path):
        LOG.warning("Geodatabase already exists.")
        return geodatabase_path
    try:
        arcpy.management.CreateFileGDB(
            out_folder_path=os.path.dirname(geodatabase_path),
            out_name=os.path.basename(geodatabase_path),
            out_version='current')
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    if xml_workspace_path:
        try:
            arcpy.management.ImportXMLWorkspaceDocument(
                target_geodatabase=geodatabase_path,
                in_file=xml_workspace_path,
                import_type='data' if include_xml_data else 'schema_only',
                config_keyword='defaults')
        except arcpy.ExecuteError:
            LOG.exception("ArcPy execution.")
            raise
    helpers.log_line('end', logline, log_level)
    return geodatabase_path


##@helpers.log_function
##def create_geodatabase_xml_backup(self, geodatabase_path, output_path,
##                                  include_data=False,
##                                  include_metadata=True, log_level='info'):
    ##"""Create backup of geodatabase as XML workspace document."""
    ##logline = "Create backup for {} in {}.".format(geodatabase_path,
    ##                                               output_path)
    ##helpers.log_line('start', logline, log_level)
    ##try:
    ##    arcpy.management.ExportXMLWorkspaceDocument(
    ##        in_data = geodatabase_path, out_file = output_path,
    ##        export_type = 'data' if include_data else 'schema_only',
    ##        storage_type = 'binary', export_metadata = include_metadata)
    ##except arcpy.ExecuteError:
    ##    LOG.exception("ArcPy execution.")
    ##    raise
    ##helpers.log_line('end', logline, log_level)
    ##return output_path


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


##@helpers.log_function
##def set_dataset_privileges(self, dataset_path, user_name, allow_view=None,
##                           allow_edit=None, log_level='info'):
    ##"""Set privileges for dataset in enterprise geodatabase."""
    ##logline = "Set privileges for {} on {}.".format(user_name,
    ##                                                dataset_path)
    ##helpers.log_line('start', logline, log_level)
    ##PRIVILEGE_MAP = {True: 'grant', False: 'revoke', None: 'as_is'}
    ##try:
    ##    arcpy.management.ChangePrivileges(
    ##        in_dataset = dataset_path, user = user_name,
    ##        View = PRIVILEGE_MAP[allow_view],
    ##        Edit = PRIVILEGE_MAP[allow_edit])
    ##except arcpy.ExecuteError:
    ##    LOG.exception("ArcPy execution.")
    ##    raise
    ##helpers.log_line('end', logline, log_level)
    ##return dataset_path
