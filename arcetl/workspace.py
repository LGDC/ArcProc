# -*- coding=utf-8 -*-
"""Workspace operations."""
import logging
import os
import tempfile

import arcpy

from . import arcwrap, helpers


LOG = logging.getLogger(__name__)


@helpers.log_function
def build_locator(locator_path, **kwargs):
    """Build network (dataset or geometric).

    Args:
        locator_path (str): Path of locator.
    Kwargs:
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    log_level = helpers.LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Build locator %s.", locator_path)
    try:
        arcpy.geocoding.RebuildAddressLocator(locator_path)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    LOG.log(log_level, "End: Build.")
    return locator_path


@helpers.log_function
def build_network(network_path, **kwargs):
    """Build network (dataset or geometric).

    Args:
        network_path (str): Path of network.
    Kwargs:
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    kwargs.setdefault('log_level', 'info')
    meta = {'description': "Build network {}.".format(network_path),
            #pylint: disable=no-member
            'network_type': arcpy.Describe(network_path).dataType}
    #pylint: enable=no-member
    helpers.log_line('start', meta['description'], kwargs['log_level'])
    if meta['network_type'] == 'GeometricNetwork':
        build_function = arcpy.management.RebuildGeometricNetwork
        build_kwargs = {'geometric_network': network_path,
                        # Geometric network build requires logfile output.
                        'out_log': os.path.join(tempfile.gettempdir(), 'log')}
    elif meta['network_type'] == 'NetworkDataset':
        helpers.toggle_arc_extension('Network', toggle_on=True)
        build_function = arcpy.na.BuildNetwork
        build_kwargs = {'in_network_dataset': network_path}
    else:
        raise ValueError("{} not a valid network type.".format(network_path))
    try:
        build_function(**build_kwargs)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    helpers.toggle_arc_extension('Network', toggle_off=True)
    if 'out_log' in build_kwargs:
        os.remove(build_kwargs['out_log'])
    helpers.log_line('end', meta['description'], kwargs['log_level'])
    return network_path


@helpers.log_function
def compress_geodatabase(geodatabase_path, **kwargs):
    """Compress geodatabase.

    Args:
        geodatabase_path (str): Path of geodatabase.
    Kwargs:
        disconnect_users (bool): Flag to disconnect users before compressing.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('disconnect_users', False), ('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    meta = {'description': "Compress {}.".format(geodatabase_path)}
    helpers.log_line('start', meta['description'], kwargs['log_level'])
    try:
        meta['workspace_type'] = arcpy.Describe(geodatabase_path).workspaceType
    except AttributeError:
        raise ValueError(
            "{} not a valid geodatabase.".format(geodatabase_path))
    if meta['workspace_type'] == 'LocalDatabase':
        compress_function = arcpy.management.CompressFileGeodatabaseData
        compress_kwargs = {'in_data': geodatabase_path}
    elif meta['workspace_type'] == 'RemoteDatabase':
        compress_function = arcpy.management.Compress
        compress_kwargs = {'in_workspace': geodatabase_path}
    else:
        raise ValueError(
            "{} not a valid geodatabase type.".format(meta['workspace_type']))
    if all([meta['workspace_type'] == 'RemoteDatabase',
            kwargs['disconnect_users']]):
        arcpy.AcceptConnections(
            sde_workspace=geodatabase_path, accept_connections=False)
        arcpy.DisconnectUser(sde_workspace=geodatabase_path, users='all')
    try:
        compress_function(**compress_kwargs)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    if all([meta['workspace_type'] == 'RemoteDatabase',
            kwargs['disconnect_users']]):
        arcpy.AcceptConnections(
            sde_workspace=geodatabase_path, accept_connections=True)
    helpers.log_line('end', meta['description'], kwargs['log_level'])
    return geodatabase_path


@helpers.log_function
def copy_dataset(dataset_path, output_path, **kwargs):
    """Copy features into a new dataset.

    Wraps arcwrap.copy_dataset.

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
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    # Other kwarg defaults set in the wrapped function.
    kwargs.setdefault('log_level', 'info')
    meta = {'description': "Copy {} to {}.".format(dataset_path, output_path)}
    helpers.log_line('start', meta['description'], kwargs['log_level'])
    result = arcwrap.copy_dataset(dataset_path, output_path, **kwargs)
    helpers.log_line('end', meta['description'], kwargs['log_level'])
    return result


@helpers.log_function
def create_dataset(dataset_path, field_metadata_list=None, **kwargs):
    """Create new dataset.

    Wraps arcwrap.create_dataset.

    Args:
        dataset_path (str): Path of dataset to create.
        field_metadata_list (iter): Iterable of field metadata dicts.
    Kwargs:
        geometry_type (str): Type of geometry, if a spatial dataset.
        spatial_reference_id (int): EPSG code for spatial reference, if a
            spatial dataset.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    # Other kwarg defaults set in the wrapped function.
    kwargs.setdefault('log_level', 'info')
    meta = {'description': "Create dataset {}.".format(dataset_path)}
    helpers.log_line('start', meta['description'], kwargs['log_level'])
    result = arcwrap.create_dataset(dataset_path, field_metadata_list, **kwargs)
    helpers.log_line('end', meta['description'], kwargs['log_level'])
    return result


@helpers.log_function
def create_file_geodatabase(geodatabase_path, **kwargs):
    """Create new file geodatabase.

    Args:
        geodatabase_path (str): Path of geodatabase to create.
    Kwargs:
        xml_workspace_path (str): Path of XML workspace document to define
            geodatabase with.
        include_xml_data (bool): Flag to include data from a provided XML
            workspace document, if it has any.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('include_xml_data', False), ('log_level', 'info'),
                          ('xml_workspace_path', None)]:
        kwargs.setdefault(*kwarg_default)
    meta = {
        'description': "Create file geodatabase at {}.".format(
            geodatabase_path)}
    helpers.log_line('start', meta['description'], kwargs['log_level'])
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
    if kwargs['xml_workspace_path']:
        try:
            arcpy.management.ImportXMLWorkspaceDocument(
                target_geodatabase=geodatabase_path,
                in_file=kwargs['xml_workspace_path'],
                import_type=(
                    'data' if kwargs['include_xml_data'] else 'schema_only'),
                config_keyword='defaults')
        except arcpy.ExecuteError:
            LOG.exception("ArcPy execution.")
            raise
    helpers.log_line('end', meta['description'], kwargs['log_level'])
    return geodatabase_path


@helpers.log_function
def create_geodatabase_xml_backup(geodatabase_path, output_path, **kwargs):
    """Create backup of geodatabase as XML workspace document.

    Args:
        geodatabase_path (str): Path of geodatabase.
        output_path (str): Path of output XML workspace document.
    Kwargs:
        include_data (bool): Flag to include data in backup.
        include_metadata (bool): Flag to include metadata in backup.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('include_data', False), ('include_metadata', True),
                          ('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    meta = {'description': "Create backup {} for {}.".format(geodatabase_path,
                                                             output_path)}
    helpers.log_line('start', meta['description'], kwargs['log_level'])
    try:
        arcpy.management.ExportXMLWorkspaceDocument(
            in_data=geodatabase_path, out_file=output_path,
            export_type='data' if kwargs['include_data'] else 'schema_only',
            storage_type='binary', export_metadata=kwargs['include_metadata'])
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    helpers.log_line('end', meta['description'], kwargs['log_level'])
    return output_path


@helpers.log_function
def delete_dataset(dataset_path, **kwargs):
    """Delete dataset.

    Wraps arcwrap.delete_dataset.

    Args:
        dataset_path (str): Path of dataset.
    Kwargs:
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    # Other kwarg defaults set in the wrapped function.
    kwargs.setdefault('log_level', 'info')
    meta = {'description': "Delete {}.".format(dataset_path)}
    helpers.log_line('start', meta['description'], kwargs['log_level'])
    result = arcwrap.delete_dataset(dataset_path)
    helpers.log_line('end', meta['description'], kwargs['log_level'])
    return result


@helpers.log_function
def execute_sql_statement(statement, database_path, **kwargs):
    """Runs a SQL statement via SDE's SQL execution interface.

    Only works if path resolves to an actual SQL database.

    Args:
        dataset_path (str): Path of dataset.
    Kwargs:
        log_level (str): Level at which to log this function.
    Returns:
        str, tuple.
    """
    kwargs.setdefault('log_level', 'info')
    meta = {'description': "Execute SQL statement."}
    helpers.log_line('start', meta['description'], kwargs['log_level'])
    conn = arcpy.ArcSDESQLExecute(server=database_path)
    try:
        result = conn.execute(statement)
    except AttributeError:
        LOG.exception("Incorrect SQL syntax.")
        raise
    finally:
        del conn  # Yeah, what can you do?
    helpers.log_line('end', meta['description'], kwargs['log_level'])
    return result


@helpers.log_function
def set_dataset_privileges(dataset_path, user_name, allow_view=None,
                           allow_edit=None, **kwargs):
    """Set privileges for dataset in enterprise geodatabase.

    For the allow-flags, True = grant; False = revoke; None = as is.

    Args:
        dataset_path (str): Path of dataset.
        allow_view (bool): Flag to allow or revoke view privileges.
        allow_edit (bool): Flag to allow or revoke edit privileges.
    Kwargs:
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    kwargs.setdefault('log_level', 'info')
    meta = {
        'description': "Set privileges for {} on {}.".format(
            user_name, dataset_path),
        'boolean_privilege_map': {
            True: 'grant', False: 'revoke', None: 'as_is'}}
    helpers.log_line('start', meta['description'], kwargs['log_level'])
    try:
        arcpy.management.ChangePrivileges(
            in_dataset=dataset_path, user=user_name,
            View=meta['boolean_privilege_map'][allow_view],
            Edit=meta['boolean_privilege_map'][allow_edit])
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    helpers.log_line('end', meta['description'], kwargs['log_level'])
    return dataset_path
