# -*- coding=utf-8 -*-
"""Workspace operations."""
import functools
import logging
import os

import arcpy

from arcetl import arcobj
from arcetl.helpers import LOG_LEVEL_MAP, toggle_arc_extension


LOG = logging.getLogger(__name__)


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
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Build locator %s.", locator_path)
    arcpy.geocoding.RebuildAddressLocator(locator_path)
    LOG.log(log_level, "End: Build.")
    return locator_path


def build_network(network_path, **kwargs):
    """Build network dataset.

    Args:
        network_path (str): Path of network.
    Kwargs:
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Build network %s.", network_path)
    toggle_arc_extension('Network', toggle_on=True)
    arcpy.na.BuildNetwork(in_network_dataset=network_path)
    toggle_arc_extension('Network', toggle_off=True)
    LOG.log(log_level, "End: Build.")
    return network_path


def compress(workspace_path, **kwargs):
    """Compress workspace (usually geodatabase).

    Args:
        geodatabase_path (str): Path of workspace.
    Kwargs:
        disconnect_users (bool): Flag to disconnect users before compressing.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('disconnect_users', False), ('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Compress workspace %s.", workspace_path)
    workspace_meta = metadata(workspace_path)
    if workspace_meta['is_file_geodatabase']:
        function = arcpy.management.CompressFileGeodatabaseData
    elif workspace_meta['is_enterprise_database']:
        function = arcpy.management.Compress
    else:
        raise ValueError("Compressing {} unsupported.".format(workspace_path))
    if all([workspace_meta['is_enterprise_database'],
            kwargs['disconnect_users']]):
        arcpy.AcceptConnections(sde_workspace=workspace_path,
                                accept_connections=False)
        arcpy.DisconnectUser(sde_workspace=workspace_path, users='all')
    function(workspace_path)
    if all([workspace_meta['is_enterprise_database'],
            kwargs['disconnect_users']]):
        arcpy.AcceptConnections(sde_workspace=workspace_path,
                                accept_connections=True)
    LOG.log(log_level, "End: Compress.")
    return workspace_path


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
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Create file geodatabase %s.", geodatabase_path)
    if os.path.exists(geodatabase_path):
        LOG.warning("Geodatabase already exists.")
        return geodatabase_path
    arcpy.management.CreateFileGDB(
        out_folder_path=os.path.dirname(geodatabase_path),
        out_name=os.path.basename(geodatabase_path), out_version='current'
        )
    if kwargs['xml_workspace_path']:
        arcpy.management.ImportXMLWorkspaceDocument(
            target_geodatabase=geodatabase_path,
            in_file=kwargs['xml_workspace_path'],
            import_type=(
                'data' if kwargs['include_xml_data'] else 'schema_only'),
            config_keyword='defaults'
            )
    LOG.log(log_level, "End: Create.")
    return geodatabase_path


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
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Create XML backup of geodatabase %s at %s.",
            geodatabase_path, output_path)
    arcpy.management.ExportXMLWorkspaceDocument(
        in_data=geodatabase_path, out_file=output_path,
        export_type='data' if kwargs['include_data'] else 'schema_only',
        storage_type='binary', export_metadata=kwargs['include_metadata']
        )
    LOG.log(log_level, "End: Create.")
    return output_path


def dataset_names(workspace_path, **kwargs):
    """Generator for workspace's dataset names.

    wildcard requires an * to indicate where open; case insensitive.

    Args:
        workspace_path (str): Path of workspace.
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
            ('include_tables', True), ('wildcard', None)
        ]:
        kwargs.setdefault(*kwarg_default)
    old_workspace_path = arcpy.env.workspace
    arcpy.env.workspace = workspace_path
    listfuncs = []
    if kwargs['include_feature_classes']:
        list_fcs = functools.partial(arcpy.ListFeatureClasses,
                                     wild_card=kwargs['wildcard'])
        listfuncs.append(list_fcs)  # Root-level.
        if kwargs['include_feature_datasets']:
            for name in arcpy.ListDatasets():
                listfuncs.append(list_fcs(feature_dataset=name))
    if kwargs['include_rasters']:
        listfuncs.append(
            functools.partial(arcpy.ListRasters, wild_card=kwargs['wildcard'])
            )
    if kwargs['include_tables']:
        listfuncs.append(
            functools.partial(arcpy.ListTables, wild_card=kwargs['wildcard'])
            )
    for listfunc in listfuncs:
        for name in listfunc():
            yield name
    arcpy.env.workspace = old_workspace_path


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


def execute_sql(statement, database_path, **kwargs):
    """Executes a SQL statement via SDE's SQL execution interface.

    Only works if path resolves to an actual SQL database.

    Args:
        dataset_path (str): Path of dataset.
    Kwargs:
        log_level (str): Level at which to log this function.
    Returns:
        str, tuple.
    """
    for kwarg_default in [('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Execute SQL statement.")
    conn = arcpy.ArcSDESQLExecute(server=database_path)
    try:
        result = conn.execute(statement)
    except AttributeError:
        LOG.exception("Incorrect SQL syntax.")
        raise
    finally:
        del conn  # Yeah, what can you do?
    LOG.log(log_level, "End: Execute.")
    return result


def is_valid(workspace_path):
    """Check whether workspace exists/is valid.

    Args:
        workspace_path (str): Path of workspace.
    Returns:
        bool.
    """
    return all([workspace_path is not None, arcpy.Exists(workspace_path),
                metadata(workspace_path)['data_type'] == 'Workspace'])


def metadata(workspace_path):
    """Return dictionary of workspace metadata.

    Args:
        workspace_path (str): Path of workspace.
    Returns:
        dict.
    """
    return arcobj.workspace_as_metadata(arcpy.Describe(workspace_path))