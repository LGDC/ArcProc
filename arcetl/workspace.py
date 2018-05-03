"""Workspace operations."""
import logging
import os

import arcpy

from arcetl import arcobj
from arcetl.helpers import leveled_logger


LOG = logging.getLogger(__name__)


def build_locator(locator_path, **kwargs):
    """Build locator.

    Args:
        locator_path (str): Path of the locator.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        str: Path of the built locator.
    """
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    log("Start: Build locator %s.", locator_path)
    arcpy.geocoding.RebuildAddressLocator(locator_path)
    log("End: Build.")
    return locator_path


def build_network(network_path, **kwargs):
    """Build network dataset.

    Args:
        network_path (str): Path of the network dataset.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        str: Path of the built network dataset.
    """
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    log("Start: Build network %s.", network_path)
    with arcobj.ArcExtension('Network'):
        arcpy.na.BuildNetwork(in_network_dataset=network_path)
    log("End: Build.")
    return network_path


def compress(workspace_path, disconnect_users=False, **kwargs):
    """Compress workspace (usually geodatabase).

    Args:
        geodatabase_path (str): Path of the workspace.
        disconnect_users (bool): Flag to disconnect users before compressing.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        str: Path of the compressed workspace.

    Raises:
        ValueError: If `workspace_path` doesn't reference a compressable
            geodatabase.
    """
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    log("Start: Compress workspace %s.", workspace_path)
    workspace_meta = metadata(workspace_path)
    if workspace_meta['is_file_geodatabase']:
        compress_func = arcpy.management.CompressFileGeodatabaseData
    elif workspace_meta['is_enterprise_database']:
        compress_func = arcpy.management.Compress
    else:
        raise ValueError("Compressing {} unsupported.".format(workspace_path))
    if all((workspace_meta['is_enterprise_database'], disconnect_users)):
        arcpy.AcceptConnections(sde_workspace=workspace_path,
                                accept_connections=False)
        arcpy.DisconnectUser(sde_workspace=workspace_path, users='all')
    compress_func(workspace_path)
    if all((workspace_meta['is_enterprise_database'], disconnect_users)):
        arcpy.AcceptConnections(sde_workspace=workspace_path,
                                accept_connections=True)
    log("End: Compress.")
    return workspace_path


def create_file_geodatabase(geodatabase_path, xml_workspace_path=None,
                            include_xml_data=False, **kwargs):
    """Create new file geodatabase.

    Args:
        geodatabase_path (str): Path of the geodatabase.
        xml_workspace_path (str): Path of the XML workspace document to
            initialize the geodatabase with.
        include_xml_data (bool): Flag to include data stored in the XML
            workspace document, if it has any.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        str: Path of the created file geodatabase.
    """
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    log("Start: Create file geodatabase %s.", geodatabase_path)
    if os.path.exists(geodatabase_path):
        LOG.warning("Geodatabase already exists.")
        return geodatabase_path
    arcpy.management.CreateFileGDB(
        out_folder_path=os.path.dirname(geodatabase_path),
        out_name=os.path.basename(geodatabase_path), out_version='current'
        )
    if xml_workspace_path:
        arcpy.management.ImportXMLWorkspaceDocument(
            target_geodatabase=geodatabase_path,
            in_file=xml_workspace_path,
            import_type='data' if include_xml_data else 'schema_only',
            config_keyword='defaults'
            )
    log("End: Create.")
    return geodatabase_path


def create_geodatabase_xml_backup(geodatabase_path, output_path,
                                  include_data=False, include_metadata=True,
                                  **kwargs):
    """Create backup of geodatabase as XML workspace document.

    Args:
        geodatabase_path (str): Path of the geodatabase.
        output_path (str): Path of the XML workspace document to create.
        include_data (bool): Flag to include data in the output.
        include_metadata (bool): Flag to include metadata in the output.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        str: Path of the created XML workspace document.
    """
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    log("Start: Create XML backup of geodatabase %s at %s.",
        geodatabase_path, output_path)
    arcpy.management.ExportXMLWorkspaceDocument(
        in_data=geodatabase_path, out_file=output_path,
        export_type='data' if include_data else 'schema_only',
        storage_type='binary', export_metadata=include_metadata
        )
    log("End: Create.")
    return output_path


def dataset_names(workspace_path, include_feature_classes=True,
                  include_rasters=True, include_tables=True,
                  only_top_level=False, **kwargs):
    """Generate names of datasets in workspace.

    Args:
        workspace_path (str): Path of the workspace.
        include_feature_classes (bool): Flag to include feature class datasets.
        include_rasters (bool): Flag to include raster datasets.
        include_tables (bool): Flag to include nonspatial tables.
        only_top_level (bool): Flag to only list datasets at the top-level.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        name_validator (function): Function to validate dataset names yielded.

    Yields:
        str: Name of the next dataset in the workspace.
    """
    dataset_types = tuple()
    if include_feature_classes:
        dataset_types += ('FeatureClass',)
    if include_rasters:
        dataset_types += ('RasterCatalog', 'RasterDataset')
    if include_tables:
        dataset_types += ('Table',)
    for root_path, _, names in arcpy.da.Walk(workspace_path,
                                             datatype=dataset_types):
        if only_top_level and root_path != workspace_path:
            continue
        for name in names:
            if kwargs.get('name_validator', lambda n: True)(name):
                yield name


def dataset_paths(workspace_path, include_feature_classes=True,
                  include_rasters=True, include_tables=True,
                  only_top_level=False, **kwargs):
    """Generate paths of datasets in workspace.

    Args:
        workspace_path (str): Path of the workspace.
        include_feature_classes (bool): Flag to include feature class datasets.
        include_rasters (bool): Flag to include raster datasets.
        include_tables (bool): Flag to include nonspatial tables.
        only_top_level (bool): Flag to only list datasets at the top-level.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        name_validator (function): Function to validate dataset names yielded.

    References:
        dataset_names.

    Yields:
        str: Path of the next dataset in the workspace.
    """
    for name in dataset_names(workspace_path, include_feature_classes,
                              include_rasters, include_tables, only_top_level,
                              **kwargs):
        yield os.path.join(workspace_path, name)


domain_metadata = arcobj.domain_metadata  # pylint: disable=invalid-name


def execute_sql(statement, database_path, **kwargs):
    """Execute SQL statement via ArcSDE's SQL execution interface.

    Only works if database_path resolves to an actual SQL database.

    Args:
        statement (str): SQL statement to execute.
        database_path (str): Path of the database to execute statement in.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        object: Return value from the SQL statement's execution. Likely
            return types:
                bool: Success (True) or failure (False) of statement not
                    returning rows.
                list: A List of lists representing returned rows.
                object: A single return value.

    Raises:
        AttributeError: If statement SQL syntax is incorrect.
    """
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    log("Start: Execute SQL statement.")
    conn = arcpy.ArcSDESQLExecute(server=database_path)
    try:
        result = conn.execute(statement)
    except AttributeError:
        LOG.exception("Incorrect SQL syntax.")
        raise
    finally:
        del conn  # Yeah, what can you do?
    log("End: Execute.")
    return result


def is_valid(workspace_path):
    """Indicate whether workspace exists/is valid.

    Args:
        workspace_path (str): Path of the workspace to verify.

    Returns:
        bool: Indicates the workspace is valid (True) or not (False).
    """
    return (workspace_path is not None and arcpy.Exists(workspace_path)
            and metadata(workspace_path)['data_type'] == 'Workspace')


metadata = arcobj.workspace_metadata  # pylint: disable=invalid-name
