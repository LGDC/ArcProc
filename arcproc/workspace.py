"""Workspace operations."""
import logging
import os

import arcpy

from arcproc.arcobj import domain_metadata, workspace_metadata


LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""

arcpy.SetLogHistory(False)


def build_locator(locator_path, **kwargs):
    """Build locator.

    Args:
        locator_path (str): Path of the locator.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        str: Path of the built locator.
    """
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(level, "Start: Build locator `%s`.", locator_path)
    arcpy.geocoding.RebuildAddressLocator(locator_path)
    LOG.log(level, "End: Build.")
    return locator_path


def compress(workspace_path, disconnect_users=False, **kwargs):
    """Compress workspace (usually geodatabase).

    Args:
        geodatabase_path (str): Path of the workspace.
        disconnect_users (bool): Flag to disconnect users before compressing.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        str: Path of compressed workspace.

    Raises:
        ValueError: If `workspace_path` doesn't reference a compressable geodatabase.
    """
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(level, "Start: Compress workspace `%s`.", workspace_path)
    meta = {"workspace": workspace_metadata(workspace_path)}
    if not meta["workspace"]["is_enterprise_database"]:
        raise ValueError("Compressing `{}` unsupported.".format(workspace_path))

    if disconnect_users:
        arcpy.AcceptConnections(sde_workspace=workspace_path, accept_connections=False)
        arcpy.DisconnectUser(sde_workspace=workspace_path, users="all")
    arcpy.management.Compress(workspace_path)
    if disconnect_users:
        arcpy.AcceptConnections(sde_workspace=workspace_path, accept_connections=True)
    LOG.log(level, "End: Compress.")
    return workspace_path


def copy(workspace_path, output_path, **kwargs):
    """Copy workspace to another location.

    Args:
        workspace_path (str): Path of the workspace.
        output_path (str): Path of output workspace.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        str: Output path of copied workspace.

    Raises:
        ValueError: If dataset type not supported.
    """
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(level, "Start: Copy workspace `%s` to `%s`.", workspace_path, output_path)
    meta = {"dataset": workspace_metadata(workspace_path)}
    if not meta["dataset"]["can_copy"]:
        raise ValueError("`{}` unsupported dataset type.".format(workspace_path))

    arcpy.management.Copy(workspace_path, output_path)
    LOG.log(level, "End: Copy.")
    return output_path


def create_file_geodatabase(
    geodatabase_path, xml_workspace_path=None, include_xml_data=False, **kwargs
):
    """Create new file geodatabase.

    Args:
        geodatabase_path (str): Path of the geodatabase.
        xml_workspace_path (str): Path of the XML workspace document to initialize the
            geodatabase with.
        include_xml_data (bool): Flag to include data stored in the XML workspace
            document, if it has any.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        str: Path of created file geodatabase.
    """
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(level, "Start: Create file geodatabase `%s`.", geodatabase_path)
    if os.path.exists(geodatabase_path):
        LOG.warning("Geodatabase already exists.")
        return geodatabase_path

    arcpy.management.CreateFileGDB(
        out_folder_path=os.path.dirname(geodatabase_path),
        out_name=os.path.basename(geodatabase_path),
        out_version="current",
    )
    if xml_workspace_path:
        arcpy.management.ImportXMLWorkspaceDocument(
            target_geodatabase=geodatabase_path,
            in_file=xml_workspace_path,
            import_type=("data" if include_xml_data else "schema_only"),
            config_keyword="defaults",
        )
    LOG.log(level, "End: Create.")
    return geodatabase_path


def create_geodatabase_xml_backup(
    geodatabase_path, output_path, include_data=False, include_metadata=True, **kwargs
):
    """Create backup of geodatabase as XML workspace document.

    Args:
        geodatabase_path (str): Path of the geodatabase.
        output_path (str): Path of the XML workspace document to create.
        include_data (bool): Flag to include data in the output.
        include_metadata (bool): Flag to include metadata in the output.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        str: Path of created XML workspace document.
    """
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level,
        "Start: Create XML backup of geodatabase `%s` at `%s`.",
        geodatabase_path,
        output_path,
    )
    arcpy.management.ExportXMLWorkspaceDocument(
        in_data=geodatabase_path,
        out_file=output_path,
        export_type=("data" if include_data else "schema_only"),
        storage_type="binary",
        export_metadata=include_metadata,
    )
    LOG.log(level, "End: Create.")
    return output_path


def dataset_names(workspace_path, **kwargs):
    """Generate names of datasets in workspace.

    Args:
        workspace_path (str): Path of the workspace.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        include_feature_classes (bool): Flag to include feature class datasets. Default
            is True.
        include_rasters (bool): Flag to include raster datasets. Default is True.
        include_tables (bool): Flag to include nonspatial tables. Default is True.
        only_top_level (bool): Flag to only list datasets at the top-level. Default is
            False.
        name_validator (function): Function to validate dataset names yielded. Default
            is all names considered valid.

    Yields:
        str: Name of the next dataset in the workspace.
    """
    kwargs.setdefault("include_feature_classes", True)
    kwargs.setdefault("include_rasters", True)
    kwargs.setdefault("include_tables", True)
    kwargs.setdefault("only_top_level", False)
    kwargs.setdefault("name_validator", lambda n: True)
    for dataset_path in dataset_paths(workspace_path, **kwargs):
        yield os.path.basename(dataset_path)


def dataset_paths(workspace_path, **kwargs):
    """Generate paths of datasets in workspace.

    Args:
        workspace_path (str): Path of the workspace.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        include_feature_classes (bool): Flag to include feature class datasets. Default
            is True.
        include_rasters (bool): Flag to include raster datasets. Default is True.
        include_tables (bool): Flag to include nonspatial tables. Default is True.
        only_top_level (bool): Flag to only list datasets at the top-level. Default is
            False.
        name_validator (function): Function to validate dataset names yielded. Default
            is all names considered valid.

    Yields:
        str: Path of the next dataset in the workspace.
    """
    kwargs.setdefault("include_feature_classes", True)
    kwargs.setdefault("include_rasters", True)
    kwargs.setdefault("include_tables", True)
    kwargs.setdefault("only_top_level", False)
    kwargs.setdefault("name_validator", lambda n: True)
    dataset_types = {
        "feature_classes": ["FeatureClass"],
        "rasters": ["RasterCatalog", "RasterDataset"],
        "tables": ["Table"],
    }
    include_data_types = []
    for _type in dataset_types:
        if kwargs["include_" + _type]:
            include_data_types.extend(dataset_types[_type])
    for root_path, _, _dataset_names in arcpy.da.Walk(
        workspace_path, datatype=include_data_types
    ):
        if kwargs["only_top_level"] and root_path != workspace_path:
            continue

        for dataset_name in _dataset_names:
            if kwargs["name_validator"](dataset_name):
                yield os.path.join(root_path, dataset_name)


domain_metadata = domain_metadata  # pylint: disable=invalid-name


def execute_sql(statement, database_path, **kwargs):
    """Execute SQL statement via ArcSDE's SQL execution interface.

    Only works if database_path resolves to an actual SQL database.

    Args:
        statement (str): SQL statement to execute.
        database_path (str): Path of the database to execute statement in.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        object: Return value from the SQL statement execution. Likely return types:
            bool: True for successful execution of statement with no return value or
                retured rows. False if failure.
            list: A List of lists representing returned rows.
            object: A single return value.

    Raises:
        AttributeError: If statement SQL syntax is incorrect.
    """
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(level, "Start: Execute SQL statement.")
    conn = arcpy.ArcSDESQLExecute(server=database_path)
    try:
        result = conn.execute(statement)
    except AttributeError:
        LOG.exception("Incorrect SQL syntax.")
        raise

    finally:
        # Yeah, what can you do?
        del conn
    LOG.log(level, "End: Execute.")
    return result


def is_valid(workspace_path):
    """Indicate whether workspace exists/is valid.

    Args:
        workspace_path (str): Path of the workspace to verify.

    Returns:
        bool: True if workspace is valid, False otherwise.
    """
    return all(
        [
            workspace_path is not None,
            arcpy.Exists(workspace_path),
            workspace_metadata(workspace_path)["data_type"] == "Workspace",
        ]
    )
