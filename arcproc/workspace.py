"""Workspace operations."""
import logging
from pathlib import Path

import arcpy

from arcproc.arcobj import domain_metadata, workspace_metadata


LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""

arcpy.SetLogHistory(False)


def build_locator(locator_path, **kwargs):
    """Build locator.

    Args:
        locator_path (pathlib.Path, str): Path of the locator.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        pathlib.Path: Path of the built locator.
    """
    level = kwargs.get("log_level", logging.INFO)
    locator_path = Path(locator_path)
    LOG.log(level, "Start: Build locator `%s`.", locator_path)
    # ArcPy 2.8.0: Convert to str.
    arcpy.geocoding.RebuildAddressLocator(in_address_locator=str(locator_path))
    LOG.log(level, "End: Build.")
    return locator_path


def compress(workspace_path, disconnect_users=False, **kwargs):
    """Compress versioned geodatabase.

    Args:
        workspace_path (pathlib.Path, str): Path of the workspace.
        disconnect_users (bool): Flag to disconnect users before compressing.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        pathlib.Path: Path of compressed workspace.

    Raises:
        ValueError: If `workspace_path` doesn't reference a compressable geodatabase.
    """
    level = kwargs.get("log_level", logging.INFO)
    workspace_path = Path(workspace_path)
    LOG.log(level, "Start: Compress workspace `%s`.", workspace_path)
    # Shim: Convert to str.
    meta = {"workspace": workspace_metadata(str(workspace_path))}
    if not meta["workspace"]["is_enterprise_database"]:
        raise ValueError(f"Compressing `{workspace_path}` unsupported.")

    if disconnect_users:
        arcpy.AcceptConnections(sde_workspace=workspace_path, accept_connections=False)
        # ArcPy2.8.0: Convert to str.
        arcpy.DisconnectUser(sde_workspace=str(workspace_path), users="ALL")
    # ArcPy2.8.0: Convert to str.
    arcpy.management.Compress(in_workspace=str(workspace_path))
    if disconnect_users:
        arcpy.AcceptConnections(sde_workspace=workspace_path, accept_connections=True)
    LOG.log(level, "End: Compress.")
    return workspace_path


def copy(workspace_path, output_path, **kwargs):
    """Copy workspace to another location.

    Args:
        workspace_path (pathlib.Path, str): Path of the workspace.
        output_path (pathlib.Path, str): Path of output workspace.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        pathlib.Path: Output path of copied workspace.

    Raises:
        ValueError: If dataset type not supported.
    """
    level = kwargs.get("log_level", logging.INFO)
    output_path = Path(output_path)
    workspace_path = Path(workspace_path)
    LOG.log(level, "Start: Copy workspace `%s` to `%s`.", workspace_path, output_path)
    # Shim: Convert to str.
    meta = {"dataset": workspace_metadata(str(workspace_path))}
    if not meta["dataset"]["can_copy"]:
        raise ValueError(f"`{workspace_path}` unsupported dataset type.")

    # ArcPy2.8.0: Convert to str x2.
    arcpy.management.Copy(in_data=str(workspace_path), out_data=str(output_path))
    LOG.log(level, "End: Copy.")
    return output_path


def create_file_geodatabase(
    geodatabase_path, xml_workspace_path=None, include_xml_data=False, **kwargs
):
    """Create new file geodatabase.

    Args:
        geodatabase_path (pathlib.Path, str): Path of the geodatabase.
        xml_workspace_path (pathlib.Path, str, None): Path of the XML workspace document
            to initialize the geodatabase with.
        include_xml_data (bool): Flag to include data stored in the XML workspace
            document, if it has any.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        pathlib.Path: Path of created file geodatabase.
    """
    geodatabase_path = Path(geodatabase_path)
    if xml_workspace_path:
        xml_workspace_path = Path(xml_workspace_path)
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(level, "Start: Create file geodatabase `%s`.", geodatabase_path)
    if geodatabase_path.exists():
        LOG.warning("Geodatabase already exists.")
        return geodatabase_path

    arcpy.management.CreateFileGDB(
        # ArcPy2.8.0: Convert to str.
        out_folder_path=str(geodatabase_path.parent),
        out_name=geodatabase_path.name,
        out_version="CURRENT",
    )
    if xml_workspace_path:
        arcpy.management.ImportXMLWorkspaceDocument(
            # ArcPy2.8.0: Convert to str.
            target_geodatabase=str(geodatabase_path),
            # ArcPy2.8.0: Convert to str.
            in_file=str(xml_workspace_path),
            import_type=("DATA" if include_xml_data else "SCHEMA_ONLY"),
            config_keyword="DEFAULTS",
        )
    LOG.log(level, "End: Create.")
    return geodatabase_path


def create_geodatabase_xml_backup(
    geodatabase_path, output_path, include_data=False, include_metadata=True, **kwargs
):
    """Create backup of geodatabase as XML workspace document.

    Args:
        geodatabase_path (pathlib.Path, str): Path of the geodatabase.
        output_path (pathlib.Path, str): Path of the XML workspace document to create.
        include_data (bool): Flag to include data in the output.
        include_metadata (bool): Flag to include metadata in the output.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        pathlib.Path: Path of created XML workspace document.
    """
    geodatabase_path = Path(geodatabase_path)
    level = kwargs.get("log_level", logging.INFO)
    output_path = Path(output_path)
    LOG.log(
        level,
        "Start: Create XML backup of geodatabase `%s` at `%s`.",
        geodatabase_path,
        output_path,
    )
    arcpy.management.ExportXMLWorkspaceDocument(
        # ArcPy2.8.0: Conver to str.
        in_data=str(geodatabase_path),
        # ArcPy2.8.0: Conver to str.
        out_file=str(output_path),
        export_type=("DATA" if include_data else "SCHEMA_ONLY"),
        storage_type="BINARY",
        export_metadata=include_metadata,
    )
    LOG.log(level, "End: Create.")
    return output_path


def dataset_names(workspace_path, **kwargs):
    """Generate names of datasets in workspace.

    Args:
        workspace_path (pathlib.Path, str): Path of the workspace.
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
    workspace_path = Path(workspace_path)
    kwargs.setdefault("include_feature_classes", True)
    kwargs.setdefault("include_rasters", True)
    kwargs.setdefault("include_tables", True)
    kwargs.setdefault("only_top_level", False)
    kwargs.setdefault("name_validator", lambda n: True)
    for dataset_path in dataset_paths(workspace_path, **kwargs):
        yield dataset_path.name


def dataset_paths(workspace_path, **kwargs):
    """Generate paths of datasets in workspace.

    Args:
        workspace_path (pathlib.Path, str): Path of the workspace.
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
        pathlib.Path: Path of the next dataset in the workspace.
    """
    workspace_path = Path(workspace_path)
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
        top=workspace_path, datatype=include_data_types
    ):
        root_path = Path(root_path)
        if kwargs["only_top_level"] and root_path != workspace_path:
            continue

        for dataset_name in _dataset_names:
            if kwargs["name_validator"](dataset_name):
                yield root_path / dataset_name


domain_metadata = domain_metadata  # pylint: disable=invalid-name


def execute_sql(statement, database_path, **kwargs):
    """Execute SQL statement via ArcSDE's SQL execution interface.

    Only works if database_path resolves to an actual SQL database.

    Args:
        statement (str): SQL statement to execute.
        database_path (pathlib.Path, str): Path of the database to execute statement in.
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
    database_path = Path(database_path)
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
    """Indicate whether workspace exists and is valid.

    Args:
        workspace_path (pathlib.Path, str): Path of the workspace to validate.

    Returns:
        bool: True if workspace is valid, False otherwise.
    """
    workspace_path = Path(workspace_path)
    valid = False
    if arcpy.Exists(workspace_path):
        # Shim: Convert to str.
        valid = workspace_metadata(str(workspace_path))["data_type"] == "Workspace"
    return valid
