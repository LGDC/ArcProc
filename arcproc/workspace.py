"""Workspace-level operations."""
import pathlib
import typing
from contextlib import ContextDecorator
from logging import INFO, Logger, getLogger
from pathlib import Path
from types import FunctionType, TracebackType
from typing import Iterator, Optional, Type, TypeVar, Union

import arcpy
from arcpy import Describe, Exists, SetLogHistory
from arcpy.da import Editor, Walk
from arcpy.geocoding import RebuildAddressLocator
from arcpy.management import (
    Copy,
    CreateFileGDB,
    Delete,
    ExportXMLWorkspaceDocument,
    ImportXMLWorkspaceDocument,
)

from arcproc.metadata import Dataset, Workspace


LOG: Logger = getLogger(__name__)
"""Module-level logger."""

SetLogHistory(False)

# Py3.7: Can replace usage with `typing.Self` in Py3.11.
TSession = TypeVar("TSession", bound="Session")
"""Type variable to enable method return of self on Session."""


class Session(ContextDecorator):
    """Context manager for a workspace session."""

    workspace_path: Path
    """Path to workspace."""

    def __init__(
        self, workspace_path: Union[Path, str], use_edit_session: bool = True
    ) -> None:
        """Initialize instance.

        Args:
            workspace_path: Path to workspace.
            use_edit_session: True if session is to be an active edit session.
        """
        workspace_path = Path(workspace_path)
        self._editor = Editor(workspace_path) if use_edit_session else None
        self.workspace_path = workspace_path

    def __enter__(self) -> TSession:
        self.start()
        return self

    def __exit__(
        self,
        exception_type: Optional[Type[BaseException]],
        exception_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> bool:
        self.stop(save_changes=(not exception_type))

    @property
    def active(self) -> bool:
        """Edit session is active if True."""
        return self._editor.isEditing if self._editor else False

    def start(self) -> bool:
        """Start an active edit session.

        Returns:
            State of edit session after attempting start.
        """
        if self._editor and not self._editor.isEditing:
            self._editor.startEditing(with_undo=True, multiuser_mode=True)
            self._editor.startOperation()
        return self.active

    def stop(self, save_changes: bool = True) -> bool:
        """Stop an active edit session.

        Args:
            save_changes: Save edit changes if True.

        Returns:
            State of edit session after attempting start.
        """
        if self._editor and self._editor.isEditing:
            if save_changes:
                self._editor.stopOperation()
            else:
                self._editor.abortOperation()
            self._editor.stopEditing(save_changes)
        return not self.active


def build_locator(locator_path: Union[Path, str], *, log_level: int = INFO) -> bool:
    """Build locator.

    Args:
        locator_path: Path to locator.
        log_level: Level to log the function at.

    Returns:
        True if successfully built, False otherwise.
    """
    locator_path = Path(locator_path)
    LOG.log(log_level, "Start: Build locator `%s`.", locator_path)
    # ArcPy 2.8.0: Convert to str.
    RebuildAddressLocator(in_address_locator=str(locator_path))
    LOG.log(log_level, "End: Build.")
    return True


def compress_geodatabase_versions(
    geodatabase_path: typing.Union[pathlib.Path, str], *, disconnect_users: bool = False
) -> tuple[bool, int, int]:
    """Compress versions to default in enterprise geodatabase.

    Note: This tool assumes the compress log is named `SDE_compress_log`.

    Args:
        geodatabase_path: Path to geodatabase.
        disconnect_users: Disconnect users before compressing if True.

    Returns:
        Tuple containing (result boolean, start state count, end state count).
    """
    LOG.info("Start: Compress versioned geodatabase via %s.", geodatabase_path)
    if disconnect_users:
        arcpy.AcceptConnections(geodatabase_path, accept_connections=False)
        # ArcPy2.8.0: Convert to str.
        arcpy.DisconnectUser(str(geodatabase_path), users="ALL")
    # ArcPy2.8.0: Convert to str.
    arcpy.management.Compress(str(geodatabase_path))
    if disconnect_users:
        arcpy.AcceptConnections(geodatabase_path, accept_connections=True)
    for dirpath, _, table_names in arcpy.da.Walk(geodatabase_path, datatype="Table"):
        for table_name in table_names:
            if table_name.lower().endswith("sde_compress_log"):
                log_path = pathlib.Path(dirpath, table_name)
                break

        else:
            continue

        break

    with arcpy.da.SearchCursor(
        # ArcPy2.8.0: Convert to str.
        str(log_path),
        field_names=["compress_status", "start_state_count", "end_state_count"],
        sql_clause=(None, "ORDER BY compress_start DESC"),
    ) as cursor:
        status, start_state_count, end_state_count = next(cursor)
    LOG.info("End: Compress.")
    return (status == "SUCCESS", start_state_count, end_state_count)


def copy_workspace(
    workspace_path: Union[Path, str],
    *,
    output_path: Union[Path, str],
    log_level: int = INFO,
) -> Workspace:
    """Copy workspace to another location.

    Args:
        workspace_path: Path to workspace.
        output_path: Path to copied workspace.
        log_level: Level to log the function at.

    Returns:
        Workspace metadata instance for output workspace.

    Raises:
        ValueError: If workspace type not supported.
    """
    output_path = Path(output_path)
    workspace_path = Path(workspace_path)
    LOG.log(
        log_level, "Start: Copy workspace `%s` to `%s`.", workspace_path, output_path
    )
    if not Workspace(workspace_path).can_copy:
        raise ValueError(f"`{workspace_path}` unsupported dataset type.")

    # ArcPy2.8.0: Convert to str x2.
    Copy(in_data=str(workspace_path), out_data=str(output_path))
    LOG.log(log_level, "End: Copy.")
    return Workspace(output_path)


def create_file_geodatabase(
    geodatabase_path: Union[Path, str],
    *,
    xml_workspace_path: Optional[Union[Path, str]] = None,
    include_xml_data: bool = False,
    log_level: int = INFO,
) -> Workspace:
    """Create new file geodatabase.

    Args:
        geodatabase_path: Path to file geodatabase.
        xml_workspace_path: Path to XML workspace document to initialize geodatabase.
        include_xml_data: Include any data stored in the XML workspace document if True.
        log_level: Level to log the function at.

    Returns:
        Workspace metadata instance for file geodatabase.
    """
    geodatabase_path = Path(geodatabase_path)
    LOG.log(log_level, "Start: Create file geodatabase at `%s`.", geodatabase_path)
    if xml_workspace_path:
        xml_workspace_path = Path(xml_workspace_path)
    if geodatabase_path.exists():
        LOG.warning("Geodatabase already exists.")
    else:
        CreateFileGDB(
            # ArcPy2.8.0: Convert to str.
            out_folder_path=str(geodatabase_path.parent),
            out_name=geodatabase_path.name,
            out_version="CURRENT",
        )
        if xml_workspace_path:
            ImportXMLWorkspaceDocument(
                # ArcPy2.8.0: Convert to str.
                target_geodatabase=str(geodatabase_path),
                # ArcPy2.8.0: Convert to str.
                in_file=str(xml_workspace_path),
                import_type=("DATA" if include_xml_data else "SCHEMA_ONLY"),
                config_keyword="DEFAULTS",
            )
    LOG.log(log_level, "End: Create.")
    return Workspace(geodatabase_path)


def create_geodatabase_xml_backup(
    geodatabase_path: Union[Path, str],
    *,
    output_path: Union[Path, str],
    include_data: bool = False,
    include_metadata: bool = True,
    log_level: int = INFO,
) -> Path:
    """Create backup of geodatabase as XML workspace document.

    Args:
        geodatabase_path: Path to geodatabase.
        output_path: Path to XML workspace document.
        include_data (bool): Include data in output if True.
        include_metadata (bool): Include metadata in output if True.
        log_level: Level to log the function at.

    Returns:
        Path to created XML workspace document.
    """
    geodatabase_path = Path(geodatabase_path)
    output_path = Path(output_path)
    LOG.log(
        log_level,
        "Start: Create XML backup of geodatabase `%s` at `%s`.",
        geodatabase_path,
        output_path,
    )
    ExportXMLWorkspaceDocument(
        # ArcPy2.8.0: Conver to str.
        in_data=str(geodatabase_path),
        # ArcPy2.8.0: Conver to str.
        out_file=str(output_path),
        export_type=("DATA" if include_data else "SCHEMA_ONLY"),
        storage_type="BINARY",
        export_metadata=include_metadata,
    )
    LOG.log(log_level, "End: Create.")
    return output_path


def delete_workspace(
    workspace_path: Union[Path, str], *, log_level: int = INFO
) -> Workspace:
    """Delete workspace.

    Args:
        workspace_path: Path to workspace.
        log_level: Level to log the function at.

    Returns:
        Workspace metadata instance for now-deleted workspace.
    """
    workspace_path = Path(workspace_path)
    LOG.log(log_level, "Start: Delete workspace `%s`.", workspace_path)
    if not is_valid_workspace(workspace_path):
        raise ValueError(f"`{workspace_path}` not a valid workspace.")

    _workspace = Workspace(workspace_path)
    if not _workspace.can_delete:
        raise ValueError(f"`{workspace_path}` unsupported workspace type.")

    # ArcPy2.8.0: Convert to str.
    Delete(in_data=str(workspace_path))
    LOG.log(log_level, "End: Delete.")
    return _workspace


def is_valid_workspace(workspace_path: Union[Path, str]) -> bool:
    """Return True if workspace is extant & valid.

    Args:
        workspace_path: Path to workspace.
    """
    workspace_path = Path(workspace_path)
    exists = workspace_path and Exists(dataset=workspace_path)
    if exists:
        # ArcPy2.8.0: Conver to str.
        valid = Describe(str(workspace_path)).dataType == "Workspace"
    else:
        valid = False
    return valid


def workspace_dataset_names(
    workspace_path: Union[Path, str],
    *,
    include_feature_classes: bool = True,
    include_rasters: bool = True,
    include_tables: bool = True,
    only_top_level: bool = False,
    name_validator: Optional[FunctionType] = None,
) -> Iterator[str]:
    """Generate names of datasets in workspace.

    Args:
        workspace_path: Path to workspace.
        include_feature_classes: Include feature class datasets in generator if True.
        include_rasters: Include raster datasets in generator if True.
        include_tables: Include table datasets in generator if True.
        only_top_level: List only datasets at the top-level of the workspace if True.
        name_validator: Function to validate dataset names yielded.
    """
    workspace_path = Path(workspace_path)
    for dataset_path in workspace_dataset_paths(
        workspace_path,
        include_feature_classes=include_feature_classes,
        include_rasters=include_rasters,
        include_tables=include_tables,
        only_top_level=only_top_level,
        name_validator=name_validator,
    ):
        yield dataset_path.name


def workspace_dataset_paths(
    workspace_path: Union[Path, str],
    *,
    include_feature_classes: bool = True,
    include_rasters: bool = True,
    include_tables: bool = True,
    only_top_level: bool = False,
    name_validator: Optional[FunctionType] = None,
) -> Iterator[Path]:
    """Generate paths of datasets in workspace.

    Args:
        workspace_path: Path to workspace.
        include_feature_classes: Include feature class datasets in generator if True.
        include_rasters: Include raster datasets in generator if True.
        include_tables: Include table datasets in generator if True.
        only_top_level: List only datasets at the top-level of the workspace if True.
        name_validator: Function to validate dataset names yielded.
    """
    workspace_path = Path(workspace_path)
    data_types = []
    if include_feature_classes:
        data_types.append("FeatureClass")
    if include_rasters:
        data_types += ["RasterCatalog", "RasterDataset"]
    if include_tables:
        data_types.append("Table")
    for root_path, _, dataset_names in Walk(workspace_path, datatype=data_types):
        root_path = Path(root_path)
        if only_top_level and root_path != workspace_path:
            continue

        for dataset_name in dataset_names:
            if name_validator:
                if not name_validator(dataset_name):
                    continue

            yield root_path / dataset_name


def workspace_datasets(
    workspace_path: Union[Path, str],
    *,
    include_feature_classes: bool = True,
    include_rasters: bool = True,
    include_tables: bool = True,
    only_top_level: bool = False,
    name_validator: Optional[FunctionType] = None,
) -> Iterator[Dataset]:
    """Generate Dataset metadata objects of datasets in workspace.

    Args:
        workspace_path: Path to workspace.
        include_feature_classes: Include feature class datasets in generator if True.
        include_rasters: Include raster datasets in generator if True.
        include_tables: Include table datasets in generator if True.
        only_top_level: List only datasets at the top-level of the workspace if True.
        name_validator: Function to validate dataset names yielded.
    """
    workspace_path = Path(workspace_path)
    for dataset_path in workspace_dataset_paths(
        workspace_path,
        include_feature_classes=include_feature_classes,
        include_rasters=include_rasters,
        include_tables=include_tables,
        only_top_level=only_top_level,
        name_validator=name_validator,
    ):
        yield Dataset(dataset_path)
