"""Workspace operations."""
import logging
from contextlib import ContextDecorator
from pathlib import Path
from types import FunctionType, TracebackType
from typing import Iterator, Optional, Type, TypeVar, Union

import arcpy

from arcproc.metadata import Workspace


LOG: logging.Logger = logging.getLogger(__name__)
"""Module-level logger."""

# Py3.7: Can replace usage with `typing.Self` in Py3.11.
TEditing = TypeVar("TEditing", bound="Editing")
"""Type variable to enable method return of self on Editing."""


arcpy.SetLogHistory(False)


class Editing(ContextDecorator):
    """Context manager for editing features."""

    workspace_path: Path
    """Path to editing workspace."""

    def __init__(
        self, workspace_path: Union[Path, str], use_edit_session: bool = True
    ) -> None:
        """Initialize instance.

        Args:
            workspace_path: Path to editing workspace.
            use_edit_session: True if edits are to be made in an edit session.
        """
        workspace_path = Path(workspace_path)
        self._editor = arcpy.da.Editor(workspace_path) if use_edit_session else None
        self.workspace_path = workspace_path

    def __enter__(self) -> TEditing:
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


def build_locator(
    locator_path: Union[Path, str], *, log_level: int = logging.INFO
) -> bool:
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
    arcpy.geocoding.RebuildAddressLocator(in_address_locator=str(locator_path))
    LOG.log(log_level, "End: Build.")
    return True


def copy(
    workspace_path: Union[Path, str],
    *,
    output_path: Union[Path, str],
    log_level: int = logging.INFO,
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
    arcpy.management.Copy(in_data=str(workspace_path), out_data=str(output_path))
    LOG.log(log_level, "End: Copy.")
    return Workspace(output_path)


def create_file_geodatabase(
    geodatabase_path: Union[Path, str],
    *,
    xml_workspace_path: Optional[Union[Path, str]] = None,
    include_xml_data: bool = False,
    log_level: int = logging.INFO,
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
    LOG.log(log_level, "End: Create.")
    return Workspace(geodatabase_path)


def create_geodatabase_xml_backup(
    geodatabase_path: Union[Path, str],
    *,
    output_path: Union[Path, str],
    include_data: bool = False,
    include_metadata: bool = True,
    log_level: int = logging.INFO,
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
    arcpy.management.ExportXMLWorkspaceDocument(
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


def dataset_names(
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
    for dataset_path in dataset_paths(
        workspace_path,
        include_feature_classes=include_feature_classes,
        include_rasters=include_rasters,
        include_tables=include_tables,
        only_top_level=only_top_level,
        name_validator=name_validator,
    ):
        yield dataset_path.name


def dataset_paths(
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
    for root_path, _, _dataset_names in arcpy.da.Walk(
        workspace_path, datatype=data_types
    ):
        root_path = Path(root_path)
        if only_top_level and root_path != workspace_path:
            continue

        for dataset_name in _dataset_names:
            if name_validator:
                if not name_validator(dataset_name):
                    continue

            yield root_path / dataset_name


def delete(
    workspace_path: Union[Path, str], *, log_level: int = logging.INFO
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
    if not is_valid(workspace_path):
        raise ValueError(f"`{workspace_path}` not a valid workspace.")

    _workspace = Workspace(workspace_path)
    if not _workspace.can_delete:
        raise ValueError(f"`{workspace_path}` unsupported workspace type.")

    # ArcPy2.8.0: Convert to str.
    arcpy.management.Delete(in_data=str(workspace_path))
    LOG.log(log_level, "End: Delete.")
    return _workspace


def is_valid(workspace_path: Union[Path, str]) -> bool:
    """Return True if workspace is extant & valid.

    Args:
        workspace_path: Path to workspace.
    """
    workspace_path = Path(workspace_path)
    exists = workspace_path and arcpy.Exists(dataset=workspace_path)
    if exists:
        # ArcPy2.8.0: Conver to str.
        valid = arcpy.Describe(str(workspace_path)).dataType == "Workspace"
    else:
        valid = False
    return valid
