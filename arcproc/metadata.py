"""Metadata objects."""
from dataclasses import asdict, dataclass, field
import logging
from pathlib import Path
from typing import Any, Union

import arcpy

from arcproc.exceptions import DomainNotFoundError


__all__ = []

LOG: logging.Logger = logging.getLogger(__name__)
"""Module-level logger."""

arcpy.SetLogHistory(False)


@dataclass
class Domain:
    """Representation of geodatabase domain information."""

    geodatabase_path: Union[Path, str]
    """Path to geodatabase the domain resides within."""
    name: str
    """Name of the domain."""

    object: arcpy.da.Domain = field(init=False)
    """ArcPy domain object."""
    code_description: "Union[dict[str, str], None]" = field(init=False)
    """Mapping of coded-value code to its description."""
    description: str = field(init=False)
    """Description of the domain."""
    is_coded_value: bool = field(init=False)
    """Is coded-value domain if True, False if range domain."""
    is_range: bool = field(init=False)
    """Is range domain if True, False if coded-value domain."""
    owner: str = field(init=False)
    """Owner of the domain (if enterprise geodatabase)."""
    range: "tuple[float, float]" = field(init=False)
    """Tuple of range minimum & maximum."""
    range_minimum: float = field(init=False)
    """Range minimum."""
    range_maximum: float = field(init=False)
    """Range maximum."""
    type: str = field(init=False)
    """Domain value type."""

    def __post_init__(self):
        self.geodatabase_path = Path(self.geodatabase_path)
        for domain in arcpy.da.ListDomains(self.geodatabase_path):
            if domain.name.lower() == self.name.lower():
                self.object = domain
                break

        else:
            raise DomainNotFoundError(self.geodatabase_path, self.name)

        self.code_description = self.object.codedValues
        self.description = self.object.description
        self.is_coded_value = self.object.domainType == "CodedValue"
        self.is_range = self.object.domainType == "Range"
        # To ensure property uses internal casing.
        self.name = self.object.name
        self.owner = self.object.owner
        self.range = self.object.range
        if not self.range:
            self.range_minimum, self.range_maximum = None, None
        self.type = self.object.type

    @property
    def as_dict(self) -> dict:
        """Metadata as dictionary."""
        return asdict(self)
        return asdict(self)


@dataclass
class SpatialReference:
    """Representation of spatial reference information."""

    source_item: Union[int, arcpy.Geometry, arcpy.SpatialReference, Path, str, None]
    """Source item to construct spatial reference object from."""

    object: arcpy.SpatialReference = field(init=False)
    """ArcPy spatial reference object."""
    name: str = field(init=False)
    """Name of the spatial reference."""
    wkid: Union[int, None] = field(init=False)
    """Well-known ID (WKID) for the spatial reference."""
    wkt: str = field(init=False)
    """Spatial reference as well-known text (WKT)."""
    angular_unit: str = field(init=False)
    """Angular unit for the spatial reference."""
    linear_unit: str = field(init=False)
    """Linear unit for the spatial reference."""

    def __post_init__(self):
        # WKID/factory code.
        if isinstance(self.source_item, int):
            self.object = arcpy.SpatialReference(self.source_item)
        elif isinstance(self.source_item, arcpy.Geometry):
            self.object = self.source_item.spatialReference
        elif isinstance(self.source_item, arcpy.SpatialReference):
            self.object = self.source_item
        elif isinstance(self.source_item, (Path, str)):
            # Describe-able object. spatialReference != arcpy.SpatialReference.
            self.object = arcpy.SpatialReference(
                # ArcPy2.8.0: Convert Path to str.
                arcpy.Describe(str(self.source_item)).spatialReference.factoryCode
            )
        # Allowing NoneType objects just tells ArcPy SR arguments to use dataset SR.
        if self.source_item is None:
            self.object = None
        self.name = getattr(self.object, "name", "")
        self.wkid = getattr(self.object, "factoryCode", None)
        self.wkt = getattr(self.object, "exportToString", str)()
        self.angular_unit = getattr(self.object, "angularUnitName", "")
        self.linear_unit = getattr(self.object, "linearUnitName", "")

    @property
    def as_dict(self) -> dict:
        """Metadata as dictionary."""
        return asdict(self)


@dataclass
class Workspace:
    """Representation of workspace information."""

    path: Union[Path, str]
    """Path to workspace."""

    can_copy: bool = field(init=False)
    """Workspace can be simply copied in filesystem if True."""
    can_move: bool = field(init=False)
    """Workspace can be simply moved in filesystem if True."""
    connection_string: str = field(init=False)
    """Connection string for enterprise database connection."""
    domain_names: "list[str]" = field(init=False)
    """Names of domains within workspace (geodatabase)."""
    domains: "list[Domain]" = field(init=False)
    """Metadata objects for domains within workspace (geodatabase)."""
    factory_prog_id: str = field(init=False)
    """Workspace factory prog ID. Rarely used outside metadata post-init."""
    is_enterprise_database: bool = field(init=False)
    """Workspace is an enterprise database if True."""
    is_folder: bool = field(init=False)
    """Workspace is a folder if True."""
    is_file_geodatabase: bool = field(init=False)
    """Workspace is a file geodatabase if True."""
    is_geodatabase: bool = field(init=False)
    """Workspace is a geodatabase (enterprise, file, or personal) if True."""
    is_in_memory: bool = field(init=False)
    """Workspace is the `in_memory` workspace if True."""
    is_memory: bool = field(init=False)
    """Workspace is the `memory` workspace if True."""
    is_personal_geodatabase: bool = field(init=False)
    """Workspace is a personal geodatabase if True."""
    name: str = field(init=False)
    """Name of the workspace."""
    # Not really `Any` - ArcPy describe objects are not exposed in a way to ref them.
    object: Any = field(init=False)
    """ArcPy workspace describe-object.

    Type is `Any` because ArcPy describe-objects are not exposed for reference.
    """

    def __post_init__(self):
        # ArcPy 2.8.0: Convert to str.
        self.object = arcpy.Describe(str(self.path))
        self.can_copy = self.can_move = self.object.workspaceType in [
            "FileSystem",
            "LocalDatabase",
        ]
        self.connection_string = self.object.connectionString
        self.domain_names = self.object.domains
        self.domains = [
            Domain(self.object.catalogPath, domain_name)
            for domain_name in self.domain_names
        ]
        self.factory_prog_id = self.object.workspaceFactoryProgID
        self.name = self.object.name
        # To ensure property uses internal casing & resolution.
        self.path = Path(self.object.catalogPath)
        self.is_enterprise_database = "SdeWorkspace" in self.factory_prog_id
        self.is_folder = self.factory_prog_id == ""
        self.is_file_geodatabase = "FileGDBWorkspace" in self.factory_prog_id
        self.is_geodatabase = "Database" in self.object.workspaceType
        self.is_in_memory = "InMemoryWorkspace" in self.factory_prog_id
        self.is_memory = "ColumnaDBWorkspace" in self.factory_prog_id
        self.is_personal_geodatabase = "AccessWorkspace" in self.factory_prog_id

    @property
    def as_dict(self) -> dict:
        """Metadata as dictionary."""
        return asdict(self)
