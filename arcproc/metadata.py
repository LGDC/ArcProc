"""Metadata objects."""
from dataclasses import asdict, dataclass, field
import logging
from pathlib import Path
from typing import Optional, Union

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
    """Path to geodatabase domain resides within."""
    name: str
    """Name of the domain."""
    object: arcpy.da.Domain = field(init=False)
    """ArcPy spatial reference object."""
    code_description: "Optional[dict[str, str]]" = field(init=False)
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
    range_minimum: float = field(init=False)
    range_maximum: float = field(init=False)
    type: str = field(init=False)

    def __post_init__(self):
        geodatabase_path = Path(self.geodatabase_path)
        for domain in arcpy.da.ListDomains(str(geodatabase_path)):
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

    def as_dict(self) -> Union[int, None]:
        """Return spatial reference as a dictionary."""
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

    def as_dict(self) -> Union[int, None]:
        """Return spatial reference as a dictionary."""
        return asdict(self)
