"""Metadata objects."""
from dataclasses import asdict, dataclass, field
import logging
from pathlib import Path
from typing import Union

import arcpy


__all__ = []

LOG: logging.Logger = logging.getLogger(__name__)
"""Module-level logger."""

arcpy.SetLogHistory(False)


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
