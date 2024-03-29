"""Metadata objects."""
from dataclasses import dataclass, field, fields
from logging import Logger, getLogger
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union, cast

from arcpy import Describe, Exists
from arcpy import Field as ArcField
from arcpy import Geometry, ListFields, SetLogHistory
from arcpy import SpatialReference as ArcSpatialReference
from arcpy.da import Domain as ArcDomain
from arcpy.da import ListDomains, SearchCursor
from arcpy.management import GetCount

from arcproc.exceptions import (
    DatasetNotFoundError,
    DomainNotFoundError,
    FieldNotFoundError,
)


__all__ = []

LOG: Logger = getLogger(__name__)
"""Module-level logger."""


SetLogHistory(False)


@dataclass
class Domain:
    """Representation of geodatabase domain information."""

    geodatabase_path: Union[Path, str]
    """Path to geodatabase the domain resides within."""
    name: str
    """Name of the domain."""

    description: str = field(init=False)
    """Description of the domain."""
    owner: str = field(init=False)
    """Owner of the domain (if enterprise geodatabase)."""
    type: str = field(init=False)
    """Domain value type."""
    is_coded_value: bool = field(init=False)
    """Is coded-value domain if True, False if range domain."""
    is_range: bool = field(init=False)
    """Is range domain if True, False if coded-value domain."""
    code_description: Dict[str, str] = field(init=False)
    """Mapping of coded-value code to its description."""
    range: Tuple[float, float] = field(init=False)
    """Tuple of range minimum & maximum."""
    range_minimum: float = field(init=False)
    """Range minimum."""
    range_maximum: float = field(init=False)
    """Range maximum."""
    object: ArcDomain = field(init=False)
    """ArcPy domain object."""

    def __post_init__(self) -> None:
        if not any([self.geodatabase_path and self.name]):
            raise AttributeError("Must provide `geodatabase_path` + `name` or `object`")

        self.geodatabase_path = Path(self.geodatabase_path)
        for domain in ListDomains(self.geodatabase_path):
            if domain.name.lower() == self.name.lower():
                self.object = domain
                break

        else:
            raise DomainNotFoundError(self.geodatabase_path, self.name)

        # To ensure property uses internal casing.
        self.name = self.object.name
        self.description = self.object.description
        self.owner = self.object.owner
        self.type = self.object.type
        self.is_coded_value = self.object.domainType == "CodedValue"
        self.is_range = self.object.domainType == "Range"
        self.code_description = self.object.codedValues if self.is_coded_value else {}
        if self.is_range:
            self.range = tuple(float(number) for number in self.object.range)
        else:
            self.range = (0.0, 0.0)
        self.range_minimum, self.range_maximum = self.range

    @property
    def as_dict(self) -> dict:
        """Metadata as dictionary."""
        return dict((field.name, getattr(self, field.name)) for field in fields(self))


@dataclass
class Field:
    """Representation of dataset field information."""

    dataset_path: Optional[Union[Path, str]] = None
    """Path to dataset the field resides within."""
    name: Optional[str] = None
    """Name of the field."""
    object: Optional[ArcField] = None
    """ArcPy field object."""

    alias: str = field(init=False)
    "Alias name of the field."
    default_value: Any = field(init=False)
    "Default value of the field."
    is_editable: bool = field(init=False)
    """The field is editable if True."""
    is_nullable: bool = field(init=False)
    """The field is NULL-able if True."""
    is_required: bool = field(init=False)
    """The field is required if True."""
    length: int = field(init=False)
    """Length of the field."""
    precision: int = field(init=False)
    """Precision for field values."""
    scale: int = field(init=False)
    """Scale of the field."""
    type: str = field(init=False)
    "Field value type."

    def __post_init__(self) -> None:
        if not any([self.dataset_path and self.name, self.object]):
            raise AttributeError("Must provide `dataset_path` + `name` or `object`")

        if self.dataset_path and self.name:
            self.dataset_path = Path(self.dataset_path)
            _fields = ListFields(self.dataset_path, wild_card=self.name)
            _fields = cast(List[ArcField], _fields)
            for _field in _fields:
                if _field.name.lower() == self.name.lower():
                    self.object = _field
                    break

            else:
                raise FieldNotFoundError(self.dataset_path, self.name)

        self.object = cast(ArcField, self.object)
        self.alias = self.object.aliasName
        self.default_value = self.object.defaultValue
        self.is_editable = self.object.editable
        self.is_nullable = self.object.isNullable
        self.is_required = self.object.required
        self.length = self.object.length
        # To ensure property uses internal casing.
        self.name = self.object.name
        self.precision = self.object.precision
        self.scale = self.object.scale
        self.type = self.object.type

    @property
    def as_dict(self) -> dict:
        """Metadata as dictionary."""
        return dict((field.name, getattr(self, field.name)) for field in fields(self))

    @property
    def field_as_dict(self) -> dict:
        """Field attributes as dictionary.

        Intentionally aligned with arguments for `add_field`.
        """
        attribute_names = [
            "name",
            "alias",
            "is_nullable",
            "is_required",
            "length",
            "precision",
            "scale",
            "type",
        ]
        return {name: getattr(self, name) for name in attribute_names}


@dataclass
class SpatialReference:
    """Representation of spatial reference information."""

    source_item: Union[int, Geometry, ArcSpatialReference, Path, str, None]
    """Source item to construct spatial reference object from."""

    object: ArcSpatialReference = field(init=False)
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

    def __post_init__(self) -> None:
        if isinstance(self.source_item, SpatialReference):
            self.object = self.source_item.object
        # WKID/factory code.
        elif isinstance(self.source_item, int):
            self.object = ArcSpatialReference(self.source_item)
        elif isinstance(self.source_item, Geometry):
            self.object = self.source_item.spatialReference
        elif isinstance(self.source_item, ArcSpatialReference):
            self.object = self.source_item
        elif isinstance(self.source_item, (Path, str)):
            # Describe-able object. spatialReference != ArcSpatialReference.
            if Exists(self.source_item):
                self.object = ArcSpatialReference(
                    # ArcPy2.8.0: Convert Path to str.
                    Describe(str(self.source_item)).spatialReference.factoryCode
                )
            # Likely a coordinate system name.
            else:
                self.object = ArcSpatialReference(self.source_item)
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
        return dict((field.name, getattr(self, field.name)) for field in fields(self))


@dataclass
class Workspace:
    """Representation of workspace information."""

    path: Optional[Union[Path, str]] = None
    """Path to workspace."""
    object: Optional[Any] = None
    """ArcPy workspace describe-object.

    Type is `Any` because ArcPy describe-objects are not exposed for reference.
    """

    can_copy: bool = field(init=False)
    """Workspace can be simply copied in filesystem if True."""
    can_delete: bool = field(init=False)
    """Workspace can be deleted if True."""
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

    def __post_init__(self) -> None:
        if not any([self.path, self.object]):
            raise AttributeError("Must provide `path` or `object`")

        if self.path:
            # ArcPy 2.8.0: Convert to str.
            self.object = Describe(str(self.path))

        self.can_copy = self.can_move = self.object.workspaceType in [
            "FileSystem",
            "LocalDatabase",
        ]
        self.can_delete = self.can_copy
        self.connection_string = self.object.connectionString
        self.domain_names = self.object.domains if self.object.domains else []
        self.domains = [
            Domain(self.object.catalogPath, domain_name)
            for domain_name in self.domain_names
        ]
        self.factory_prog_id = self.object.workspaceFactoryProgID
        # To ensure property uses internal casing & resolution.
        self.path = Path(self.object.catalogPath)
        self.is_enterprise_database = "SdeWorkspace" in self.factory_prog_id
        self.is_folder = self.factory_prog_id == ""
        self.is_file_geodatabase = "FileGDBWorkspace" in self.factory_prog_id
        self.is_geodatabase = "Database" in self.object.workspaceType
        self.is_in_memory = "InMemoryWorkspace" in self.factory_prog_id
        self.is_memory = "ColumnaDBWorkspace" in self.factory_prog_id
        self.is_personal_geodatabase = "AccessWorkspace" in self.factory_prog_id
        # `name` property is always the pathname.
        if self.is_enterprise_database:
            self.name = self.object.connectionProperties.database
        else:
            self.name = self.object.name

    @property
    def as_dict(self) -> dict:
        """Metadata as dictionary."""
        return dict((field.name, getattr(self, field.name)) for field in fields(self))


# Metadata classes that reference above classes.


@dataclass
class Dataset:
    """Representation of dataset information."""

    path: Optional[Path] = None
    """Path to dataset."""
    object: Optional[Any] = None
    """ArcPy workspace describe-object.

    Type is `Any` because ArcPy describe-objects are not exposed for reference.
    """

    area_field: Union[Field, None] = field(init=False)
    """Geometry area field on dataset."""
    area_field_name: str = field(init=False)
    """Name of geometry area field on dataset."""
    field_name_token: Dict[str, str] = field(default_factory=dict, init=False)
    """Mapping of field name on the dataset to appropriate token."""
    field_names: List[str] = field(default_factory=list, init=False)
    """Names of fields on the dataset."""
    field_names_tokenized: "list[str]" = field(default_factory=list, init=False)
    """Names of fields on the dataset, tokenized where relevant."""
    fields: List[Field] = field(default_factory=list, init=False)
    """Metadata instances for fields on the dataset."""
    geometry_field: Union[Field, None] = field(init=False)
    """Geometry field on dataset."""
    geometry_field_name: str = field(init=False)
    """Name of geometry field on dataset."""
    geometry_type: str = field(init=False)
    """Type of geometry represented."""
    is_spatial: bool = field(init=False)
    """The dataset is spatial if True."""
    is_table: bool = field(init=False)
    """The dataset is considered a table if True."""
    is_versioned: bool = field(init=False)
    """The dataset is versioned if True."""
    length_field: Union[Field, None] = field(init=False)
    """Geoemtry length field on dataset."""
    length_field_name: str = field(init=False)
    """Name of geometry length field on dataset."""
    name: str = field(init=False)
    """Name of the dataset."""
    oid_field: Union[Field, None] = field(init=False)
    """Object ID field on dataset."""
    oid_field_name: str = field(init=False)
    """Name of object ID field on dataset."""
    spatial_reference: SpatialReference = field(init=False)
    """Spatial reference metadata instance for dataset."""
    user_field_names: "list[str]" = field(default_factory=list, init=False)
    """Names of user-defined fields on the dataset."""
    user_fields: "list[Field]" = field(default_factory=list, init=False)
    """Metadata instances for user-defined fields on the dataset."""
    workspace_path: Union[Path, str] = field(init=False)
    """Path to workspace for the dataset resides within."""

    def __post_init__(self) -> None:
        if not any([self.path, self.object]):
            raise AttributeError("Must provide `path` or `object`")

        if self.path:
            if not Exists(self.path):
                raise DatasetNotFoundError(self.path)

            # ArcPy2.8.0: Convert to str.
            self.object = Describe(str(self.path))
        self.object = cast(Any, self.object)
        self.path = cast(Path, self.path)
        self.area_field_name = getattr(self.object, "areaFieldName", "")
        self.geometry_field_name = getattr(self.object, "shapeFieldName", "")
        self.geometry_type = getattr(self.object, "shapeType", "")
        self.is_spatial = hasattr(self.object, "shapeType")
        self.is_table = hasattr(self.object, "hasOID")
        self.is_versioned = getattr(self.object, "isVersioned", False)
        self.length_field_name = getattr(self.object, "lengthFieldName", "")
        self.name = cast(str, self.object.name)
        self.oid_field_name = getattr(self.object, "OIDFieldName", "")
        # To ensure property uses internal casing & resolution.
        self.path = Path(self.object.catalogPath)
        self.spatial_reference = SpatialReference(
            getattr(getattr(self.object, "spatialReference", None), "factoryCode", None)
        )
        self.workspace_path = Path(self.object.path)

        for field_object in getattr(self.object, "fields", []):
            _field = Field(object=field_object)
            self.field_names.append(_field.name)
            self.fields.append(_field)
            if _field.name == self.area_field_name:
                self.area_field = _field
                self.field_name_token[_field.name] = "SHAPE@AREA"
                self.field_names_tokenized.append("SHAPE@AREA")
            elif _field.name == self.geometry_field_name:
                self.geometry_field = _field
                self.field_name_token[_field.name] = "SHAPE@"
                self.field_names_tokenized.append("SHAPE@")
            elif _field.name == self.length_field_name:
                self.length_field = _field
                self.field_name_token[_field.name] = "SHAPE@LENGTH"
                self.field_names_tokenized.append("SHAPE@LENGTH")
            elif _field.name == self.oid_field_name:
                self.oid_field = _field
                self.field_name_token[_field.name] = "OID@"
                self.field_names_tokenized.append("OID@")
            else:
                self.field_names_tokenized.append(_field.name)
                self.user_field_names.append(_field.name)
                self.user_fields.append(_field)

    @property
    def as_dict(self) -> dict:
        """Metadata as dictionary."""
        return dict((field.name, getattr(self, field.name)) for field in fields(self))

    @property
    def feature_count(self) -> int:
        """Number of features in dataset."""
        # ArcPy2.8.0: Convert to str.
        return int(GetCount(str(self.path)).getOutput(0))

    @property
    def has_true_curves(self) -> bool:
        """Return True if any present features have true curves."""
        if self.is_spatial and not self.path.name.lower().endswith(".shp"):
            cursor = SearchCursor(str(self.path), field_names=["SHAPE@"])
            with cursor:
                for (geometry,) in cursor:
                    if geometry is not None and geometry.hasCurves:
                        return True

        return False


# Type aliases.


SpatialReferenceSourceItem = Union[
    SpatialReference, int, Geometry, SpatialReference, Path, str, None
]
"""Type alias for allowable SpatialReference source items."""
