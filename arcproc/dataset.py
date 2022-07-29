"""Dataset-level operations."""
from contextlib import ContextDecorator
from functools import partial
from logging import DEBUG, INFO, Logger, getLogger
from pathlib import Path
from types import TracebackType
from typing import Any, Iterable, Iterator, List, Optional, Type, TypeVar, Union

from arcpy import ExecuteError, Exists, FeatureSet, FieldInfo, RecordSet, SetLogHistory
from arcpy.conversion import FeatureClassToFeatureClass, TableToTable
from arcpy.da import ListSubtypes, SearchCursor
from arcpy.management import (
    AddField,
    AddIndex,
    AddSpatialIndex,
    AssignDefaultToField,
    CompressFileGeodatabaseData,
    CopyFeatures,
    CopyRows,
    CreateFeatureclass,
    CreateTable,
    Delete,
    GetCount,
    MakeFeatureLayer,
    MakeTableView,
    SelectLayerByAttribute,
)

from arcproc.helpers import unique_name
from arcproc.metadata import (
    Dataset,
    Field,
    SpatialReference,
    SpatialReferenceSourceItem,
)


LOG: Logger = getLogger(__name__)
"""Module-level logger."""

SetLogHistory(False)

# Py3.7: Can replace usage with `typing.Self` in Py3.11.
TDatasetView = TypeVar("TDatasetView", bound="DatasetView")
"""Type variable to enable method return of self on DatasetView."""
# Py3.7: Can replace usage with `typing.Self` in Py3.11.
TTempDatasetCopy = TypeVar("TTempDatasetCopy", bound="TempDatasetCopy")
"""Type variable to enable method return of self on TempDatasetCopy."""


class DatasetView(ContextDecorator):
    """Context manager for an ArcGIS dataset view (feature layer/table view)."""

    dataset: Dataset
    """Metadata instance for dataset."""
    dataset_path: Path
    """Path to dataset."""
    field_names: List[str]
    """Collection of field names to include in view."""
    is_spatial: bool
    """True if view is spatial, False if not."""
    name: str
    """Name to give view."""

    def __init__(
        self,
        dataset_path: Union[Path, str],
        *,
        field_names: Optional[Iterable[str]] = None,
        dataset_where_sql: Optional[str] = None,
        force_nonspatial: bool = False,
        name: Optional[str] = None,
    ) -> None:
        """Initialize instance.

        Args:
            dataset_path: Path to dataset.
            name: Name of view. If set to None, name will be auto-generated.
            field_names: Collection of field names to include in view. If set to None,
                all fields will be included.
            dataset_where_sql: SQL where-clause for dataset subselection.
            force_nonspatial: Forces view to be nonspatial if True.
        """
        self.dataset = Dataset(path=dataset_path)
        self.dataset_path = Path(dataset_path)
        self._dataset_where_sql = dataset_where_sql
        self.field_names = (
            self.dataset.field_names if field_names is None else list(field_names)
        )
        self.is_spatial = self.dataset.is_spatial and not force_nonspatial
        self.name = name if name else unique_name("View")

    def __enter__(self) -> TDatasetView:
        return self.create()

    def __exit__(
        self,
        exception_type: Optional[Type[BaseException]],
        exception_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> bool:
        self.discard()

    @property
    def feature_count(self) -> int:
        """Number of features in view."""
        return int(GetCount(self.name).getOutput(0))

    @property
    def dataset_where_sql(self) -> str:
        """SQL where-clause property for dataset subselection.

        Setting this property will change dataset subselection for the view.
        """
        return self._dataset_where_sql

    @dataset_where_sql.setter
    def dataset_where_sql(self, value: str) -> None:
        if self.exists:
            SelectLayerByAttribute(
                in_layer_or_view=self.name,
                selection_type="NEW_SELECTION",
                where_clause=value,
            )
        self._dataset_where_sql = value

    @dataset_where_sql.deleter
    def dataset_where_sql(self) -> None:
        if self.exists:
            SelectLayerByAttribute(
                in_layer_or_view=self.name, selection_type="CLEAR_SELECTION"
            )
        self._dataset_where_sql = None

    @property
    def exists(self) -> bool:
        """True if view currently exists, False otherwise."""
        return Exists(self.name)

    @property
    def field_info(self) -> FieldInfo:
        """Field information object of field settings for the view."""
        cmp_field_names = [name.lower() for name in self.field_names]
        field_info = FieldInfo()
        split_rule = "NONE"
        for field_name in self.dataset.field_names:
            visible = "VISIBLE" if field_name.lower() in cmp_field_names else "HIDDEN"
            field_info.addField(field_name, field_name, visible, split_rule)
        return field_info

    def as_chunks(self, chunk_size: int) -> Iterator[TDatasetView]:
        """Generate "chunks" of view features in new DatasetView.

        DatasetView yielded under context management, i.e. view will be discarded
        when generator moves to next chunk-view.

        Args:
            chunk_size: Number of features in each chunk-view.
        """
        # ArcPy where clauses cannot use `BETWEEN`.
        where_sql_template = (
            "{oid_field_name} >= {from_oid} AND {oid_field_name} <= {to_oid}"
        )
        if self._dataset_where_sql:
            where_sql_template += f" AND ({self._dataset_where_sql})"
        # Get iterable of all object IDs in dataset.
        # ArcPy2.8.0: Convert to str.
        cursor = SearchCursor(
            in_table=str(self.dataset_path),
            field_names=["OID@"],
            where_clause=self._dataset_where_sql,
        )
        with cursor:
            # Sorting is important: allows selection by ID range.
            oids = sorted(oid for oid, in cursor)
        while oids:
            chunk_where_sql = where_sql_template.format(
                oid_field_name=self.dataset.oid_field_name,
                from_oid=min(oids),
                to_oid=max(oids[:chunk_size]),
            )
            chunk_view = DatasetView(self.name, dataset_where_sql=chunk_where_sql)
            with chunk_view:
                yield chunk_view

            # Remove chunk from set.
            oids = oids[chunk_size:]

    def create(self) -> TDatasetView:
        """Create view."""
        kwargs = {
            "where_clause": self.dataset_where_sql,
            # ArcPy2.8.0: Convert to str.
            "workspace": str(self.dataset.workspace_path),
            "field_info": self.field_info,
        }
        if self.is_spatial:
            MakeFeatureLayer(
                # ArcPy2.8.0: Convert to str.
                in_features=str(self.dataset_path),
                out_layer=self.name,
                **kwargs,
            )
        else:
            MakeTableView(
                # ArcPy2.8.0: Convert to str.
                in_table=str(self.dataset_path),
                out_view=self.name,
                **kwargs,
            )
        return self

    def discard(self) -> bool:
        """Discard view.

        Returns:
            True if view discarded, False otherwise.
        """
        if self.exists:
            Delete(self.name)
        return not self.exists


class TempDatasetCopy(ContextDecorator):
    """Context manager for a temporary copy of a dataset."""

    copy_path: Path
    """Path to copy dataset."""
    dataset: Dataset
    """Metadata instance for original dataset."""
    dataset_path: Path
    """Path to original dataset."""
    dataset_where_sql: Optional[str] = None
    """SQL where-clause property for original dataset subselection."""
    field_names: List[str]
    """Collection of field names to include in copy."""
    is_spatial: bool
    """True if copy is spatial, False if not."""

    def __init__(
        self,
        dataset_path: Union[Path, str],
        *,
        field_names: Optional[Iterable[str]] = None,
        dataset_where_sql: Optional[str] = None,
        copy_path: Optional[Union[Path, str]] = None,
        force_nonspatial: bool = False,
    ) -> None:
        """Initialize instance.

        Note:
            To make a temp dataset without copying any template rows:
            `dataset_where_sql="0 = 1"`

        Args:
            dataset_path: Path to original dataset.
            copy_path: Path to copy dataset. If set to None, path will be auto-
                generated.
            field_names: Collection of field names to include in copy. If set to None,
                all fields will be included.
            dataset_where_sql: SQL where-clause property for original dataset
                subselection.
            force_nonspatial: Forces view to be nonspatial if True.
        """
        self.copy_path = (
            Path(copy_path) if copy_path else unique_dataset_path("TempCopy")
        )
        self.dataset = Dataset(path=dataset_path)
        self.dataset_path = Path(dataset_path)
        self.dataset_where_sql = dataset_where_sql
        self.field_names = (
            self.dataset.field_names if field_names is None else list(field_names)
        )
        self.is_spatial = self.dataset.is_spatial and not force_nonspatial

    def __enter__(self) -> TTempDatasetCopy:
        return self.create()

    def __exit__(
        self,
        exception_type: Optional[Type[BaseException]],
        exception_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> bool:
        self.discard()

    @property
    def exists(self) -> bool:
        """True if copy dataset currently exists, False otherwise."""
        return Exists(self.copy_path)

    def create(self) -> TTempDatasetCopy:
        """Create copy dataset."""
        view = DatasetView(
            self.dataset_path,
            field_names=self.field_names,
            dataset_where_sql=self.dataset_where_sql,
            force_nonspatial=(not self.is_spatial),
        )
        with view:
            if self.is_spatial:
                # ArcPy2.8.0: Convert to str.
                CopyFeatures(
                    in_features=view.name, out_feature_class=str(self.copy_path)
                )
            else:
                # ArcPy2.8.0: Convert to str.
                CopyRows(in_rows=view.name, out_table=str(self.copy_path))
        return self

    def discard(self) -> bool:
        """Discard copy dataset.

        Returns:
            True if dataset discarded, False otherwise.
        """
        if self.exists:
            # ArcPy2.8.0: Convert to str.
            Delete(str(self.copy_path))
        return not self.exists


def add_index(
    dataset_path: Union[Path, str],
    *,
    field_names: Iterable[str],
    index_name: Optional[str] = None,
    is_ascending: bool = False,
    is_unique: bool = False,
    fail_on_lock_ok: bool = False,
    log_level: int = INFO,
) -> List[Field]:
    """Add index to dataset fields.

    Args:
        dataset_path: Path to dataset.
        field_names: Sequence of participating field names.
        index_name (str): Name for index. Only applicable to non-spatial indexes for
            geodatabase datasets.
        is_ascending: Build index with values in ascending order if True. Only
            applicable to non-spatial indexes for enterprise geodatabase datasets.
        is_unique: Build index with unique constraint if True. Only
            applicable to non-spatial indexes for enterprise geodatabase datasets.
        fail_on_lock_ok: If True, indicate success even if dataset locks prevent
            adding index.
        log_level: Level to log the function at.

    Returns:
        Sequence of field metadata instances for participating fields.

    Raises:
        RuntimeError: If more than one field and any are geometry-type.
        arcpy.ExecuteError: If dataset lock prevents adding index.
    """
    dataset_path = Path(dataset_path)
    field_names = list(field_names)
    LOG.log(
        log_level,
        "Start: Add index to field(s) `%s` on `%s`.",
        field_names,
        dataset_path,
    )
    field_types = {
        field.type.upper()
        for field in Dataset(dataset_path).fields
        if field.name.lower() in [field_name.lower() for field_name in field_names]
    }
    if "GEOMETRY" in field_types:
        if len(field_names) > 1:
            raise RuntimeError("Cannot create a composite spatial index.")

        # ArcPy2.8.0: Convert to str.
        func = partial(AddSpatialIndex, in_features=str(dataset_path))
    else:
        func = partial(
            AddIndex,
            # ArcPy2.8.0: Convert to str.
            in_table=str(dataset_path),
            fields=field_names,
            index_name=index_name,
            unique=is_unique,
            ascending=is_ascending,
        )
    try:
        func()
    except ExecuteError as error:
        if error.message.startswith("ERROR 000464"):
            LOG.warning("Lock on `%s` prevents adding index.", dataset_path)
            if not fail_on_lock_ok:
                raise

    LOG.log(log_level, "End: Add.")
    return [Field(dataset_path, field_name) for field_name in field_names]


def compress_dataset(
    dataset_path: Union[Path, str],
    *,
    bad_allocation_ok: bool = False,
    log_level: int = INFO,
) -> Dataset:
    """Compress dataset.

    Only applicable to file geodatabase datasets.

    Args:
        dataset_path: Path to dataset.
        bad_allocation_ok: Will not raise ExecuteError on bad allocations. "Bad
            allocation" generally occurs when dataset is too big to compress.
        log_level: Level to log the function at.

    Returns:
        Dataset metadata instance for compressed dataset.
    """
    dataset_path = Path(dataset_path)
    LOG.log(log_level, "Start: Compress dataset `%s`.", dataset_path)
    try:
        # ArcPy2.8.0: Convert to str.
        CompressFileGeodatabaseData(in_data=str(dataset_path))
    except ExecuteError as error:
        # Bad allocation error just means the dataset is too big to compress.
        if str(error) == (
            "bad allocation\nFailed to execute (CompressFileGeodatabaseData).\n"
        ):
            LOG.error("Compress error: bad allocation.")
            if not bad_allocation_ok:
                raise

        else:
            LOG.error("""str(error) = "%s\"""", error)
            LOG.error("""repr(error) = "%r\"""", error)
            raise

    LOG.log(log_level, "End: Compress.")
    return Dataset(dataset_path)


def copy_dataset(
    dataset_path: Union[Path, str],
    *,
    field_names: Optional[Iterable[str]] = None,
    dataset_where_sql: Optional[str] = None,
    output_path: Union[Path, str],
    overwrite: bool = False,
    schema_only: bool = False,
    log_level: int = INFO,
) -> Dataset:
    """Copy dataset features into new dataset.

    Args:
        dataset_path: Path to dataset.
        output_path: Path to output dataset.
        field_names: Collection of field names to include in output. If set to None, all
            fields will be included.
        dataset_where_sql: SQL where-clause property for dataset subselection.
        overwrite: Overwrite existing dataset at output path if True.
        schema_only: Copy only the schema--omitting data--if True.
        log_level: Level to log the function at.

    Returns:
        Dataset metadata instance for output dataset.

    Raises:
        ValueError: If dataset type not supported.
    """
    dataset_path = Path(dataset_path)
    output_path = Path(output_path)
    if field_names is not None:
        field_names = list(field_names)
    else:
        field_names = Dataset(dataset_path).user_field_names
    LOG.log(log_level, "Start: Copy dataset `%s` as `%s`.", dataset_path, output_path)
    _dataset = Dataset(dataset_path)
    view = DatasetView(
        dataset_path,
        field_names=field_names,
        dataset_where_sql=dataset_where_sql if not schema_only else "0 = 1",
    )
    with view:
        if overwrite and Exists(output_path):
            delete_dataset(output_path, log_level=DEBUG)
        if _dataset.is_spatial:
            # ArcPy2.8.0: Convert to str.
            FeatureClassToFeatureClass(
                in_features=view.name,
                out_path=str(output_path.parent),
                out_name=output_path.name,
            )
        elif _dataset.is_table:
            # ArcPy2.8.0: Convert to str.
            TableToTable(
                in_rows=view.name,
                out_path=str(output_path.parent),
                out_name=output_path.name,
            )
        else:
            raise ValueError(f"`{dataset_path}` unsupported dataset type.")

    LOG.log(log_level, "End: Copy.")
    return Dataset(output_path)


def copy_dataset_features(
    dataset_path: Union[Path, str],
    *,
    field_names: Optional[Iterable[str]] = None,
    dataset_where_sql: Optional[str] = None,
    output_path: Union[Path, str],
    overwrite: bool = False,
    schema_only: bool = False,
    log_level: int = INFO,
) -> Dataset:
    """Copy dataset features into new dataset.

    Args:
        dataset_path: Path to dataset.
        output_path: Path to output dataset.
        field_names: Collection of field names to include in output. If set to None, all
            fields will be included.
        dataset_where_sql: SQL where-clause property for dataset subselection.
        overwrite: Overwrite existing dataset at output path if True.
        schema_only: Copy only the schema--omitting data--if True.
        log_level: Level to log the function at.

    Returns:
        Dataset metadata instance for output dataset.

    Raises:
        ValueError: If dataset type not supported.
    """
    dataset_path = Path(dataset_path)
    output_path = Path(output_path)
    if field_names is not None:
        field_names = list(field_names)
    else:
        field_names = Dataset(dataset_path).user_field_names
    LOG.log(log_level, "Start: Copy dataset `%s` as `%s`.", dataset_path, output_path)
    _dataset = Dataset(dataset_path)
    view = DatasetView(
        dataset_path,
        field_names=field_names,
        dataset_where_sql=dataset_where_sql if not schema_only else "0 = 1",
    )
    with view:
        if overwrite and Exists(output_path):
            delete_dataset(output_path, log_level=DEBUG)
        if _dataset.is_spatial:
            # ArcPy2.8.0: Convert to str.
            CopyFeatures(in_features=view.name, out_feature_class=str(output_path))
        elif _dataset.is_table:
            # ArcPy2.8.0: Convert to str.
            CopyRows(in_rows=view.name, out_table=str(output_path))
        else:
            raise ValueError(f"`{dataset_path}` unsupported dataset type.")

    LOG.log(log_level, "End: Copy.")
    return Dataset(output_path)


def create_dataset(
    dataset_path: Union[Path, str],
    *,
    field_metadata_list: Optional[Iterable[Union[Field, dict]]] = None,
    geometry_type: Optional[str] = None,
    spatial_reference_item: SpatialReferenceSourceItem = 4326,
    log_level: int = INFO,
) -> Dataset:
    """Create new dataset.

    Args:
        dataset_path: Path to dataset.
        field_metadata_list: Collection of field metadata instances or mappings.
        geometry_type: Type of geometry, if a spatial dataset. Will create a nonspatial
            dataset if set to None.
        spatial_reference_item: Item from which the spatial reference of the output
            geometry will be derived. Default is 4326 (EPSG code for unprojected WGS84).
        log_level: Level to log the function at.

    Returns:
        Dataset metadata instance for created dataset.
    """
    dataset_path = Path(dataset_path)
    LOG.log(log_level, "Start: Create dataset `%s`.", dataset_path)
    if geometry_type:
        if spatial_reference_item is None:
            spatial_reference_item = 4326
        # ArcPy2.8.0: Convert Path to str.
        CreateFeatureclass(
            out_path=str(dataset_path.parent),
            out_name=dataset_path.name,
            geometry_type=geometry_type,
            has_z="DISABLED",
            spatial_reference=SpatialReference(spatial_reference_item).object,
        )
    else:
        # ArcPy2.8.0: Convert Path to str.
        CreateTable(out_path=str(dataset_path.parent), out_name=dataset_path.name)
    if field_metadata_list:
        for field_metadata in field_metadata_list:
            if isinstance(field_metadata, Field):
                field_metadata = field_metadata.field_as_dict
            # ArcPy2.8.0: Convert to str.
            AddField(
                in_table=str(dataset_path),
                field_name=field_metadata["name"],
                field_type=field_metadata["type"],
                field_precision=field_metadata.get("precision"),
                field_scale=field_metadata.get("scale"),
                field_length=field_metadata.get("length", 64),
                field_alias=field_metadata.get("alias"),
                field_is_nullable=field_metadata.get("is_nullable", True),
                field_is_required=field_metadata.get("is_required", False),
            )
    LOG.log(log_level, "End: Create.")
    return Dataset(dataset_path)


def dataset_as_feature_set(
    dataset_path: Union[Path, str],
    *,
    field_names: Optional[Iterable[str]] = None,
    dataset_where_sql: Optional[str] = None,
    force_record_set: bool = False,
) -> Union[FeatureSet, RecordSet]:
    """Return dataset as feature set.

    Args:
        dataset_path: Path to dataset.
        field_names: Collection of field names to include in output. If set to None, all
            fields will be included.
        dataset_where_sql: SQL where-clause property for dataset subselection.
        force_record_set: If True, return record set. If False, return feature set if
            spatial dataset & record set if non-spatial.
    """
    dataset_path = Path(dataset_path)
    if field_names is not None:
        field_names = list(field_names)
    view = DatasetView(
        dataset_path, field_names=field_names, dataset_where_sql=dataset_where_sql
    )
    with view:
        if force_record_set or not view.is_spatial:
            return RecordSet(table=view.name)

        return FeatureSet(table=view.name)


def dataset_feature_count(
    dataset_path: Union[Path, str], *, dataset_where_sql: Optional[str] = None
) -> int:
    """Return number of features in dataset.

    Args:
        dataset_path: Path to dataset.
        dataset_where_sql: SQL where-clause property for dataset subselection.
    """
    dataset_path = Path(dataset_path)
    view = DatasetView(dataset_path, dataset_where_sql=dataset_where_sql)
    with view:
        return view.feature_count


def delete_dataset(dataset_path: Union[Path, str], *, log_level: int = INFO) -> Dataset:
    """Delete dataset.

    Args:
        dataset_path: Path to dataset.
        log_level: Level to log the function at.

    Returns:
        Dataset metadata instance for now-deleted dataset.
    """
    dataset_path = Path(dataset_path)
    LOG.log(log_level, "Start: Delete dataset `%s`.", dataset_path)
    _dataset = Dataset(dataset_path)
    # ArcPy2.8.0: Convert to str.
    Delete(in_data=str(dataset_path))
    LOG.log(log_level, "End: Delete.")
    return _dataset


def is_valid_dataset(dataset_path: Union[Path, str]) -> bool:
    """Return True if dataset is extant & valid.

    Args:
        dataset_path: Path to dataset.
    """
    dataset_path = Path(dataset_path)
    exists = dataset_path and Exists(dataset=dataset_path)
    if exists:
        try:
            valid = Dataset(dataset_path).is_table
        except IOError:
            valid = False
    else:
        valid = False
    return valid


def remove_all_default_field_values(
    dataset_path: Union[Path, str], *, log_level: int = INFO
) -> Dataset:
    """Remove all default field values in dataset.

    Args:
        dataset_path: Path to dataset.
        log_level: Level to log the function at.

    Returns:
        Dataset metadata instance for dataset.
    """
    dataset_path = Path(dataset_path)
    LOG.log(
        log_level,
        "Start: Remove all default field values for dataset `%s`.",
        dataset_path,
    )
    subtype_codes = [
        code
        for code, _property in ListSubtypes(dataset_path).items()
        if _property["SubtypeField"]
    ]
    for field in Dataset(dataset_path).fields:
        if field.default_value is None:
            continue

        LOG.log(log_level, "Removing default value for field `%s`.", field.name)
        set_default_field_value(
            dataset_path,
            field_name=field.name,
            value=None,
            subtype_codes=subtype_codes,
            log_level=DEBUG,
        )
    LOG.log(log_level, "End: Remove.")
    # Make new Dataset instance to update the field information.
    return Dataset(dataset_path)


def set_default_field_value(
    dataset_path: Union[Path, str],
    *,
    field_name: str,
    value: Any = None,
    subtype_codes: Optional[Iterable[int]] = None,
    log_level: int = INFO,
) -> Field:
    """Set default value for field on dataset.

    Args:
        dataset_path: Path to dataset.
        field_name: Name of field.
        value: Default value to assign.
        subtype_codes: Codes for subtypes to participate in the default value.
        log_level: Level to log the function at.

    Returns:
        Field metadata instance for field.

    Raises:
        OSError: If dataset in workspace that does not support changing field defaults.
    """
    dataset_path = Path(dataset_path)
    LOG.log(
        log_level,
        "Start: Set default value for field `%s` on dataset `%s` to `%s`.",
        field_name,
        dataset_path,
        value,
    )
    if dataset_path.parent.name == "in_memory":
        raise OSError("Cannot change field default in `in_memory` workspace")

    # ArcPy2.8.0: Convert Path to str.
    AssignDefaultToField(
        in_table=str(dataset_path),
        field_name=field_name,
        default_value=value if value is not None else "",
        subtype_code=list(subtype_codes) if subtype_codes else [],
        clear_value=value is None,
    )
    LOG.log(log_level, "End: Set.")
    return Field(dataset_path, field_name)


def unique_dataset_path(
    prefix: str = "",
    suffix: str = "",
    *,
    unique_length: int = 4,
    workspace_path: Union[Path, str] = Path("memory"),
) -> Path:
    """Return unique dataset path in the given workspace.

    Args:
        prefix: Prefix insert before the unique part of the name.
        suffix: Suffix to append after the unique part of the name.
        unique_length: Number of unique characters to include in the name.
        workspace_path: Path of workspace to create the dataset in.
    """
    workspace_path = Path(workspace_path) if workspace_path else Path("memory")
    name = unique_name(
        prefix, suffix, unique_length=unique_length, allow_initial_digit=False
    )
    while Exists(workspace_path / name):
        name = unique_name(
            prefix, suffix, unique_length=unique_length, allow_initial_digit=False
        )
    return workspace_path / name
