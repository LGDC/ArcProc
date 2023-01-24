"""Process manager objects."""
from collections import Counter
from contextlib import ContextDecorator
from datetime import datetime as _datetime
from inspect import signature
from logging import DEBUG, Logger, getLogger
from pathlib import Path
from types import FunctionType, MethodType, TracebackType
from typing import Any, Iterable, Optional, Type, TypeVar, Union

from arcpy import SetLogHistory

from arcproc.dataset import (
    copy_dataset_features,
    create_dataset,
    delete_dataset,
    is_valid_dataset,
    remove_all_default_field_values,
    unique_dataset_path,
)
from arcproc.features import (
    delete_features,
    insert_features_from_dataset,
    update_features_from_dataset,
)
from arcproc.metadata import Field, SpatialReferenceSourceItem
from arcproc.misc import log_entity_states, slugify, time_elapsed


LOG: Logger = getLogger(__name__)
"""Module-level logger."""

SetLogHistory(False)

# Py3.7: Can replace usage with `typing.Self` in Py3.11.
TProcedure = TypeVar("TProcedure", bound="Procedure")
"""Type variable to enable method return of self on Procedure."""


class Procedure(ContextDecorator):
    """Manager for a single Arc-style procedure."""

    keep_transforms: bool = False
    """Preserve transformation datasets if True."""
    name: str = "Unnamed Procedure"
    """Procedure name."""
    time_started: _datetime
    """Timestamp for when procedure started."""
    transform_path: Path = None
    """Path to current transformation dataset."""
    workspace_path: Path = "memory"
    """Path to workspace for transformation datasets."""

    def __init__(
        self,
        name: Optional[str] = None,
        *,
        workspace_path: Optional[Union[Path, str]] = None,
    ) -> None:
        """Initialize instance.

        Args:
            name: Procedure name.
            workspace_path: Path to workspace for transformation datasets.
        """
        self.time_started = _datetime.now()
        if name:
            self.name = name
        if workspace_path:
            self.workspace_path = Path(workspace_path)
        LOG.info("""Starting procedure for "%s".""", self.name)

    def __enter__(self) -> TProcedure:
        return self

    def __exit__(
        self,
        exception_type: Optional[Type[BaseException]],
        exception_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> bool:
        self.close()

    @property
    def available_transform_path(self) -> Path:
        """Path in transformation workspace available for use as dataset."""
        return unique_dataset_path(
            prefix=self.slug + "_", workspace_path=self.workspace_path
        )

    @property
    def slug(self) -> str:
        """Slugified version of procedure name."""
        return slugify(self.name, separator="_")

    def close(self) -> None:
        """Clean up instance."""
        LOG.info("""Ending procedure for "%s".""", self.name)
        if not self.keep_transforms:
            if self.transform_path and is_valid_dataset(self.transform_path):
                delete_dataset(self.transform_path, log_level=DEBUG)
                self.transform_path = None
        time_elapsed(self.time_started, logger=LOG)
        LOG.info("Ended.")

    def extract(
        self,
        dataset_path: Union[Path, str],
        *,
        field_names: Optional[Iterable[str]] = None,
        dataset_where_sql: Optional[str] = None,
    ) -> TProcedure:
        """Extract features to transform workspace.

        Args:
            dataset_path: Path to dataset.
            field_names: Names of fields to extract. If set to None, all fields will be
                included.
            dataset_where_sql: SQL where-clause for dataset subselection.

        Returns:
            Reference to instance.
        """
        dataset_path = Path(dataset_path)
        LOG.info("Start: Extract `%s`.", dataset_path)
        self.transform_path = self.available_transform_path
        states = Counter()
        states["extracted"] = copy_dataset_features(
            dataset_path,
            field_names=field_names,
            dataset_where_sql=dataset_where_sql,
            output_path=self.transform_path,
            log_level=DEBUG,
        ).feature_count
        # ArcPy 2.8.0: Workaround for BUG-000091314.
        remove_all_default_field_values(self.transform_path, log_level=DEBUG)
        log_entity_states("features", states, logger=LOG)
        LOG.info("End: Extract.")
        return self

    def initialize_schema(
        self,
        template_path: Optional[Union[Path, str]] = None,
        *,
        field_metadata_list: Optional[Iterable[Union[Field, dict]]] = None,
        geometry_type: Optional[str] = None,
        spatial_reference_item: SpatialReferenceSourceItem = 4326,
    ) -> TProcedure:
        """Initialize dataset schema in transform workspace.

        Keyword arguments that describe the schema will only be referenced
        if template_path is undefined.

        Args:
            template_path: Path to dataset for use as schema template.
            field_metadata_list: Sequence of field metadata. Will be ignored if
                template_path set.
            geometry_type: Type of geometry. Will create a nonspatial dataset if set to
                None. Will be ignored if template_path set.
            spatial_reference_item: Item from which the spatial reference of the
                transform geometry will be derived. Default is 4326 (EPSG code for
                unprojected WGS84). Will be ignored if template_path set.

        Returns:
            Reference to instance.
        """
        LOG.info("Start: Initialize schema.")
        self.transform_path = self.available_transform_path
        if template_path:
            template_path = Path(template_path)
            copy_dataset_features(
                template_path,
                output_path=self.transform_path,
                schema_only=True,
                log_level=DEBUG,
            )
            # ArcPy 2.8.0: Workaround for BUG-000091314.
            remove_all_default_field_values(self.transform_path, log_level=DEBUG)
        else:
            create_dataset(
                self.transform_path,
                field_metadata_list=field_metadata_list,
                geometry_type=geometry_type,
                spatial_reference_item=spatial_reference_item,
                log_level=DEBUG,
            )
        LOG.info("End: Initialize.")
        return self

    def load(
        self,
        dataset_path: Union[Path, str],
        *,
        preserve_features: bool = False,
        use_edit_session: bool = False,
    ) -> TProcedure:
        """Load features from transform-dataset to load-dataset.

        Args:
            dataset_path: Path to dataset.
            preserve_features: Keep current features in load-dataset if True;
                remove them before adding transform-features if False.
            use_edit_session: True if edits are to be made in an edit session.

        Returns:
            Reference to instance.
        """
        dataset_path = Path(dataset_path)
        LOG.info("Start: Load `%s`.", dataset_path)
        states = Counter()
        # Load to an existing dataset.
        if is_valid_dataset(dataset_path):
            if not preserve_features:
                states["deleted"] = delete_features(
                    dataset_path,
                    use_edit_session=use_edit_session,
                    log_level=DEBUG,
                )["deleted"]
            states["inserted"] = insert_features_from_dataset(
                dataset_path,
                source_path=self.transform_path,
                use_edit_session=use_edit_session,
                log_level=DEBUG,
            )["inserted"]
        # Load to a new dataset.
        else:
            states["copied"] = copy_dataset_features(
                self.transform_path, output_path=dataset_path, log_level=DEBUG
            ).feature_count
        log_entity_states("features", states, logger=LOG)
        LOG.info("End: Load.")
        return self

    def transform(
        self, transformation: Union[FunctionType, MethodType], **kwargs: Any
    ) -> Any:
        """Run transform operation as defined in the workspace.

        Args:
            transformation: Function or method used to perform a transformation upon the
                current transform-dataset.
            **kwargs: Arbitrary keyword arguments; passed through to the transformation.

        Returns:
            Return value of the transformation.
        """
        parameters = signature(transformation).parameters
        # Unless otherwise stated, dataset path is self.transform_path.
        if "dataset_path" in parameters:
            kwargs.setdefault("dataset_path", self.transform_path)
        # Add output_path to kwargs if needed.
        if "output_path" in parameters:
            output_path = kwargs.setdefault(
                "output_path", self.available_transform_path
            )
        else:
            output_path = self.transform_path
        result = transformation(**kwargs)
        if output_path != self.transform_path:
            if not self.keep_transforms and is_valid_dataset(self.transform_path):
                delete_dataset(self.transform_path, log_level=DEBUG)
            self.transform_path = output_path
        return result

    def update(
        self,
        dataset_path: Union[Path, str],
        *,
        field_names: Optional[Iterable[str]] = None,
        id_field_names: Iterable[str],
        delete_missing_features: bool = True,
        use_edit_session: bool = False,
    ) -> TProcedure:
        """Update features from transform- to load-dataset.

        Args:
            dataset_path: Path to dataset.
            field_names: Names of fields for update. Fields must exist in both datasets.
                If set to None, all user fields present in both datasets will be
                updated, along with the geometry field (if present).
            id_field_names: Names of the feature ID fields. Fields must exist in both
                datasets.
            delete_missing_features: True if update should delete features missing from
                source dataset.
            use_edit_session: True if edits are to be made in an edit session.

        Returns:
            Reference to instance.
        """
        dataset_path = Path(dataset_path)
        LOG.info("Start: Update `%s`.", dataset_path)
        states = update_features_from_dataset(
            dataset_path,
            field_names=field_names,
            id_field_names=id_field_names,
            source_path=self.transform_path,
            delete_missing_features=delete_missing_features,
            use_edit_session=use_edit_session,
            log_level=DEBUG,
        )
        log_entity_states("features", states, logger=LOG)
        LOG.info("End: Update.")
        return self
