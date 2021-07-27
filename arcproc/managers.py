"""Process manager objects."""
from collections import Counter

try:
    from contextlib import ContextDecorator
except ImportError:
    # Py2.
    from contextlib2 import ContextDecorator
import datetime
import logging

import funcsigs

from arcproc import dataset
from arcproc import features
from arcproc.helpers import elapsed, log_entity_states, slugify, unique_path
import arcpy


LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""


class Procedure(ContextDecorator):
    """Manages a single Arc-style procedure.

    Attributes:
        name (str): Procedure name.
        slug (str): Slugified version of procedure name.
        start_time (datetime.datetime): Date & time procedure started.
        transform_path (str): Path of the current transform dataset.
    """

    def __init__(self, name="Unnamed", workspace_path="memory", keep_transforms=False):
        """Initialize instance.

        Args:
            name (str): Procedure name.
            workspace_path (str):  Path for the transformation workspace.
            keep_transforms (bool): Keep the transformation outputs if True.
        """
        self.start_time = datetime.datetime.now()
        self.name = name
        self.slug = slugify(name, separator="_")
        self.keep_transforms = keep_transforms
        self.transform_path = None
        self.workspace_path = workspace_path
        # ArcGIS Desktop does not implement the "memory" workspace.
        if (
            arcpy.GetInstallInfo()["ProductName"] == "Desktop"
            and self.workspace_path == "memory"  # noqa: W503
        ):
            self.workspace_path = "in_memory"
        LOG.info("""Starting procedure for "%s".""", self.name)

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.close()

    @property
    def available_transform_path(self):
        """str: A path in the transformation workspace available for use."""
        path = unique_path(prefix=self.slug + "_", workspace_path=self.workspace_path)
        while dataset.is_valid(path):
            path = self.available_transform_path
        return path

    def close(self):
        """Clean up instance."""
        LOG.info("""Ending procedure for "%s".""", self.name)
        if (
            not self.keep_transforms
            and self.transform_path  # noqa: W503
            and dataset.is_valid(self.transform_path)  # noqa: W503
        ):
            dataset.delete(self.transform_path, log_level=logging.DEBUG)
            self.transform_path = None
        elapsed(self.start_time, LOG)
        LOG.info("Ended.")

    def extract(self, dataset_path, extract_where_sql=None, **kwargs):
        """Extract features to transform workspace.

        Args:
            dataset_path (str): Path of the dataset to extract.
            extract_where_sql (str): SQL where-clause for extract subselection.

        Keyword Args:
            field_names (iter): Collection of field names to include in output.

        Returns:
            arcproc.managers.Procedure: Reference to the instance.
        """
        LOG.info("Start: Extract `%s`.", dataset_path)
        self.transform_path = self.available_transform_path
        states = dataset.copy(
            dataset_path=dataset_path,
            output_path=self.transform_path,
            dataset_where_sql=extract_where_sql,
            field_names=kwargs.get("field_names"),
            log_level=logging.DEBUG,
        )
        # Workaround for BUG-000091314. Only affect Pro-ArcPy, not Desktop.
        if arcpy.GetInstallInfo()["ProductName"] == "ArcGISPro":
            dataset.remove_all_default_field_values(
                dataset_path=self.transform_path, log_level=logging.DEBUG
            )
        log_entity_states("features", states, LOG)
        LOG.info("End: Extract.")
        return self

    def init_schema(self, template_path=None, **kwargs):
        """Initialize dataset schema. Use only when extracting dataset.

        Keyword arguments that describe the schema will only be referenced
        if template_path is undefined.

        Args:
            template_path (str): Path of the dataset to use as schema
                template.
            **kwargs: Arbitrary keyword arguments. See below.

        Keyword Args:
            field_metadata_list (iter): Field metadata mappings. Will be ignored if
                template_path used.
            geometry_type (str): Geometry type. Valid types are: point, multipoint,
                polygon, polyline. If unstated or another value, dataset will be
                nonspatial. Will be ignored if template_path used.
            spatial_reference_item: Item from which the spatial reference of the output
                geometry will be derived. Default is 4326 (EPSG code for unprojected
                WGS84).  Will be ignored if template_path used.

        Returns:
            arcproc.managers.Procedure: Reference to the instance.
        """
        LOG.info("Start: Initialize schema.")
        self.transform_path = self.available_transform_path
        if template_path:
            dataset.copy(
                dataset_path=template_path,
                output_path=self.transform_path,
                schema_only=True,
                log_level=logging.DEBUG,
            )
            # Workaround for BUG-000091314. Only affect Pro-ArcPy, not Desktop.
            if arcpy.GetInstallInfo()["ProductName"] == "ArcGISPro":
                dataset.remove_all_default_field_values(
                    dataset_path=self.transform_path, log_level=logging.DEBUG
                )
        else:
            dataset.create(
                dataset_path=self.transform_path, log_level=logging.DEBUG, **kwargs
            )
        LOG.info("End: Initialize.")
        return self

    def load(
        self, dataset_path, load_where_sql=None, preserve_features=False, **kwargs
    ):
        """Load features from transform- to load-dataset.

        Args:
            dataset_path (str): Path of dataset to load.
            load_where_sql (str): SQL where-clause for subselection from the
                transform-dataset.
            preserve_features (bool): Keep current features in load-dataset if True;
                remove them before adding transform-features if False.

        Keyword Args:
            use_edit_session (bool): Updates are done in an edit session if True.
                Default is False.

        Returns:
            arcproc.managers.Procedure: Reference to the instance.
        """
        LOG.info("Start: Load `%s`.", dataset_path)
        # Load to an existing dataset.
        if dataset.is_valid(dataset_path):
            states = Counter()
            if not preserve_features:
                states.update(
                    features.delete(dataset_path, log_level=logging.DEBUG, **kwargs)
                )
            states.update(
                features.insert_from_path(
                    dataset_path,
                    insert_dataset_path=self.transform_path,
                    insert_where_sql=load_where_sql,
                    log_level=logging.DEBUG,
                    **kwargs
                )
            )
        # Load to a new dataset.
        else:
            states = dataset.copy(
                self.transform_path,
                output_path=dataset_path,
                dataset_where_sql=load_where_sql,
                log_level=logging.DEBUG,
            )
        log_entity_states("features", states, LOG)
        LOG.info("End: Load.")
        return self

    def transform(self, transformation, **kwargs):
        """Run transform operation as defined in the workspace.

        Args:
            transformation: Function or method used to perform a transformation upon
                the transform-dataset.
            **kwargs: Arbitrary keyword arguments; passed through to the transformation.

        Returns:
            arcproc.managers.Procedure: Reference to the instance.
        """
        # Unless otherwise stated, dataset path is self.transform_path.
        kwargs.setdefault("dataset_path", self.transform_path)
        # Add output_path to kwargs if needed.
        if "output_path" in funcsigs.signature(transformation).parameters:
            kwargs.setdefault("output_path", self.available_transform_path)
        transformation(**kwargs)
        # If there"s a new output, replace old transform.
        if "output_path" in funcsigs.signature(transformation).parameters:
            if not self.keep_transforms and dataset.is_valid(self.transform_path):
                dataset.delete(self.transform_path, log_level=logging.DEBUG)
            self.transform_path = kwargs["output_path"]
        return self

    def update(self, dataset_path, id_field_names, field_names=None, **kwargs):
        """Update features from transform- to load-dataset.

        Args:
            dataset_path (str): Path of the dataset.
            id_field_names (iter): Names of the ID field.
            field_names (iter): Field names to check & update. Listed fields must be
                present in both datasets. If field_names is None, all fields will be
                checked & updated.
            **kwargs: Arbitrary keyword arguments. Refer to those listed for
                arcproc.features.update_from_path.

        Returns:
            arcproc.managers.Procedure: Reference to the instance.
        """
        LOG.info("Start: Update `%s`.", dataset_path)
        states = features.update_from_path(
            dataset_path,
            update_dataset_path=self.transform_path,
            id_field_names=id_field_names,
            field_names=field_names,
            log_level=logging.DEBUG,
            **kwargs
        )
        log_entity_states("features", states, LOG)
        LOG.info("End: Update.")
        return self
