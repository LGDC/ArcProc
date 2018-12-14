"""ETL objects."""
from collections import Counter
import logging

import funcsigs

from arcetl import dataset
from arcetl import features
from arcetl.helpers import unique_path


LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""


class ArcETL(object):
    """Manages a single Arc-style ETL process.

    Attributes:
        name (str): Name reference for ETL.
        transform_path (str): Path of the current transform dataset.
    """

    def __init__(self, name="Unnamed ETL"):
        """Initialize instance.

        Args:
            name (str): Name reference for ETL.
        """
        self.name = name
        self.transform_path = None
        LOG.info("Initialized ArcETL instance for %s.", self.name)

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.close()

    def close(self):
        """Clean up instance."""
        LOG.info("Closing ArcETL instance for %s.", self.name)
        # Clear the transform dataset.
        if self.transform_path and dataset.is_valid(self.transform_path):
            dataset.delete(self.transform_path, log_level=None)
            self.transform_path = None
        LOG.info("Closed.")

    def extract(self, dataset_path, extract_where_sql=None):
        """Extract features to transform workspace.

        Args:
            dataset_path (str): Path of the dataset to extract.
            extract_where_sql (str): SQL where-clause for extract subselection.

        Returns:
            str: Path of the transform-dataset with extracted features.
        """
        LOG.info("Start: Extract %s.", dataset_path)
        self.transform_path = dataset.copy(
            dataset_path=dataset_path,
            output_path=unique_path("extract"),
            dataset_where_sql=extract_where_sql,
            log_level=None,
        )
        LOG.info("End: Extract.")
        return self.transform_path

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
            geometry_type (str): NGeometry type. Valid types are: point, multipoint,
                polygon, polyline. If unstated or another value, dataset will be
                nonspatial. Will be ignored if template_path used.
            spatial_reference_item: Item from which the spatial reference of the output
                geometry will be derived. Default is 4326 (EPSG code for unprojected
                WGS84).  Will be ignored if template_path used.

        Returns:
            str: Path of the current transformation output dataset.
        """
        LOG.info("Start: Initialize schema.")
        self.transform_path = unique_path("init")
        if template_path:
            dataset.copy(
                dataset_path=template_path,
                output_path=self.transform_path,
                schema_only=True,
                log_level=None,
            )
        else:
            dataset.create(dataset_path=self.transform_path, log_level=None, **kwargs)
        LOG.info("End: Initialize.")
        return self.transform_path

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
            collections.Counter: Counts for each update type.
        """
        kwargs.setdefault("use_edit_session", False)
        LOG.info("Start: Load %s.", dataset_path)
        feature_count = Counter()
        # Load to an existing dataset.
        if dataset.is_valid(dataset_path):
            if not preserve_features:
                feature_count.update(
                    features.delete(dataset_path, log_level=None, **kwargs)
                )
            feature_count.update(
                features.insert_from_path(
                    dataset_path,
                    insert_dataset_path=self.transform_path,
                    insert_where_sql=load_where_sql,
                    use_edit_session=kwargs["use_edit_session"],
                    log_level=None,
                )
            )
        # Load to a new dataset.
        else:
            dataset.copy(
                self.transform_path,
                output_path=dataset_path,
                dataset_where_sql=load_where_sql,
                log_level=None,
            )
            feature_count["inserted"] = dataset.feature_count(dataset_path)
        for key in ["deleted", "inserted"]:
            LOG.info("%s features %s.", feature_count[key], key)
        LOG.info("End: Load.")
        return feature_count

    def transform(self, transformation, **kwargs):
        """Run transform operation as defined in the workspace.

        Args:
            transformation: Function or method used to perform a transformation upon
                the transform-dataset.
            **kwargs: Arbitrary keyword arguments; passed through to the transformation.

        Returns:
            Result object of the transformation.
        """
        # Unless otherwise stated, dataset path is self.transform_path.
        kwargs.setdefault("dataset_path", self.transform_path)
        # Add output_path to kwargs if needed.
        if "output_path" in funcsigs.signature(transformation).parameters:
            kwargs.setdefault(
                "output_path",
                unique_path(getattr(transformation, "__name__", "transform")),
            )
        result = transformation(**kwargs)
        # If there"s a new output, replace old transform.
        if "output_path" in funcsigs.signature(transformation).parameters:
            if dataset.is_valid(self.transform_path):
                dataset.delete(self.transform_path, log_level=None)
            self.transform_path = kwargs["output_path"]
        return result

    def update(self, dataset_path, id_field_names, field_names=None, **kwargs):
        """Update features from transform- to load-dataset.

        Args:
            dataset_path (str): Path of the dataset.
            id_field_names (iter): Names of the ID field.
            field_names (iter): Field names to check & update. Listed fields must be
                present in both datasets. If field_names is None, all fields will be
                checked & updated.
            **kwargs: Arbitrary keyword arguments. See below.

        Keyword Args:
            update_where_sql (str): SQL where-clause for update-dataset subselection.
            delete_missing_features (bool): Update should delete features not present in
                update_features if True; keep them if False. Default is True.
            use_edit_session (bool): Updates are done in an edit session if True.
                Default is True.

        Returns:
            collections.Counter: Counts for each update type.
        """
        kwargs.setdefault("update_where_sql")
        kwargs.setdefault("delete_missing_features", True)
        kwargs.setdefault("use_edit_session", True)
        LOG.info("Start: Update %s.", dataset_path)
        feature_count = features.update_from_path(
            dataset_path,
            update_dataset_path=self.transform_path,
            id_field_names=id_field_names,
            field_names=field_names,
            log_level=None,
            **kwargs
        )
        for key in features.UPDATE_TYPES:
            LOG.info("%s features %s.", feature_count[key], key)
        LOG.info("End: Update.")
        return feature_count
