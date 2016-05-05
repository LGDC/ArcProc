# -*- coding=utf-8 -*-
"""ETL objects."""
import inspect
import logging

from . import arcwrap, features, helpers, metadata, temp_ops


LOG = logging.getLogger(__name__)


class ArcETL(object):
    """Manages a single Arc-style ETL process."""

    def __init__(self, name=None):
        self.name = name if name else 'unnamed ETL'
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
        if all([self.transform_path,
                metadata.is_valid_dataset(self.transform_path)]):
            arcwrap.delete_dataset(self.transform_path)
            self.transform_path = None
        LOG.info("Closed.")

    def extract(self, extract_path, extract_where_sql=None, schema_only=False):
        """Extract features to transform workspace."""
        _description = "Extract {}.".format(extract_path)
        helpers.log_line('start', _description)
        # Remove previously-extant transform dataset.
        if all([self.transform_path,
                metadata.is_valid_dataset(self.transform_path)]):
            arcwrap.delete_dataset(self.transform_path)
        # Extract to a new dataset.
        self.transform_path = arcwrap.copy_dataset(
            dataset_path=extract_path,
            output_path=helpers.unique_temp_dataset_path('extract'),
            dataset_where_sql=extract_where_sql, schema_only=schema_only)
        helpers.log_line('end', _description)
        return self.transform_path

    def load(self, load_path, load_where_sql=None, preserve_features=False):
        """Load features from transform workspace to the load-dataset."""
        _description = "Load {}.".format(load_path)
        helpers.log_line('start', _description)
        if metadata.is_valid_dataset(load_path):
            # Load to an existing dataset.
            # Unless preserving features, initialize the target dataset.
            if not preserve_features:
                features.delete_features(load_path, log_level=None)
            features.insert_features_from_path(
                dataset_path=load_path,
                insert_dataset_path=self.transform_path,
                insert_where_sql=load_where_sql, log_level=None)
        else:
            # Load to a new dataset.
            arcwrap.copy_dataset(
                dataset_path=self.transform_path, output_path=load_path,
                dataset_where_sql=load_where_sql)
        helpers.log_line('end', _description)
        return load_path

    def make_asssertion(self, assertion_name, **kwargs):
        """Check whether an assertion is valid or not."""
        raise NotImplementedError

    def transform(self, transform_name, **kwargs):
        """Run transform operation as defined in the workspace."""
        transform = getattr(temp_ops, transform_name)
        # Unless otherwise stated, dataset path is self.transform path.
        if 'dataset_path' not in kwargs:
            kwargs['dataset_path'] = self.transform_path
        # Add output_path to kwargs if needed.
        if 'output_path' in inspect.getargspec(transform).args:
            kwargs['output_path'] = (
                helpers.unique_temp_dataset_path(transform_name))
        result = transform(**kwargs)
        # If there's a new output, replace old transform.
        if 'output_path' in kwargs:
            if metadata.is_valid_dataset(self.transform_path):
                arcwrap.delete_dataset(self.transform_path)
            self.transform_path = result
        return result
