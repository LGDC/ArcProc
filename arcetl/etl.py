# -*- coding=utf-8 -*-
"""Library of etl objects & functions."""
import inspect
import logging

from . import operations
from . import properties
from .helpers import log_line, unique_temp_dataset_path


LOG = logging.getLogger(__name__)


class ArcETL(object):
    """Manages a single Arc-style ETL process."""

    def __init__(self):
        self.transform_path = None
        LOG.info("Initialized ArcETL instance.")

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.close()

    def close(self):
        """Clean up instance."""
        LOG.info("Closing ArcETL instance.")
        # Clear the transform dataset.
        if all([self.transform_path,
                properties.is_valid_dataset(self.transform_path)]):
            operations.delete_dataset(self.transform_path, log_level=None)
            self.transform_path = None
        LOG.info("Closed.")

    def extract(self, extract_path, extract_where_sql=None, schema_only=False):
        """Extract features to transform workspace."""
        _description = "Extract {}.".format(extract_path)
        log_line('start', _description)
        # Extract to a new dataset.
        self.transform_path = operations.copy_dataset(
            extract_path, unique_temp_dataset_path('extract'),
            extract_where_sql, schema_only, log_level=None)
        log_line('end', _description)
        return self.transform_path

    def load(self, load_path, load_where_sql=None, preserve_features=False):
        """Load features from transform workspace to the load-dataset."""
        _description = "Load {}.".format(load_path)
        log_line('start', _description)
        if properties.is_valid_dataset(load_path):
            # Load to an existing dataset.
            # Unless preserving features, initialize the target dataset.
            if not preserve_features:
                operations.delete_features(dataset_path=load_path,
                                           log_level=None)
            operations.insert_features_from_path(
                load_path, self.transform_path, load_where_sql, log_level=None)
        else:
            # Load to a new dataset.
            operations.copy_dataset(self.transform_path, load_path,
                                    load_where_sql, log_level=None)
        log_line('end', _description)
        return load_path

    def make_asssertion(self, assertion_name, **kwargs):
        """Check whether an assertion is valid or not."""
        raise NotImplementedError

    def transform(self, transform_name, **kwargs):
        """Run transform operation as defined in the workspace."""
        transform = getattr(operations, transform_name)
        # Unless otherwise stated, dataset path is self.transform path.
        if 'dataset_path' not in kwargs:
            kwargs['dataset_path'] = self.transform_path
        # Add output_path to kwargs if needed.
        if 'output_path' in inspect.getargspec(transform).args:
            kwargs['output_path'] = unique_temp_dataset_path(transform_name)
        result = transform(**kwargs)
        # If there's a new output, replace old transform.
        if 'output_path' in kwargs:
            if properties.is_valid_dataset(self.transform_path):
                operations.delete_dataset(self.transform_path, log_level=None)
            self.transform_path = result
        return result
