# -*- coding=utf-8 -*-
"""ETL objects."""
import logging

import funcsigs

from arcetl import dataset, features
from . import helpers, metadata


LOG = logging.getLogger(__name__)


class ArcETL(object):
    """Manages a single Arc-style ETL process."""

    def __init__(self, name=None):
        self.name = name if name else 'Unnamed ETL'
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
            dataset.delete(self.transform_path, log_level=None)
            self.transform_path = None
        LOG.info("Closed.")

    def extract(self, extract_path, extract_where_sql=None, schema_only=False):
        """Extract features to transform workspace."""
        LOG.info("Start: Extract %s.", extract_path)
        self.transform_path = dataset.copy(
            dataset_path=extract_path,
            output_path=helpers.unique_temp_dataset_path('extract'),
            dataset_where_sql=extract_where_sql, schema_only=schema_only,
            log_level=None
            )
        LOG.info("End: Extract.")
        return self.transform_path

    def load(self, load_path, load_where_sql=None, preserve_features=False):
        """Load features from transform- to load-dataset."""
        LOG.info("Start: Load %s.", load_path)
        if metadata.is_valid_dataset(load_path):
            # Load to an existing dataset.
            # Unless preserving features, initialize target dataset.
            if not preserve_features:
                features.delete(load_path, log_level=None)
            features.insert_from_path(dataset_path=load_path,
                                      insert_dataset_path=self.transform_path,
                                      insert_where_sql=load_where_sql,
                                      log_level=None)
        else:
            # Load to a new dataset.
            dataset.copy(self.transform_path, load_path,
                         dataset_where_sql=load_where_sql, log_level=None)
        LOG.info("End: Load.")
        return load_path

    def make_asssertion(self, assertion_name, **kwargs):
        """Check whether an assertion is valid or not."""
        raise NotImplementedError

    def transform(self, transformation, **kwargs):
        """Run transform operation as defined in the workspace."""
        # Unless otherwise stated, dataset path is self.transform_path.
        if 'dataset_path' not in kwargs:
            kwargs['dataset_path'] = self.transform_path
        # Add output_path to kwargs if needed.
        if all([
                'output_path' in funcsigs.signature(transformation).parameters,
                'output_path' not in kwargs]):
            kwargs['output_path'] = (
                helpers.unique_temp_dataset_path(transformation.__name__))
        result = transformation(**kwargs)
        # If there's a new output, replace old transform.
        if 'output_path' in kwargs:
            if metadata.is_valid_dataset(self.transform_path):
                dataset.delete(self.transform_path, log_level=None)
            self.transform_path = result
        return result
