# -*- coding=utf-8 -*-
"""Library of etl objects & functions."""
import collections
import csv
import datetime
import inspect
import logging
import uuid

import arcpy

from .helpers import (
    log_function, log_line, unique_ids, unique_name, unique_temp_dataset_path,
    )
from . import operations
from . import properties


LOG = logging.getLogger(__name__)


# Classes (ETL).

class ArcETL(object):
    """Manages a single Arc-style ETL process."""

    def __init__(self, workspace=None):
        self.workspace = workspace if workspace else ArcWorkspace()
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
        if all([self.transform_path
                and self.workspace.is_valid_dataset(self.transform_path)]):
            self.workspace.delete_dataset(self.transform_path, log_level=None)
            self.transform_path = None
        LOG.info("Closed.")

    def extract(self, extract_path, extract_where_sql=None, schema_only=False):
        """Extract features to transform workspace."""
        _description = "Extract {}.".format(extract_path)
        log_line('start', _description)
        # Extract to a new dataset.
        self.transform_path = self.workspace.copy_dataset(
            extract_path, unique_temp_dataset_path('extract'),
            extract_where_sql, schema_only, log_level=None)
        log_line('end', _description)
        return self.transform_path

    def load(self, load_path, load_where_sql=None, preserve_features=False):
        """Load features from transform workspace to the load-dataset."""
        _description = "Load {}.".format(load_path)
        log_line('start', _description)
        if self.workspace.is_valid_dataset(load_path):
            # Load to an existing dataset.
            # Unless preserving features, initialize the target dataset.
            if not preserve_features:
                self.workspace.delete_features(dataset_path=load_path,
                                               log_level=None)
            self.workspace.insert_features_from_path(
                load_path, self.transform_path, load_where_sql,
                log_level=None)
        else:
            # Load to a new dataset.
            self.workspace.copy_dataset(self.transform_path, load_path,
                                        load_where_sql, log_level=None)
        log_line('end', _description)
        return load_path

    def make_asssertion(self, assertion_name, **kwargs):
        """Check whether an assertion is valid or not."""
        raise NotImplementedError

    def transform(self, transform_name, **kwargs):
        """Run transform operation as defined in the workspace."""
        transform = getattr(self.workspace, transform_name)
        # Unless otherwise stated, dataset path is self.transform path.
        if 'dataset_path' not in kwargs:
            kwargs['dataset_path'] = self.transform_path
        # Add output_path to kwargs if needed.
        if 'output_path' in inspect.getargspec(transform).args:
            kwargs['output_path'] = unique_temp_dataset_path(transform_name)
        result = transform(**kwargs)
        # If there's a new output, replace old transform.
        if 'output_path' in kwargs:
            if self.workspace.is_valid_dataset(self.transform_path):
                self.workspace.delete_dataset(self.transform_path,
                                              log_level=None)
            self.transform_path = result
        return result


class ArcWorkspace(object):
    """Manages an Arc-style workspace with built-in operations."""

    def __init__(self, path=None):
        self.path = path if path else 'in_memory'
        # Set arcpy workspace for tools that require it.
        # Otherwise, avoid implied paths.
        arcpy.env.workspace = self.path
        LOG.info("Initialized ArcWorkspace instance.")
        # Replacement functions.
        # Operations - dataset.
        self.add_field = operations.add_field
        self.add_fields_from_metadata_list = (
            operations.add_fields_from_metadata_list)
        self.add_index = operations.add_index
        self.delete_field = operations.delete_field
        self.duplicate_field = operations.duplicate_field
        self.join_field = operations.join_field
        self.rename_field = operations.rename_field
        # Operations - feature.
        self.adjust_features_for_shapefile = (
            operations.adjust_features_for_shapefile)
        self.clip_features = operations.clip_features
        self.delete_features = operations.delete_features
        self.dissolve_features = operations.dissolve_features
        self.erase_features = operations.erase_features
        self.keep_features_by_location = operations.keep_features_by_location
        self.identity_features = operations.identity_features
        self.insert_features_from_iterables = (
            operations.insert_features_from_iterables)
        self.insert_features_from_path = operations.insert_features_from_path
        self.overlay_features = operations.overlay_features
        self.union_features = operations.union_features
        self.update_field_by_coded_value_domain = (
            operations.update_field_by_coded_value_domain)
        self.update_field_by_constructor_method = (
            operations.update_field_by_constructor_method)
        self.update_field_by_expression = (
            operations.update_field_by_expression)
        self.update_field_by_feature_matching = (
            operations.update_field_by_feature_matching)
        self.update_field_by_function = operations.update_field_by_function
        self.update_field_by_geometry = operations.update_field_by_geometry
        self.update_field_by_joined_value = (
            operations.update_field_by_joined_value)
        self.update_field_by_near_feature = (
            operations.update_field_by_near_feature)
        self.update_field_by_overlay = operations.update_field_by_overlay
        self.update_field_by_unique_id = operations.update_field_by_unique_id
        self.update_fields_by_geometry_node_ids = (
            operations.update_fields_by_geometry_node_ids)
        # Operations - products.
        self.convert_polygons_to_lines = operations.convert_polygons_to_lines
        self.convert_table_to_spatial_dataset = (
            operations.convert_table_to_spatial_dataset)
        self.generate_facility_service_rings = (
            operations.generate_facility_service_rings)
        self.planarize_features = operations.planarize_features
        self.project = operations.project
        self.write_rows_to_csvfile = operations.write_rows_to_csvfile
        # Operations - workspace.
        self.compress_geodatabase = operations.compress_geodatabase
        self.copy_dataset = operations.copy_dataset
        self.create_dataset = operations.create_dataset
        self.create_dataset_view = operations.create_dataset_view
        self.create_file_geodatabase = operations.create_file_geodatabase
        self.create_geodatabase_xml_backup = (
            operations.create_geodatabase_xml_backup)
        self.delete_dataset = operations.delete_dataset
        self.execute_sql_statement = operations.execute_sql_statement
        self.set_dataset_privileges = operations.set_dataset_privileges
        # Properties.
        self.dataset_metadata = properties.dataset_metadata
        self.feature_count = properties.feature_count
        self.field_metadata = properties.field_metadata
        self.field_values = properties.field_values
        self.is_valid_dataset = properties.is_valid_dataset
        self.oid_field_value = properties.oid_field_value
        self.oid_field_value_map = properties.oid_field_value_map
        self.oid_geometry = properties.oid_geometry
        self.oid_geometry_map = properties.oid_field_value_map
        self.workspace_dataset_names = properties.workspace_dataset_names
