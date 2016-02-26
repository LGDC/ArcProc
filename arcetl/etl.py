# -*- coding=utf-8 -*-
"""Library of etl objects & functions.

This module file alone is a ready-to-go template for an ArcGIS Python toolbox.
"""
import collections
import csv
import datetime
import functools
import inspect
import logging
import os
import uuid

arcpy = None  # Lazy import.


logger = logging.getLogger(__name__)


# Decorators.

def log_function(function):
    """Decorator to log details of an function or method when called."""
    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        logger.debug(
            "@log_function - {}(*{}, **{})".format(function, args, kwargs))
        return function(*args, **kwargs)
    return wrapper


# Classes (ETL).

class ArcETL(object):
    """Manages a single Arc-style ETL process."""

    def __init__(self, workspace=None):
        self.workspace = workspace if workspace else ArcWorkspace()
        self.transform_path = None
        logger.info("Initialized ArcETL instance.")

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.close()

    def close(self):
        """Clean up instance."""
        # Clear the transform dataset.
        if (self.transform_path
            and self.workspace.is_valid_dataset(self.transform_path)):
            self.workspace.delete_dataset(self.transform_path,
                                          log_level = None)
            self.transform_path = None
        logger.info("Closed ArcETL instance.")

    def extract(self, extract_path, extract_where_sql=None, schema_only=False):
        """Extract features to transform workspace."""
        logline = "Extract {}.".format(extract_path)
        log_line('start', logline)
        # Extract to a new dataset.
        self.transform_path = self.workspace.copy_dataset(
            extract_path, unique_temp_dataset_path('extract'),
            extract_where_sql, schema_only, log_level = None)
        log_line('end', logline)
        return self.transform_path

    def load(self, load_path, load_where_sql=None, preserve_features=False):
        """Load features from transform workspace to the load-dataset."""
        logline = "Load {}.".format(load_path)
        log_line('start', logline)
        if self.workspace.is_valid_dataset(load_path):
            # Load to an existing dataset.
            # Unless preserving features, initialize the target dataset.
            if not preserve_features:
                self.workspace.delete_features(
                    dataset_path = load_path, log_level = None)
            self.workspace.insert_features_from_path(
                load_path, self.transform_path, load_where_sql,
                log_level = None)
        else:
            # Load to a new dataset.
            self.workspace.copy_dataset(self.transform_path, load_path,
                                        load_where_sql, log_level = None)
        log_line('end', logline)
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
        # If arguments include output_path, supersede old transform path.
        if 'output_path' in inspect.getargspec(transform).args:
            kwargs['output_path'] = unique_temp_dataset_path(transform_name)
        result = transform(**kwargs)
        if 'output_path' in kwargs:
            # Remove old transform_path (if extant).
            if self.workspace.is_valid_dataset(self.transform_path):
                self.workspace.delete_dataset(self.transform_path,
                                              log_level = None)
            # Replace with output_path.
            self.transform_path = result
        return result


class ArcWorkspace(object):
    """Manages an Arc-style workspace with built-in operations."""

    def __init__(self, path=None):
        global arcpy
        if not arcpy:
            import arcpy
        self.path = path if path else 'in_memory'
        # Set arcpy workspace for tools that require it.
        # Otherwise, avoid implied paths.
        arcpy.env.workspace = self.path
        logger.info("Initialize ArcWorkspace instance.")

    # General execution methods.

    @log_function
    def execute_sql_statement(self, statement, path_to_database=None,
                              log_level='info'):
        """Runs a SQL statement via SDE's SQL execution interface.

        This only works if path resolves to an actual SQL database.
        """
        logline = "Execute SQL statement."
        log_line('start', logline, log_level)
        conn = arcpy.ArcSDESQLExecute(
            path_to_database if path_to_database else self.path)
        try:
            result = conn.execute(statement)
        except AttributeError:
            logger.exception("Incorrect SQL syntax.")
            raise
        del conn  # Yeah, what can you do?
        log_line('end', logline, log_level)
        return result

    # Metadata/property methods.

    @log_function
    def dataset_metadata(self, dataset_path):
        """Return dictionary of dataset's metadata."""
        metadata = {}
        arc_description = arcpy.Describe(dataset_path)
        metadata['name'] = arc_description.name
        metadata['path'] = arc_description.catalogPath
        metadata['data_type'] = arc_description.dataType
        metadata['workspace_path'] = arc_description.path
        metadata['is_table'] = hasattr(arc_description, 'hasOID')
        if metadata['is_table']:
            if hasattr(arc_description, 'OIDFieldName'):
                metadata['oid_field_name'] = arc_description.OIDFieldName
            metadata['field_names'], metadata['fields'] = [], []
            for field in arc_description.fields:
                metadata['field_names'].append(field.name)
                metadata['fields'].append(
                    {'name': field.name,
                     'alias_name': field.aliasName,
                     'base_name': field.baseName,
                     'type': field.type.lower(),
                     'length': field.length,
                     'precision': field.precision,
                     'scale': field.scale,})
                # Leaving out certain field properties which aren't
                # necessary for ETL and are often problematic.
                #'default_value': field.defaultValue,
                #'is_required': field.required,
                #'is_editable': field.editable,
                #'is_nullable': field.isNullable,
        metadata['is_spatial'] = hasattr(arc_description, 'shapeType')
        if metadata['is_spatial']:
            metadata['geometry_type'] = arc_description.shapeType.lower()
            metadata['spatial_reference_id'] = (
                arc_description.spatialReference.factoryCode)
            metadata['geometry_field_name'] = (
                arc_description.shapeFieldName.lower())
        return metadata

    @log_function
    def feature_count(self, dataset_path, dataset_where_sql=None):
        """Return the number of features in a dataset."""
        with arcpy.da.SearchCursor(
            in_table = dataset_path, field_names = ['oid@'],
            where_clause = dataset_where_sql) as cursor:
            return len([None for row in cursor])

    @log_function
    def field_metadata(self, dataset_path, field_name):
        """Return dictionary of field's info."""
        try:
            return next(
                field for field
                in self.dataset_metadata(dataset_path)['fields']
                # Case-insensitive.
                if field['name'].lower() == field_name.lower()
                )
        except StopIteration:
            raise AttributeError(
                "Field {} not present on {}".format(field_name, dataset_path)
                )

    @log_function
    def is_valid_dataset(self, dataset_path):
        """Check whether a dataset exists/is valid."""
        if dataset_path and arcpy.Exists(dataset_path):
            return self.dataset_metadata(dataset_path)['is_table']
        else:
            return False

    @log_function
    def workspace_dataset_names(self, workspace_path=None, wildcard=None,
                                include_feature_classes=True,
                                include_rasters=True, include_tables=True,
                                include_feature_datasets=True):
        """Return list of names of workspace's datasets.

        wildcard requires an * to indicate where open; case insensitive.
        """
        if workspace_path and workspace_path != self.path:
            arcpy.env.workspace = workspace_path
        dataset_names = []
        if include_feature_classes:
            # None-value represents the root level.
            feature_dataset_names = [None]
            if include_feature_datasets:
                feature_dataset_names += arcpy.ListDatasets()
            for feature_dataset_name in feature_dataset_names:
                dataset_names += arcpy.ListFeatureClasses(
                    wildcard, feature_dataset = feature_dataset_name
                    )
        if include_rasters:
            dataset_names += arcpy.ListRasters(wildcard)
        if include_tables:
            dataset_names += arcpy.ListTables(wildcard)
        if workspace_path and workspace_path != self.path:
            arcpy.env.workspace = self.path
        return dataset_names

    # Workspace management methods.

    @log_function
    def compress_geodatabase(self, geodatabase_path=None,
                             disconnect_users=False, log_level='info'):
        """Compress geodatabase."""
        logline = "Compress {}.".format(geodatabase_path)
        log_line('start', logline, log_level)
        if not geodatabase_path:
            geodatabase_path = self.path
        arc_description = arcpy.Describe(geodatabase_path)
        if not hasattr(arc_description, 'workspaceType'):
            raise ValueError(
                "{} not a valid geodatabase.".format(geodatabase_path))
        if arc_description.workspaceType == 'LocalDatabase':
            _compress = arcpy.management.CompressFileGeodatabaseData
            _compress_kwargs = {'in_data': geodatabase_path}
            # Local databases cannot disconnect users (connections managed
            # by file access).
            disconnect_users = False
        elif arc_description.workspaceType == 'RemoteDatabase':
            _compress = arcpy.management.Compress
            _compress_kwargs = {'in_workspace': geodatabase_path}
        else:
            raise ValueError("{} not a valid geodatabase type.".format(
                arc_description.workspaceType))
        if disconnect_users:
            arcpy.AcceptConnections(sde_workspace = geodatabase_path,
                                    accept_connections = False)
            arcpy.DisconnectUser(sde_workspace = geodatabase_path,
                                 users = 'all')
        try:
            _compress(**_compress_kwargs)
        except arcpy.ExecuteError:
            logger.exception("ArcPy execution.")
            raise
        if disconnect_users:
            arcpy.AcceptConnections(sde_workspace = geodatabase_path,
                                    accept_connections = True)
        log_line('end', logline, log_level)
        return geodatabase_path

    @log_function
    def copy_dataset(self, dataset_path, output_path, dataset_where_sql=None,
                     schema_only=False, overwrite=False, log_level='info'):
        """Copy dataset."""
        logline = "Copy {} to {}.".format(dataset_path, output_path)
        log_line('start', logline, log_level)
        dataset_metadata = self.dataset_metadata(dataset_path)
        dataset_view_name = self.create_dataset_view(
            unique_name('dataset_view'), dataset_path,
            dataset_where_sql, log_level = None)

        if dataset_metadata['is_spatial']:
            _copy = arcpy.management.CopyFeatures
            _copy_kwargs = {'in_features': dataset_view_name,
                            'out_feature_class': output_path}
        elif  dataset_metadata['is_table']:
            _copy = arcpy.management.CopyRows
            _copy_kwargs = {'in_rows': dataset_view_name,
                            'out_table': output_path}
        else:
            raise ValueError(
                "{} unsupported dataset type.".format(dataset_path))
        if overwrite and self.is_valid_dataset(output_path):
            self.delete_dataset(output_path, log_level = None)
        try:
            _copy(**_copy_kwargs)
        except arcpy.ExecuteError:
            logger.exception("ArcPy execution.")
            raise
        self.delete_dataset(dataset_view_name, log_level = None)
        log_line('end', logline, log_level)
        return output_path

    @log_function
    def create_dataset(self, dataset_path, field_metadata=[],
                       geometry_type=None, spatial_reference_id=None,
                       log_level='info'):
        """Create new dataset."""
        logline = "Create dataset {}.".format(dataset_path)
        log_line('start', logline, log_level)
        _create_kwargs = {'out_path': os.path.dirname(dataset_path),
                          'out_name': os.path.basename(dataset_path)}
        if geometry_type:
            _create = arcpy.management.CreateFeatureclass
            _create_kwargs['geometry_type'] = geometry_type,
            # Default to EPSG 4326 (unprojected WGS 84).
            _create_kwargs['spatial_reference'] = arcpy.SpatialReference(
                spatial_reference_id if spatial_reference_id else 4326)
        else:
            _create = arcpy.management.CreateTable
        try:
            _create(**_create_kwargs)
        except arcpy.ExecuteError:
            logger.exception("ArcPy execution.")
            raise
        if field_metadata:
            if isinstance(field_metadata, dict):
                field_metadata = [field_metadata]
            for field in field_metadata:
                self.add_field(
                    dataset_path, field['name'], field['type'],
                    field.get('length'), field.get('precision'),
                    field.get('scale'), log_level = None)
        log_line('end', logline, log_level)
        return dataset_path

    @log_function
    def create_dataset_view(self, view_name, dataset_path,
                            dataset_where_sql=None, log_level='info'):
        """Create new feature layer/table view."""
        logline = "Create dataset view of {}.".format(dataset_path)
        log_line('start', logline, log_level)
        dataset_metadata = self.dataset_metadata(dataset_path)
        _create_kwargs = {'where_clause': dataset_where_sql,
                          'workspace': self.path}
        if dataset_metadata['is_spatial']:
            _create = arcpy.management.MakeFeatureLayer
            _create_kwargs['in_features'] = dataset_path
            _create_kwargs['out_layer'] = view_name
        elif dataset_metadata['is_table']:
            _create = arcpy.management.MakeTableView
            _create_kwargs['in_table'] = dataset_path
            _create_kwargs['out_view'] = view_name
        else:
            raise ValueError(
                "{} unsupported dataset type.".format(dataset_path))
        try:
            _create(**_create_kwargs)
        except arcpy.ExecuteError:
            logger.exception("ArcPy execution.")
            raise
        log_line('end', logline, log_level)
        return view_name

    @log_function
    def create_file_geodatabase(self, geodatabase_path,
                                xml_workspace_path=None,
                                include_xml_data=False, log_level='info'):
        """Create new file geodatabase."""
        logline = "Create file geodatabase at {}.".format(geodatabase_path)
        log_line('start', logline, log_level)
        if os.path.exists(geodatabase_path):
            logger.warning("Geodatabase already exists.")
            return geodatabase_path
        try:
            arcpy.management.CreateFileGDB(
                out_folder_path = os.path.dirname(geodatabase_path),
                out_name = os.path.basename(geodatabase_path),
                out_version = 'current')
        except arcpy.ExecuteError:
            logger.exception("ArcPy execution.")
            raise
        if xml_workspace_path:
            try:
                arcpy.management.ImportXMLWorkspaceDocument(
                    target_geodatabase = geodatabase_path,
                    in_file = xml_workspace_path,
                    import_type = ('data' if include_xml_data
                                   else 'schema_only'),
                    config_keyword = 'defaults')
            except arcpy.ExecuteError:
                logger.exception("ArcPy execution.")
                raise
        log_line('end', logline, log_level)
        return geodatabase_path

    @log_function
    def create_geodatabase_xml_backup(self, geodatabase_path, output_path,
                                      include_data=False,
                                      include_metadata=True, log_level='info'):
        """Create backup of geodatabase as XML workspace document."""
        logline = "Create backup for {} in {}.".format(geodatabase_path,
                                                       output_path)
        log_line('start', logline, log_level)
        try:
            arcpy.management.ExportXMLWorkspaceDocument(
                in_data = geodatabase_path, out_file = output_path,
                export_type = 'data' if include_data else 'schema_only',
                storage_type = 'binary', export_metadata = include_metadata)
        except arcpy.ExecuteError:
            logger.exception("ArcPy execution.")
            raise
        log_line('end', logline, log_level)
        return output_path

    @log_function
    def delete_dataset(self, dataset_path, log_level='info'):
        """Delete dataset."""
        logline = "Delete {}.".format(dataset_path)
        log_line('start', logline, log_level)
        try:
            arcpy.management.Delete(dataset_path)
        except arcpy.ExecuteError:
            logger.exception("ArcPy execution.")
            raise
        log_line('end', logline, log_level)
        return dataset_path

    @log_function
    def set_dataset_privileges(self, dataset_path, user_name, allow_view=None,
                               allow_edit=None, log_level='info'):
        """Set privileges for dataset in enterprise geodatabase."""
        logline = "Set privileges for {} on {}.".format(user_name,
                                                        dataset_path)
        log_line('start', logline, log_level)
        PRIVILEGE_MAP = {True: 'grant', False: 'revoke', None: 'as_is'}
        try:
            arcpy.management.ChangePrivileges(
                in_dataset = dataset_path, user = user_name,
                View = PRIVILEGE_MAP[allow_view],
                Edit = PRIVILEGE_MAP[allow_edit])
        except arcpy.ExecuteError:
            logger.exception("ArcPy execution.")
            raise
        log_line('end', logline, log_level)
        return dataset_path

    # Schema alteration methods.

    @log_function
    def add_field(self, dataset_path, field_name, field_type,
                  field_length=None, field_precision=None, field_scale=None,
                  field_is_nullable=True, field_is_required=False,
                  log_level='info'):
        """Add field to dataset."""
        logline = "Add field {}.{}.".format(dataset_path, field_name)
        log_line('start', logline, log_level)
        TYPE_CONVERSION_MAP = {'string': 'text', 'integer': 'long'}
        field_type = TYPE_CONVERSION_MAP.get(field_type, field_type)
        if field_type.lower() == 'text' and field_length is None:
            field_length = 64
        try:
            arcpy.management.AddField(
                dataset_path, field_name, field_type = field_type,
                field_length = field_length,
                field_precision = field_precision, field_scale = field_scale,
                field_is_nullable = field_is_nullable,
                field_is_required = field_is_required)
        except arcpy.ExecuteError:
            logger.exception("ArcPy execution.")
            raise
        log_line('end', logline, log_level)
        return field_name

    @log_function
    def add_fields_from_metadata_list(self, dataset_path, metadata_list,
                                      log_level='info'):
        """Add fields to dataset from a list of metadata dictionaries."""
        logline = "Add fields to {} from a metadata list.".format(dataset_path)
        log_line('start', logline, log_level)
        for field_metadata in metadata_list:
            _add_kwargs = {'dataset_path': dataset_path, 'log_level': None}
            for attribute in ['name', 'type', 'length', 'precision', 'scale',
                              'is_nullable', 'is_required']:
                if attribute in field_metadata:
                    _add_kwargs['field_{}'.format(attribute)] = (
                        field_metadata[attribute])
            try:
                field_name = self.add_field(**_add_kwargs)
            except arcpy.ExecuteError:
                logger.exception("ArcPy execution.")
                raise
            if log_level:
                getattr(logger, log_level)("Added {}.".format(field_name))
        log_line('end', logline, log_level)
        return [field_metadata['name'] for field_metadata in metadata_list]

    @log_function
    def add_index(self, dataset_path, field_names, index_name=None,
                  is_unique=False, is_ascending=False, log_level='info'):
        """Add index to dataset fields."""
        logline = "Add index for {}.{}.".format(dataset_path, field_names)
        log_line('start', logline, log_level)
        field_types = {
            field['type'].lower() for field
            in self.dataset_metadata(dataset_path)['fields']
            if field['name'].lower() in [name.lower() for name in field_names]}
        if 'geometry' in field_types and len(field_names) != 1:
            raise RuntimeError("Cannot create a composite spatial index.")
        elif 'geometry' in field_types:
            _add = arcpy.management.AddSpatialIndex
            _add_kwargs = {'in_features': dataset_path}
        else:
            _add = arcpy.management.AddIndex
            _add_kwargs = {
                'in_table': dataset_path, 'fields': field_names,
                'index_name': (index_name if index_name
                               else '_'.join(['ndx'] + field_names)),
                'unique': is_unique, 'ascending': is_ascending}
        try:
            _add(**_add_kwargs)
        except arcpy.ExecuteError:
            logger.exception("ArcPy execution.")
            raise
        log_line('end', logline, log_level)
        return dataset_path

    @log_function
    def delete_field(self, dataset_path, field_name, log_level='info'):
        """Delete field from dataset."""
        logline = "Delete field {}.".format(field_name)
        log_line('start', logline, log_level)
        try:
            arcpy.management.DeleteField(in_table = dataset_path,
                                         drop_field = field_name)
        except arcpy.ExecuteError:
            logger.exception("ArcPy execution.")
            raise
        log_line('end', logline, log_level)
        return field_name

    @log_function
    def duplicate_field(self, dataset_path, field_name, new_field_name,
                        duplicate_values=False, dataset_where_sql=None,
                        log_level='info'):
        """Create new field as a duplicate of another."""
        logline = "Duplicate {}.{} as {}.".format(dataset_path, field_name,
                                                  new_field_name)
        log_line('start', logline, log_level)
        new_field_metadata = self.field_metadata(dataset_path, field_name)
        new_field_metadata['name'] = new_field_name
        # Cannot add OID-type field, so push to a long-type.
        if new_field_metadata['type'].lower() == 'oid':
            new_field_metadata['type'] = 'long'
        self.add_fields_from_metadata_list(dataset_path, [new_field_metadata],
                                           log_level = None)
        if duplicate_values:
            self.update_field_by_function(
                dataset_path, new_field_metadata['name'],
                function = lambda x: x, field_as_first_arg = False,
                arg_field_names = [field_name],
                dataset_where_sql = dataset_where_sql, log_level = None)
        log_line('end', logline, log_level)
        return new_field_name

    @log_function
    def join_field(self, dataset_path, join_dataset_path, join_field_name,
                   on_field_name, on_join_field_name, log_level='info'):
        """Add field and its values from join-dataset."""
        logline = "Join field {} from {}.".format(join_field_name,
                                                  join_dataset_path)
        log_line('start', logline, log_level)
        try:
            arcpy.management.JoinField(
                dataset_path, in_field = on_field_name,
                join_table = join_dataset_path,
                join_field = on_join_field_name, fields = join_field_name)
        except arcpy.ExecuteError:
            logger.exception("ArcPy execution.")
            raise
        log_line('end', logline, log_level)
        return join_field_name

    @log_function
    def rename_field(self, dataset_path, field_name, new_field_name,
                     log_level='info'):
        """Rename field."""
        logline = "Rename field {}.{} to {}.".format(dataset_path, field_name,
                                                     new_field_name)
        log_line('start', logline, log_level)
        try:
            arcpy.management.AlterField(dataset_path,
                                        field_name, new_field_name)
        except arcpy.ExecuteError:
            logger.exception("ArcPy execution.")
            raise
        log_line('end', logline, log_level)
        return new_field_name

    # Feature alteration methods.

    @log_function
    def adjust_features_for_shapefile(
        self, dataset_path, datetime_null_replacement=datetime.date.min,
        integer_null_replacement=0, numeric_null_replacement=0.0,
        string_null_replacement='', log_level='info'):
        """Adjust features to meet shapefile requirements.

        Adjustments currently made:
        * Convert datetime values to date or time based on
        preserve_time_not_date flag.
        * Convert nulls to replacement value.
        """
        logline = "Adjust features in {} for shapefile output.".format(
            dataset_path)
        log_line('start', logline, log_level)
        log_line('feature_count', self.feature_count(dataset_path), log_level)
        TYPE_FUNCTION_MAP = {
            # Blob omitted: Not a valid shapefile type.
            'date': (lambda x: datetime_null_replacement if x is None
                     # Shapefiles can only store dates, not times.
                     else x.date()),
            'double': lambda x: numeric_null_replacement if x is None else x,
            # Geometry passed-through: Shapefile loader handles this.
            #'guid': Not valid shapefile type.
            'integer': lambda x: integer_null_replacement if x is None else x,
            # OID passed-through: Shapefile loader handles this.
            # Raster omitted: Not a valid shapefile type.
            'single': lambda x: numeric_null_replacement if x is None else x,
            'smallinteger': (lambda x: integer_null_replacement if x is None
                             else x),
            'string': lambda x: string_null_replacement if x is None else x}
        for field in self.dataset_metadata(dataset_path)['fields']:
            if field['type'].lower() in TYPE_FUNCTION_MAP:
                self.update_field_by_function(
                    dataset_path, field['name'],
                    function = TYPE_FUNCTION_MAP[field['type'].lower()],
                    log_level = None)
        log_line('feature_count', self.feature_count(dataset_path), log_level)
        log_line('end', logline, log_level)
        return dataset_path

    @log_function
    def clip_features(self, dataset_path, clip_dataset_path,
                      dataset_where_sql=None, clip_where_sql=None,
                      log_level='info'):
        """Clip feature geometry where overlaps clip dataset geometry."""
        logline = "Clip {} where geometry overlaps {}.".format(
            dataset_path, clip_dataset_path)
        log_line('start', logline, log_level)
        log_line('feature_count', self.feature_count(dataset_path), log_level)
        dataset_view_name = self.create_dataset_view(
            unique_name('dataset_view'), dataset_path,
            dataset_where_sql, log_level = None)
        clip_dataset_view_name = self.create_dataset_view(
            unique_name('clip_dataset_view'), clip_dataset_path,
            clip_where_sql, log_level = None)
        temp_output_path = unique_temp_dataset_path('temp_output')
        try:
            arcpy.analysis.Clip(
                in_features = dataset_view_name,
                clip_features = clip_dataset_view_name,
                out_feature_class = temp_output_path)
        except arcpy.ExecuteError:
            logger.exception("ArcPy execution.")
            raise
        self.delete_dataset(clip_dataset_view_name, log_level = None)
        # Load back into the dataset.
        self.delete_features(dataset_view_name, log_level = None)
        self.delete_dataset(dataset_view_name, log_level = None)
        self.insert_features_from_path(dataset_path, temp_output_path,
                                       log_level = None)
        self.delete_dataset(temp_output_path, log_level = None)
        log_line('feature_count', self.feature_count(dataset_path), log_level)
        log_line('end', logline, log_level)
        return dataset_path

    @log_function
    def delete_features(self, dataset_path, dataset_where_sql=None,
                        log_level='info'):
        """Delete select features."""
        logline = "Delete features from {} where {}.".format(dataset_path,
                                                             dataset_where_sql)
        log_line('start', logline, log_level)
        log_line('feature_count', self.feature_count(dataset_path), log_level)
        dataset_view_name = self.create_dataset_view(
            unique_name('dataset_view'), dataset_path,
            dataset_where_sql, log_level = None)
        dataset_metadata = self.dataset_metadata(dataset_path)
        # Can use (faster) truncate when:
        # (1) Database-type; (2) not in-memory; (3) no sub-selection.
        if all([dataset_where_sql is None,
                dataset_metadata['data_type'] in ['FeatureClass', 'Table'],
                dataset_metadata['workspace_path'] != 'in_memory']):
            _delete = arcpy.management.TruncateTable
            _delete_kwargs = {'in_table': dataset_view_name}
        elif dataset_metadata['is_spatial']:
            _delete = arcpy.management.DeleteFeatures
            _delete_kwargs = {'in_features': dataset_view_name}
        elif dataset_metadata['is_table']:
            _delete = arcpy.management.DeleteRows
            _delete_kwargs = {'in_rows': dataset_view_name}
        else:
            raise ValueError(
                "{} unsupported dataset type.".format(dataset_path))
        _delete(**_delete_kwargs)
        self.delete_dataset(dataset_view_name, log_level = None)
        log_line('feature_count', self.feature_count(dataset_path), log_level)
        log_line('end', logline, log_level)
        return dataset_path

    @log_function
    def dissolve_features(self, dataset_path, dissolve_field_names,
                          multipart=True, unsplit_lines=False,
                          dataset_where_sql=None, log_level='info'):
        """Merge features that share values in given fields."""
        logline = "Dissolve features in {} on {}.".format(dataset_path,
                                                          dissolve_field_names)
        log_line('start', logline, log_level)
        log_line('feature_count', self.feature_count(dataset_path), log_level)
        # Set the environment tolerance, so we can be sure the in_memory
        # datasets respect it. 0.003280839895013 is the default for all
        # datasets in our geodatabases.
        arcpy.env.XYTolerance = 0.003280839895013
        dataset_view_name = self.create_dataset_view(
            unique_name('dataset_view'), dataset_path,
            dataset_where_sql, log_level = None)
        temp_output_path = unique_temp_dataset_path('temp_output')
        try:
            arcpy.management.Dissolve(
                in_features = dataset_view_name,
                out_feature_class = temp_output_path,
                dissolve_field = dissolve_field_names, multi_part = multipart,
                unsplit_lines = unsplit_lines)
        except arcpy.ExecuteError:
            logger.exception("ArcPy execution.")
            raise
        # Delete undissolved features that are now dissolved (in the temp).
        self.delete_features(dataset_view_name, log_level = None)
        self.delete_dataset(dataset_view_name, log_level = None)
        # Copy the dissolved features (in the temp) to the dataset.
        self.insert_features_from_path(dataset_path, temp_output_path,
                                       log_level = None)
        self.delete_dataset(temp_output_path, log_level = None)
        log_line('feature_count', self.feature_count(dataset_path), log_level)
        log_line('end', logline, log_level)
        return dataset_path

    @log_function
    def erase_features(self, dataset_path, erase_dataset_path,
                       dataset_where_sql=None, erase_where_sql=None,
                       log_level='info'):
        """Erase feature geometry where overlaps erase dataset geometry."""
        logline = "Erase {} where geometry overlaps {}.".format(
            dataset_path, erase_dataset_path)
        log_line('start', logline, log_level)
        log_line('feature_count', self.feature_count(dataset_path), log_level)
        dataset_view_name = self.create_dataset_view(
            unique_name('dataset_view'), dataset_path,
            dataset_where_sql, log_level = None)
        erase_dataset_view_name = self.create_dataset_view(
            unique_name('erase_dataset_view'), erase_dataset_path,
            erase_where_sql, log_level = None)
        temp_output_path = unique_temp_dataset_path('temp_output')
        try:
            arcpy.analysis.Erase(
                in_features = dataset_view_name,
                erase_features = erase_dataset_view_name,
                out_feature_class = temp_output_path)
        except arcpy.ExecuteError:
            logger.exception("ArcPy execution.")
            raise
        self.delete_dataset(erase_dataset_view_name, log_level = None)
        # Load back into the dataset.
        self.delete_features(dataset_view_name, log_level = None)
        self.delete_dataset(dataset_view_name, log_level = None)
        self.insert_features_from_path(dataset_path, temp_output_path,
                                       log_level = None)
        self.delete_dataset(temp_output_path, log_level = None)
        log_line('feature_count', self.feature_count(dataset_path), log_level)
        log_line('end', logline, log_level)
        return dataset_path

    @log_function
    def keep_features_by_location(self, dataset_path, location_dataset_path,
                                  dataset_where_sql=None,
                                  location_where_sql=None, log_level='info'):
        """Keep features where geometry overlaps location feature geometry."""
        logline = "Keep {} where geometry overlaps {}.".format(
            dataset_path, location_dataset_path)
        log_line('start', logline, log_level)
        log_line('feature_count', self.feature_count(dataset_path), log_level)
        dataset_view_name = self.create_dataset_view(
            unique_name('dataset_view'), dataset_path,
            dataset_where_sql, log_level = None)
        location_dataset_view_name = self.create_dataset_view(
            unique_name('location_dataset_view'),
            location_dataset_path, location_where_sql, log_level = None)
        try:
            arcpy.management.SelectLayerByLocation(
                in_layer = dataset_view_name, overlap_type = 'intersect',
                select_features = location_dataset_view_name,
                selection_type = 'new_selection')
            # Switch selection for non-overlapping features (to delete).
            arcpy.management.SelectLayerByLocation(
                in_layer = dataset_view_name,
                selection_type = 'switch_selection')
        except arcpy.ExecuteError:
            logger.exception("ArcPy execution.")
            raise
        self.delete_dataset(location_dataset_view_name, log_level = None)
        self.delete_features(dataset_view_name, log_level = None)
        self.delete_dataset(dataset_view_name, log_level = None)
        log_line('feature_count', self.feature_count(dataset_path), log_level)
        log_line('end', logline, log_level)
        return dataset_path

    @log_function
    def identity_features(self, dataset_path, field_name,
                          identity_dataset_path, identity_field_name,
                          replacement_value=None, dataset_where_sql=None,
                          chunk_size=4096, log_level='info'):
        """Assign unique identity value to features, splitting where necessary.

        replacement_value is a value that will substitute as the identity
        value.
        This method has a 'chunking' routine in order to avoid an
        unhelpful output error that occurs when the inputs are rather large.
        For some reason, the identity will 'succeed' with and empty output
        warning, but not create an output dataset. Running the identity against
        smaller sets of data generally avoids this conundrum.
        """
        logline = "Identity features with {}.{}.".format(identity_dataset_path,
                                                         identity_field_name)
        log_line('start', logline, log_level)
        log_line('feature_count', self.feature_count(dataset_path), log_level)
        # Create a temporary copy of the overlay dataset.
        temp_overlay_path = self.copy_dataset(
            identity_dataset_path, unique_temp_dataset_path('temp_overlay'),
            log_level = None)
        # Avoid field name collisions with neutral holding field.
        temp_overlay_field_name = self.duplicate_field(
            temp_overlay_path, identity_field_name,
            new_field_name = unique_name(identity_field_name),
            duplicate_values = True, log_level = None)
        # Get an iterable of all object IDs in the dataset.
        with arcpy.da.SearchCursor(
            in_table = dataset_path, field_names = ['oid@'],
            where_clause = dataset_where_sql) as cursor:
            # Sorting is important, allows views with ID range instead of list.
            oids = sorted(oid for (oid,) in cursor)
        oid_field_name = self.dataset_metadata(dataset_path)['oid_field_name']
        while oids:
            chunk = oids[:chunk_size]
            oids = oids[chunk_size:]
            logger.debug(
                "Chunk: Feature OIDs {} to {}".format(chunk[0], chunk[-1]))
            # ArcPy where clauses cannot use 'between'.
            chunk_where_clause = (
                "{field} >= {from_oid} and {field} <= {to_oid}".format(
                    field = oid_field_name,
                    from_oid = chunk[0], to_oid = chunk[-1]))
            if dataset_where_sql:
                chunk_where_clause += " and ({})".format(dataset_where_sql)
            chunk_view_name = self.create_dataset_view(
                unique_name('chunk_view'), dataset_path,
                chunk_where_clause, log_level = None)
            # Create temporary dataset with the identity values.
            temp_output_path = unique_temp_dataset_path('temp_output')
            try:
                arcpy.analysis.Identity(
                    in_features = chunk_view_name,
                    identity_features = temp_overlay_path,
                    out_feature_class = temp_output_path,
                    join_attributes = 'all', relationship = False)
            except arcpy.ExecuteError:
                logger.exception("ArcPy execution.")
                raise
            # Push identity (or replacement) value from temp to update field.
            # Apply replacement value if necessary.
            if replacement_value is not None:
                self.update_field_by_function(
                    temp_output_path, field_name,
                    function = lambda x: replacement_value if x else None,
                    field_as_first_arg = False,
                    arg_field_names = [temp_overlay_field_name],
                    log_level = None)
            # Identity puts empty string when identity feature not present.
            # Fix to null (replacement value function does this inherently).
            else:
                self.update_field_by_function(
                    temp_output_path, field_name,
                    function = lambda x: None if x == '' else x,
                    field_as_first_arg = False,
                    arg_field_names = [temp_overlay_field_name],
                    log_level = None)
            # Replace original chunk features with identity features.
            self.delete_features(chunk_view_name, log_level = None)
            self.delete_dataset(chunk_view_name, log_level = None)
            self.insert_features_from_path(dataset_path, temp_output_path,
                                           log_level = None)
            self.delete_dataset(temp_output_path, log_level = None)
        self.delete_dataset(temp_overlay_path, log_level = None)
        log_line('feature_count', self.feature_count(dataset_path), log_level)
        log_line('end', logline, log_level)
        return dataset_path

    @log_function
    def insert_features_from_iterables(self, dataset_path,
                                       insert_dataset_iterables, field_names,
                                       log_level='info'):
        """Insert features from a collection of iterables."""
        logline = "Insert features into {} from iterables.".format(
            dataset_path)
        log_line('start', logline, log_level)
        log_line('feature_count', self.feature_count(dataset_path), log_level)
        # Create generator if insert_dataset_iterables is a generator function.
        if inspect.isgeneratorfunction(insert_dataset_iterables):
            insert_dataset_iterables = insert_dataset_iterables()
        with arcpy.da.InsertCursor(dataset_path, field_names) as cursor:
            for row in insert_dataset_iterables:
                cursor.insertRow(row)
        log_line('feature_count', self.feature_count(dataset_path), log_level)
        log_line('end', logline, log_level)
        return dataset_path

    @log_function
    def insert_features_from_path(self, dataset_path, insert_dataset_path,
                                  insert_where_sql=None, log_level='info'):
        """Insert features from a dataset referred to by a system path."""
        logline = "Insert features into {} from {}.".format(
            dataset_path, insert_dataset_path)
        log_line('start', logline, log_level)
        log_line('feature_count', self.feature_count(dataset_path), log_level)
        # Create field maps.
        # Added because ArcGIS Pro's no-test append is case-sensitive (verified
        # 1.0-1.1.1). BUG-000090970 - ArcGIS Pro 'No test' field mapping in
        # Append tool does not auto-map to the same field name if naming
        # convention differs.
        dataset_metadata = self.dataset_metadata(dataset_path)
        dataset_field_names = [field['name'].lower()
                               for field in dataset_metadata['fields']]
        insert_dataset_metadata = self.dataset_metadata(insert_dataset_path)
        insert_dataset_field_names = [field['name'].lower() for field
                                      in insert_dataset_metadata['fields']]
        # Append takes care of geometry & OIDs independent of the field maps.
        for field_name_type in ('geometry_field_name', 'oid_field_name'):
            if dataset_metadata.get(field_name_type):
                dataset_field_names.remove(
                    dataset_metadata[field_name_type].lower())
                insert_dataset_field_names.remove(
                    insert_dataset_metadata[field_name_type].lower())
        field_maps = arcpy.FieldMappings()
        for field_name in dataset_field_names:
            if field_name in insert_dataset_field_names:
                field_map = arcpy.FieldMap()
                field_map.addInputField(insert_dataset_path, field_name)
                field_maps.addFieldMap(field_map)
        insert_dataset_view_name = self.create_dataset_view(
            unique_name('insert_dataset_view'), insert_dataset_path,
            insert_where_sql, log_level = None)
        try:
            arcpy.management.Append(
                inputs = insert_dataset_view_name, target = dataset_path,
                schema_type = 'no_test', field_mapping = field_maps)
        except arcpy.ExecuteError:
            logger.exception("ArcPy execution.")
            raise
        self.delete_dataset(insert_dataset_view_name, log_level = None)
        log_line('feature_count', self.feature_count(dataset_path), log_level)
        log_line('end', logline, log_level)
        return dataset_path

    @log_function
    def overlay_features(self, dataset_path, field_name, overlay_dataset_path,
                         overlay_field_name, replacement_value=None,
                         overlay_most_coincident=False,
                         overlay_central_coincident=False,
                         dataset_where_sql=None, chunk_size=4096,
                         log_level='info'):
        """Assign overlay value to features, splitting where necessary.

        Please note that only one overlay flag at a time can be used. If
        mutliple are set to True, the first one referenced in the code
        will be used. If no overlay flags are set, the operation will perform a
        basic intersection check, and the result will be at the whim of the
        geoprocessing environment's merge rule for the update field.
        replacement_value is a value that will substitute as the identity
        value.
        This method has a 'chunking' routine in order to avoid an
        unhelpful output error that occurs when the inputs are rather large.
        For some reason, the identity will 'succeed' with and empty output
        warning, but not create an output dataset. Running the identity against
        smaller sets of data generally avoids this conundrum.
        """
        logline = "Overlay features with {}.{}.".format(overlay_dataset_path,
                                                        overlay_field_name)
        log_line('start', logline, log_level)
        log_line('feature_count', self.feature_count(dataset_path), log_level)
        # Check flags & set details for spatial join call.
        if overlay_most_coincident:
            raise NotImplementedError(
                "overlay_most_coincident not yet implemented.")
        elif overlay_central_coincident:
            join_operation = 'join_one_to_many'
            match_option = 'have_their_center_in'
        else:
            join_operation = 'join_one_to_many'
            match_option = 'intersect'
        # Create temporary copy of overlay dataset.
        temp_overlay_path = self.copy_dataset(
            overlay_dataset_path, unique_temp_dataset_path('temp_overlay'),
            log_level = None)
        # Avoid field name collisions with neutral holding field.
        temp_overlay_field_name = self.duplicate_field(
            temp_overlay_path, overlay_field_name,
            new_field_name = unique_name(overlay_field_name),
            duplicate_values = True, log_level = None)
        # Get an iterable of all object IDs in the dataset.
        with arcpy.da.SearchCursor(
            in_table = dataset_path, field_names = ['oid@'],
            where_clause = dataset_where_sql) as cursor:
            # Sorting is important, allows views with ID range instead of list.
            oids = sorted(oid for (oid,) in cursor)
        oid_field_name = self.dataset_metadata(dataset_path)['oid_field_name']
        while oids:
            chunk = oids[:chunk_size]
            oids = oids[chunk_size:]
            logger.debug(
                "Chunk: Feature OIDs {} to {}".format(chunk[0], chunk[-1]))
            # ArcPy where clauses cannot use 'between'.
            chunk_where_clause = (
                "{field} >= {from_oid} and {field} <= {to_oid}".format(
                    field = oid_field_name,
                    from_oid = chunk[0], to_oid = chunk[-1]))
            if dataset_where_sql:
                chunk_where_clause += " and ({})".format(dataset_where_sql)
            chunk_view_name = self.create_dataset_view(
                unique_name('chunk_view'), dataset_path,
                chunk_where_clause, log_level = None)
            # Create the temp output of the overlay.
            temp_output_path = unique_temp_dataset_path('temp_output')
            try:
                arcpy.analysis.SpatialJoin(
                    target_features = chunk_view_name,
                    join_features = temp_overlay_path,
                    out_feature_class = temp_output_path,
                    join_operation = join_operation, join_type = 'keep_all',
                    match_option = match_option)
            except arcpy.ExecuteError:
                logger.exception("ArcPy execution.")
                raise
            # Push overlay (or replacement) value from temp to update field.
            # Apply replacement value if necessary.
            if replacement_value is not None:
                self.update_field_by_function(
                    temp_output_path, field_name,
                    function = lambda x: replacement_value if x else None,
                    field_as_first_arg = False,
                    arg_field_names = [temp_overlay_field_name],
                    log_level = None)
            else:
                self.update_field_by_function(
                    temp_output_path, field_name,
                    function = lambda x: x,
                    field_as_first_arg = False,
                    arg_field_names = [temp_overlay_field_name],
                    log_level = None)
            # Replace original chunk features with overlay features.
            self.delete_features(chunk_view_name, log_level = None)
            self.delete_dataset(chunk_view_name, log_level = None)
            self.insert_features_from_path(dataset_path, temp_output_path,
                                           log_level = None)
            self.delete_dataset(temp_output_path, log_level = None)
        self.delete_dataset(temp_overlay_path, log_level = None)
        log_line('feature_count', self.feature_count(dataset_path), log_level)
        log_line('end', logline, log_level)
        return dataset_path

    @log_function
    def union_features(self, dataset_path, field_name, union_dataset_path,
                       union_field_name, replacement_value=None,
                       dataset_where_sql=None, chunk_size=4096,
                       log_level='info'):
        """Assign unique union value to features, splitting where necessary.

        replacement_value is a value that will substitute as the union value.
        """
        logline = "Union features with {}.{}.".format(union_dataset_path,
                                                      union_field_name)
        log_line('start', logline, log_level)
        log_line('feature_count', self.feature_count(dataset_path), log_level)
        # Create a temporary copy of the overlay dataset.
        temp_overlay_path = self.copy_dataset(
            union_dataset_path, unique_temp_dataset_path('temp_overlay'),
            log_level = None)
        # Avoid field name collisions with neutral holding field.
        temp_overlay_field_name = self.duplicate_field(
            temp_overlay_path, union_field_name,
            new_field_name = unique_name(union_field_name),
            duplicate_values = True, log_level = None)
        # Get an iterable of all object IDs in the dataset.
        with arcpy.da.SearchCursor(
            in_table = dataset_path, field_names = ['oid@'],
            where_clause = dataset_where_sql) as cursor:
            # Sorting is important, allows views with ID range instead of list.
            oids = sorted(oid for (oid,) in cursor)
        oid_field_name = self.dataset_metadata(dataset_path)['oid_field_name']
        while oids:
            chunk = oids[:chunk_size]
            oids = oids[chunk_size:]
            logger.debug(
                "Chunk: Feature OIDs {} to {}".format(chunk[0], chunk[-1]))
            # ArcPy where clauses cannot use 'between'.
            chunk_where_clause = (
                "{field} >= {from_oid} and {field} <= {to_oid}".format(
                    field = oid_field_name,
                    from_oid = chunk[0], to_oid = chunk[-1]))
            if dataset_where_sql:
                chunk_where_clause += " and ({})".format(dataset_where_sql)
            chunk_view_name = self.create_dataset_view(
                unique_name('chunk_view'), dataset_path,
                chunk_where_clause, log_level = None)
            # Create the temp output of the union.
            temp_output_path = unique_temp_dataset_path('temp_output')
            try:
                arcpy.analysis.Union(
                    in_features = [view_name, temp_overlay_path],
                    out_feature_class = temp_output_path,
                    join_attributes = 'all', gaps = False)
            except arcpy.ExecuteError:
                logger.exception("ArcPy execution.")
                raise
            # Push union (or replacement) value from temp to update field.
            # Apply replacement value if necessary.
            if replacement_value is not None:
                self.update_field_by_function(
                    temp_output_path, field_name,
                    function = lambda x: replacement_value if x else None,
                    field_as_first_arg = False,
                    arg_field_names = [temp_overlay_field_name],
                    log_level = None)
            # Union puts empty string when union feature not present.
            # Fix to null (replacement value function does this inherently).
            else:
                self.update_field_by_function(
                    temp_output_path, field_name,
                    function = lambda x: None if x == '' else x,
                    field_as_first_arg = False,
                    arg_field_names = [temp_overlay_field_name],
                    log_level = None)
            # Replace original chunk features with union features.
            self.delete_features(chunk_view_name, log_level = None)
            self.delete_dataset(chunk_view_name, log_level = None)
            self.insert_features_from_path(dataset_path, temp_output_path,
                                           log_level = None)
            self.delete_dataset(temp_output_path, log_level = None)
        self.delete_dataset(temp_overlay_path, log_level = None)
        log_line('feature_count', self.feature_count(dataset_path), log_level)
        log_line('end', logline, log_level)
        return dataset_path

    @log_function
    def update_field_by_coded_value_domain(self, dataset_path, field_name,
                                           code_field_name, domain_name,
                                           domain_workspace_path=None,
                                           dataset_where_sql=None,
                                           log_level='info'):
        """Update field values using a coded-values domain."""
        logline = "Update field {} using domain {} referenced in {}.".format(
            field_name, domain_name, code_field_name)
        log_line('start', logline, log_level)
        code_description_map = next(
            domain for domain in arcpy.da.ListDomains(
                domain_workspace_path if domain_workspace_path else self.path)
            if domain.name.lower() == domain_name.lower()).codedValues
        self.update_field_by_function(
            dataset_path, field_name, function = code_description_map.get,
            field_as_first_arg = False, arg_field_names = [code_field_name],
            dataset_where_sql = dataset_where_sql, log_level = None)
        log_line('end', logline, log_level)
        return field_name

    @log_function
    def update_field_by_constructor_method(self, dataset_path, field_name,
                                           constructor, method_name,
                                           field_as_first_arg=True,
                                           arg_field_names=[],
                                           kwarg_field_names=[],
                                           dataset_where_sql=None,
                                           log_level='info'):
        """Update field values by passing them to a constructed object method.

        wraps ArcWorkspace.update_field_by_function.
        """
        logline = ("Update field {} using method {}"
                   "from the object constructed by {}").format(
            field_name, method_name, constructor.__name__)
        log_line('start', logline, log_level)
        function = getattr(constructor(), method_name)
        self.update_field_by_function(
            dataset_path, field_name, function, field_as_first_arg,
            arg_field_names, kwarg_field_names, dataset_where_sql,
            log_level = None)
        log_line('end', logline, log_level)
        return field_name

    @log_function
    def update_field_by_expression(self, dataset_path, field_name, expression,
                                   dataset_where_sql=None, log_level='info'):
        """Update field values using a (single) code-expression."""
        logline = "Update field {} using expression <{}>.".format(
            field_name, expression)
        log_line('start', logline, log_level)
        dataset_view_name = self.create_dataset_view(
            unique_name('dataset_view'), dataset_path, dataset_where_sql,
            log_level = None)
        try:
            arcpy.management.CalculateField(
                in_table = dataset_view_name, field = field_name,
                expression = expression, expression_type = 'python_9.3')
        except arcpy.ExecuteError:
            logger.exception("ArcPy execution.")
            raise
        self.delete_dataset(dataset_view_name, log_level = None)
        log_line('end', logline, log_level)
        return field_name

    @log_function
    def update_field_by_feature_matching(self, dataset_path, field_name,
                                         identifier_field_names,
                                         update_value_type, flag_value=None,
                                         sort_field_names=[],
                                         dataset_where_sql=None,
                                         log_level='info'):
        """Update field values by aggregating info about matching features."""
        valid_update_value_types = ['flag-value', 'match-count', 'sort-order']
        raise NotImplementedError

    @log_function
    def update_field_by_function(self, dataset_path, field_name, function,
                                 field_as_first_arg=True, arg_field_names=[],
                                 kwarg_field_names=[], dataset_where_sql=None,
                                 log_level='info'):
        """Update field values by passing them to a function.

        field_as_first_arg flag indicates that the function will consume the
        field's value as the first argument.
        arg_field_names indicate fields whose values will be positional
        arguments passed to the function.
        kwarg_field_names indicate fields who values will be passed as keyword
        arguments (field name as key).
        """
        logline = "Update field {} using function {}.".format(
            field_name, function.__name__)
        log_line('start', logline, log_level)
        cursor_field_names = (
            [field_name] + list(arg_field_names) + list(kwarg_field_names))
        with arcpy.da.UpdateCursor(in_table = dataset_path,
                                   field_names = cursor_field_names,
                                   where_clause = dataset_where_sql) as cursor:
            for row in cursor:
                args = row[1:len(arg_field_names) + 1]
                if field_as_first_arg:
                    args.insert(0, row[0])
                kwargs = dict(zip(kwarg_field_names,
                                  row[len(arg_field_names) + 1:]))
                new_value = function(*args, **kwargs)
                if row[0] != new_value:
                    cursor.updateRow([new_value] + list(row[1:]))
        log_line('end', logline, log_level)
        return field_name

    @log_function
    def update_field_by_geometry(self, dataset_path, field_name,
                                 geometry_property_cascade, update_units=None,
                                 spatial_reference_id=None,
                                 dataset_where_sql=None, log_level='info'):
        """Update field values by cascading through a geometry's attributes.

        If the spatial reference ID is not specified, the spatial reference of
        the dataset is used.
        """
        logline = ("Update field {} values"
                   "using geometry property cascade {}.").format(
            field_name, geometry_property_cascade)
        log_line('start', logline, log_level)
        if update_units:
            raise NotImplementedError("update_units not yet implemented.")
        # Common property representations converted to fit our property map.
        PROPERTY_ALIAS_CONVERSION_MAP = {
            'x': 'x-coordinate', 'y': 'y-coordinate', 'z': 'z-coordinate',
            'xmin': 'x-minimum', 'ymin': 'y-minimum', 'zmin': 'z-minimum',
            'xmax': 'x-maximum', 'ymax': 'y-maximum', 'zmax': 'z-maximum'}
        GEOMETRY_PROPERTY_MAP = {
            'area': ['area'], 'length': ['length'],
            'centroid': ['centroid'], 'extent': ['extent'],
            'x-coordinate': ['X'], 'y-coordinate': ['Y'],
            'z-coordinate': ['Z'],
            'x-minimum': ['extent', 'XMin'], 'y-minimum': ['extent', 'YMin'],
            'z-minimum': ['extent', 'ZMin'],
            'x-maximum': ['extent', 'XMax'], 'y-maximum': ['extent', 'YMax'],
            'z-maximum': ['extent', 'ZMax']}
        with arcpy.da.UpdateCursor(
            in_table = dataset_path, field_names = [field_name, 'shape@'],
            where_clause = dataset_where_sql,
            spatial_reference = (arcpy.SpatialReference(spatial_reference_id)
                                 if spatial_reference_id else None)) as cursor:
            for field_value, geometry in cursor:
                if geometry is None:
                    new_value = None
                else:
                    new_value = geometry
                    # Cascade down the geometry properties.
                    for _property in geometry_property_cascade:
                        _property = _property.lower()
                        # Convert property if an alias.
                        if _property not in GEOMETRY_PROPERTY_MAP:
                            try:
                                _property = (
                                    PROPERTY_ALIAS_CONVERSION_MAP[_property])
                            except KeyError:
                                raise ValueError(
                                    "Property {} in the cascade not a valid"
                                    "geometry property or alias.")
                        for sub_property in GEOMETRY_PROPERTY_MAP[_property]:
                            new_value = getattr(new_value, sub_property)
                if new_value != field_value:
                    cursor.updateRow((new_value, geometry))
        log_line('end', logline, log_level)
        return field_name

    @log_function
    def update_field_by_joined_value(self, dataset_path, field_name,
                                     join_dataset_path, join_field_name,
                                     on_field_pairs, dataset_where_sql=None,
                                     log_level='info'):
        """Update field values by referencing a joinable field."""
        logline = "Update field {} with joined values from {}.{}>.".format(
            field_name, join_dataset_path,  join_field_name)
        log_line('start', logline, log_level)
        # Build join-reference.
        with arcpy.da.SearchCursor(
            in_table = join_dataset_path,
            field_names = (
                [join_field_name] + [pair[1] for pair in on_field_pairs])
            ) as cursor:
            join_value = {tuple(row[1:]): row[0] for row in cursor}
        with arcpy.da.UpdateCursor(
            in_table = dataset_path,
            field_names = [field_name] + [pair[0] for pair in on_field_pairs],
            where_clause = dataset_where_sql
            ) as cursor:
            for row in cursor:
                new_value = join_value.get(tuple(row[1:]))
                if row[0] != new_value:
                    cursor.updateRow([new_value] + list(row[1:]))
        log_line('end', logline, log_level)
        return field_name

    @log_function
    def update_field_by_near_feature(self, dataset_path, field_name,
                                     near_dataset_path, near_field_name,
                                     replacement_value=None,
                                     distance_field_name=None,
                                     angle_field_name=None,
                                     x_coordinate_field_name=None,
                                     y_coordinate_field_name=None,
                                     max_search_distance=None, near_rank=1,
                                     dataset_where_sql=None, log_level='info'):
        """Update field by finding near-feature value.

        One can optionally update ancillary fields with analysis properties by
        indicating the following fields: distance_field_name, angle_field_name,
        x_coordinate_field_name, y_coordinate_field_name.
        """
        logline = "Update field {} using near-values {}.{}.".format(
            field_name, near_dataset_path,  near_field_name)
        log_line('start', logline, log_level)
        # Create a temporary copy of near dataset.
        temp_near_path = self.copy_dataset(
            near_dataset_path, unique_temp_dataset_path('temp_near'),
            log_level = None)
        # Avoid field name collisions with neutral holding field.
        temp_near_field_name = self.duplicate_field(
            temp_near_path, near_field_name,
            new_field_name = unique_name(near_field_name),
            duplicate_values = True, log_level = None)
        # Create the temp output of the near features.
        dataset_view_name = self.create_dataset_view(
            unique_name('dataset_view'), dataset_path, dataset_where_sql,
            log_level = None)
        temp_output_path = unique_temp_dataset_path('temp_output')
        try:
            arcpy.analysis.GenerateNearTable(
                in_features = dataset_view_name,
                near_features = temp_near_path,
                out_table = temp_output_path,
                search_radius = max_search_distance,
                location = any([x_coordinate_field_name,
                                y_coordinate_field_name]),
                angle = any([angle_field_name]),
                closest = 'all', closest_count = near_rank,
                # Would prefer geodesic, but that forces XY values to lon-lat.
                method ='planar')
        except arcpy.ExecuteError:
            logger.exception("ArcPy execution.")
            raise
        self.delete_dataset(dataset_view_name, log_level = None)
        # Remove near rows not matching chosen rank.
        self.delete_features(
            temp_output_path,
            dataset_where_sql = "near_rank <> {}".format(near_rank),
            log_level = None)
        # Join near values to the near output.
        self.join_field(
            temp_output_path, join_dataset_path = temp_near_path,
            join_field_name = temp_near_field_name, on_field_name = 'near_fid',
            on_join_field_name = (
                self.dataset_metadata(temp_near_path)['oid_field_name']),
            log_level = None)
        self.delete_dataset(temp_near_path, log_level = None)
        # Push overlay (or replacement) value from temp to update field.
        # Apply replacement value if necessary.
        if replacement_value is not None:
            self.update_field_by_function(
                temp_output_path, field_name,
                function = lambda x: replacement_value if x else None,
                field_as_first_arg = False,
                arg_field_names = [temp_near_field_name],
                log_level = None)
        else:
            self.update_field_by_function(
                temp_output_path, field_name,
                function = lambda x: x,
                field_as_first_arg = False,
                arg_field_names = [temp_near_field_name],
                log_level = None)
        # Update values in original dataset.
        dataset_oid_field_name = (
            self.dataset_metadata(dataset_path)['oid_field_name'])
        self.update_field_by_joined_value(
            dataset_path, field_name,
            join_dataset_path = temp_output_path, join_field_name = field_name,
            on_field_pairs = [(dataset_oid_field_name, 'in_fid')],
            dataset_where_sql = dataset_where_sql, log_level = None)
        # Update ancillary near property fields.
        if distance_field_name:
            self.update_field_by_joined_value(
                dataset_path, distance_field_name,
                join_dataset_path = temp_output_path,
                join_field_name = 'near_dist',
                on_field_pairs = [(dataset_oid_field_name, 'in_fid')],
                dataset_where_sql = dataset_where_sql, log_level = None
                )
        if angle_field_name:
            self.update_field_by_joined_value(
                dataset_path, angle_field_name,
                join_dataset_path = temp_output_path,
                join_field_name = 'near_angle',
                on_field_pairs = [(dataset_oid_field_name, 'in_fid')],
                dataset_where_sql = dataset_where_sql, log_level = None
                )
        if x_coordinate_field_name:
            self.update_field_by_joined_value(
                dataset_path, x_coordinate_field_name,
                join_dataset_path = temp_output_path,
                join_field_name = 'near_x',
                on_field_pairs = [(dataset_oid_field_name, 'in_fid')],
                dataset_where_sql = dataset_where_sql, log_level = None
                )
        if y_coordinate_field_name:
            self.update_field_by_joined_value(
                dataset_path, y_coordinate_field_name,
                join_dataset_path = temp_output_path,
                join_field_name = 'near_y',
                on_field_pairs = [(dataset_oid_field_name, 'in_fid')],
                dataset_where_sql = dataset_where_sql, log_level = None
                )
        self.delete_dataset(temp_output_path, log_level = None)
        log_line('end', logline, log_level)
        return field_name

    @log_function
    def update_field_by_overlay(self, dataset_path, field_name,
                                overlay_dataset_path, overlay_field_name,
                                replacement_value=None,
                                overlay_most_coincident=False,
                                overlay_central_coincident=False,
                                dataset_where_sql=None, log_level='info'):
        """Update field by finding overlay feature value.

        Since only one value will be selected in the overlay, operations with
        multiple overlaying features will respect the geoprocessing
        environment's merge rule. This rule generally defaults to the 'first'
        feature's value.

        Please note that only one overlay flag at a time can be used (e.g.
        overlay_most_coincident, overlay_central_coincident). If mutliple are
        set to True, the first one referenced in the code will be used. If no
        overlay flags are set, the operation will perform a basic intersection
        check, and the result will be at the whim of the geoprocessing
        environment's merge rule for the update field.
        """
        logline = "Update field {} using overlay values {}.{}.".format(
            field_name, overlay_dataset_path,  overlay_field_name)
        log_line('start', logline, log_level)
        # Check flags & set details for spatial join call.
        if overlay_most_coincident:
            raise NotImplementedError(
                "overlay_most_coincident is not yet implemented.")
        elif overlay_central_coincident:
            join_operation = 'join_one_to_one'
            match_option = 'have_their_center_in'
        else:
            join_operation = 'join_one_to_one'
            match_option = 'intersect'
        # Create temporary copy of overlay dataset.
        temp_overlay_path = self.copy_dataset(
            overlay_dataset_path, unique_temp_dataset_path('temp_overlay'),
            log_level = None)
        # Avoid field name collisions with neutral holding field.
        temp_overlay_field_name = self.duplicate_field(
            temp_overlay_path, overlay_field_name,
            new_field_name = unique_name(overlay_field_name),
            duplicate_values = True, log_level = None)
        # Create temp output of the overlay.
        dataset_view_name = self.create_dataset_view(
            unique_name('dataset_view'), dataset_path, dataset_where_sql,
            log_level = None)
        temp_output_path = unique_temp_dataset_path('temp_output')
        try:
            arcpy.analysis.SpatialJoin(
                target_features = dataset_view_name,
                join_features = temp_overlay_path,
                out_feature_class = temp_output_path,
                join_operation = join_operation, join_type = 'keep_common',
                match_option = match_option)
        except arcpy.ExecuteError:
            logger.exception("ArcPy execution.")
            raise
        self.delete_dataset(dataset_view_name, log_level = None)
        self.delete_dataset(temp_overlay_path, log_level = None)
        # Push overlay (or replacement) value from temp to update field.
        # Apply replacement value if necessary.
        if replacement_value is not None:
            self.update_field_by_function(
                temp_output_path, field_name,
                function = lambda x: replacement_value if x else None,
                field_as_first_arg = False,
                arg_field_names = [temp_overlay_field_name],
                log_level = None)
        else:
            self.update_field_by_function(
                temp_output_path, field_name,
                function = lambda x: x,
                field_as_first_arg = False,
                arg_field_names = [temp_overlay_field_name],
                log_level = None)
        # Update values in original dataset.
        self.update_field_by_joined_value(
            dataset_path, field_name,
            join_dataset_path = temp_output_path, join_field_name = field_name,
            on_field_pairs = [
                (self.dataset_metadata(dataset_path)['oid_field_name'],
                 'target_fid')],
            dataset_where_sql = dataset_where_sql, log_level = None)
        self.delete_dataset(temp_output_path, log_level = None)
        log_line('end', logline, log_level)
        return field_name

    @log_function
    def update_field_by_unique_id(self, dataset_path, field_name,
                                  dataset_where_sql=None, log_level='info'):
        """Update field values by assigning a unique ID."""
        logline = "Update field {} using unique IDs.".format(field_name)
        log_line('start', logline, log_level)
        field_metadata = self.field_metadata(dataset_path, field_name)
        field_type_map = {
            'double': float, 'single': float,
            'integer': int, 'long': int, 'short': int, 'smallinteger': int,
            'guid': uuid.UUID,
            'string': str, 'text': str}
        unique_id_pool = unique_ids(
            data_type = field_type_map[field_metadata['type']],
            string_length = field_metadata.get('length', 16))
        with arcpy.da.UpdateCursor(
            in_table = dataset_path, field_names = [field_name],
            where_clause = dataset_where_sql) as cursor:
            for row in cursor:
                cursor.updateRow([next(unique_id_pool)])
        log_line('end', logline, log_level)
        return field_name

    @log_function
    def update_fields_by_geometry_node_ids(self, dataset_path,
                                           from_id_field_name,
                                           to_id_field_name,
                                           log_level='info'):
        """Update fields with node IDs based on feature geometry.

        Method assumes the IDs are numeric.
        """
        logline = (
            "Update node ID fields {} & {} based on feature geometry.").format(
            from_id_field_name, to_id_field_name)
        log_line('start', logline, log_level)
        # Get next available node ID.
        node_ids = set()
        with arcpy.da.SearchCursor(
            in_table = dataset_path,
            field_names = [from_id_field_name, to_id_field_name]
            ) as cursor:
            for row in cursor:
                node_ids.update(row)
        # Remove missing node ID instance.
        node_ids.discard(None)
        next_open_node_id = max(node_ids) + 1 if node_ids else 1
        # Build node XY mapping.
        with arcpy.da.SearchCursor(
            in_table = dataset_path,
            field_names = ['oid@', from_id_field_name,
                           to_id_field_name, 'shape@']) as cursor:
            node_xy_map= {}
            # {node_xy: {'node_id': int(), 'f_oids': set(), 't_oids': set()},}
            for oid, fnode_id, tnode_id, geometry in cursor:
                fnode_xy = geometry.firstPoint.X, geometry.firstPoint.Y
                tnode_xy = geometry.lastPoint.X, geometry.lastPoint.Y
                # Add the XY if not yet present.
                for node_id, xy, oid_set_key in [
                    (fnode_id, fnode_xy, 'f_oids'),
                    (tnode_id, tnode_xy, 't_oids')]:
                    if xy not in node_xy_map:
                        # Add XY with the node ID.
                        node_xy_map[xy] = {'node_id': None,
                                           'f_oids': set(), 't_oids': set()}
                    # Choose lowest non-missing ID to perpetuate at the XY.
                    try:
                        node_xy_map[xy]['node_id'] = min(
                            x for x in [node_xy_map[xy]['node_id'], node_id]
                            if x is not None)
                    # ValueError means there's no ID already on there.
                    except ValueError:
                        node_xy_map[xy]['node_id'] = next_open_node_id
                        next_open_node_id += 1
                    # Add the link ID to the corresponding link set.
                    node_xy_map[xy][oid_set_key].add(oid)
        # Pivot node_xy_map into a node ID map.
        node_id_map = {}
        # {node_id: {'node_xy': tuple(), 'feature_count': int()},}
        for new_xy in node_xy_map.keys():
            new_node_id = node_xy_map[new_xy]['node_id']
            new_feature_count = len(
                node_xy_map[new_xy]['f_oids'].union(
                    node_xy_map[new_xy]['t_oids']))
            # If ID already present in node_id_map, re-ID one of the nodes.
            if new_node_id in node_id_map:
                old_node_id = new_node_id
                old_xy = node_id_map[old_node_id]['node_xy']
                old_feature_count = node_id_map[old_node_id]['feature_count']
                # If new node has more links, re-ID old node.
                if new_feature_count > old_feature_count:
                    node_xy_map[old_xy]['node_id'] = next_open_node_id
                    node_id_map[next_open_node_id] = (
                        node_id_map.pop(old_node_id))
                    next_open_node_id += 1
                # Re-ID new node if old node has more links (or tequal counts).
                else:
                    node_xy_map[new_xy]['node_id'] = next_open_node_id
                    new_node_id = next_open_node_id
                    next_open_node_id += 1
            # Now add the new node.
            node_id_map[new_node_id] = {'node_xy': new_xy,
                                        'feature_count': new_feature_count}
        # Build a feature-node mapping from node_xy_map.
        feature_nodes = {}
        # {feature_oid: {'fnode': int(), 'tnode': int()},}
        for xy in node_xy_map:
            node_id = node_xy_map[xy]['node_id']
            # If feature object ID is missing in feature_nodes: add.
            for feature_oid in node_xy_map[xy]['f_oids'].union(
                node_xy_map[xy]['t_oids']):
                if feature_oid not in feature_nodes:
                    feature_nodes[feature_oid] = {}
            for feature_oid in node_xy_map[xy]['f_oids']:
                feature_nodes[feature_oid]['fnode'] = node_id
            for feature_oid in node_xy_map[xy]['t_oids']:
                feature_nodes[feature_oid]['tnode'] = node_id
        # Push changes to features.
        with arcpy.da.UpdateCursor(
            in_table = dataset_path,
            field_names = ['oid@',
                           from_id_field_name, to_id_field_name]) as cursor:
            for oid, old_fnode_id, old_tnode_id in cursor:
                new_fnode_id = feature_nodes[oid]['fnode']
                new_tnode_id = feature_nodes[oid]['tnode']
                if any([old_fnode_id != new_fnode_id,
                        old_tnode_id != new_tnode_id]):
                    cursor.updateRow([oid, new_fnode_id, new_tnode_id])
        log_line('end', logline, log_level)
        return from_id_field_name, to_id_field_name

    # Analysis methods.

    @log_function
    def generate_facility_service_rings(self, dataset_path, output_path,
                                        network_path, cost_attribute,
                                        ring_width, max_distance,
                                        restriction_attributes=[],
                                        travel_from_facility=False,
                                        detailed_rings=False,
                                        overlap_facilities=True,
                                        id_field_name=None,
                                        dataset_where_sql=None,
                                        log_level='info'):
        """Create facility service ring features using a network dataset."""
        logline = "Generate service rings for facilities in {}".format(
            dataset_path)
        log_line('start', logline, log_level)
        # Get Network Analyst license.
        if arcpy.CheckOutExtension('Network') != 'CheckedOut':
            raise RuntimeError("Unable to check out Network Analyst license.")
        # Create break value range.
        break_values = range(ring_width, max_distance + 1, ring_width)
        dataset_view_name = self.create_dataset_view(
            unique_name('dataset_view'), dataset_path, dataset_where_sql,
            log_level = None)
        try:
            arcpy.na.MakeServiceAreaLayer(
                in_network_dataset = network_path,
                out_network_analysis_layer = 'service_area',
                impedance_attribute = cost_attribute,
                travel_from_to = ('travel_from' if travel_from_facility
                                  else 'travel_to'),
                default_break_values = ' '.join(
                    str(x) for x
                    in range(ring_width, max_distance + 1, ring_width)),
                polygon_type = ('detailed_polys' if detailed_rings
                                else 'simple_polys'),
                merge = 'no_merge' if overlap_facilities else 'no_overlap',
                nesting_type = 'rings',
                UTurn_policy = 'allow_dead_ends_and_intersections_only',
                restriction_attribute_name = restriction_attributes,
                # The trim seems to override the non-overlapping part in
                # larger analyses.
                #polygon_trim = True, poly_trim_value = ring_width,
                hierarchy = 'no_hierarchy')
        except arcpy.ExecuteError:
            logger.exception("ArcPy execution.")
            raise
        try:
            arcpy.na.AddLocations(
                in_network_analysis_layer = "service_area",
                sub_layer = "Facilities", in_table = dataset_view_name,
                field_mappings = 'Name {} #'.format(id_field_name),
                search_tolerance = max_distance,
                match_type = 'match_to_closest', append = 'clear',
                snap_to_position_along_network = 'no_snap',
                exclude_restricted_elements = True)
        except arcpy.ExecuteError:
            logger.exception("ArcPy execution.")
            raise
        try:
            arcpy.na.Solve(
                in_network_analysis_layer = "service_area",
                ignore_invalids = True, terminate_on_solve_error = True)
        except arcpy.ExecuteError:
            logger.exception("ArcPy execution.")
            raise
        self.copy_dataset('service_area/Polygons', output_path,
                          log_level = None)
        id_field_metadata = self.field_metadata(dataset_path, id_field_name)
        self.add_fields_from_metadata_list(output_path, [id_field_metadata],
                                           log_level = None)
        type_extract_function_map = {
            'short': lambda x: int(x.split(' : ')[0]) if x else None,
            'long': lambda x: int(x.split(' : ')[0]) if x else None,
            'double': lambda x: float(x.split(' : ')[0]) if x else None,
            'single': lambda x: float(x.split(' : ')[0]) if x else None,
            'string': lambda x: x.split(' : ')[0] if x else None}
        self.delete_dataset('service_area')
        self.update_field_by_function(
            output_path, id_field_name,
            function = type_extract_function_map[id_field_metadata['type']],
            field_as_first_arg = False, arg_field_names = ['Name'],
            log_level = None)
        self.delete_dataset(dataset_view_name, log_level = None)
        log_line('end', logline, log_level)
        return output_path

    # Conversion/extraction methods.

    @log_function
    def convert_polygons_to_lines(self, dataset_path, output_path,
                                  topological=False, id_field_name=None,
                                  dataset_where_sql=None, log_level='info'):
        """Convert geometry from polygons to lines.

        If topological is set to True, shared outlines will be a single,
        separate feature. Note that one cannot pass attributes to a
        topological transformation (as the values would not apply to all
        adjacent features).

        If an id field name is specified, the output dataset will identify the
        input features that defined the line feature with the name & values
        from the provided field. This option will be ignored if the output is
        non-topological lines, as the field will pass over with the rest of
        the attributes.
        """
        logline = (
            "Convert polygon features in {} to lines.".format(dataset_path))
        log_line('start', logline, log_level)
        dataset_view_name = self.create_dataset_view(
            unique_name('dataset_view'), dataset_path, dataset_where_sql,
            log_level = None)
        try:
            arcpy.management.PolygonToLine(
                in_features = dataset_view_name,
                out_feature_class = output_path, neighbor_option = topological)
        except arcpy.ExecuteError:
            logger.exception("ArcPy execution.")
            raise
        self.delete_dataset(dataset_view_name)
        if topological:
            if id_field_name:
                id_field_info = self.field_metadata(dataset_path,
                                                    id_field_name)
                oid_field_name = (
                    self.dataset_metadata(dataset_path)['oid_field_name'])
            sides = ['left', 'right']
            for side in sides:
                side_oid_field_name = '{}_FID'.format(side.upper())
                if id_field_name:
                    side_id_field_info = id_field_info.copy()
                    side_id_field_info['name'] = '{}_{}'.format(
                        side, id_field_name
                        )
                    # Cannot create an OID-type field, so force to long.
                    if side_id_field_info['type'].lower() == 'oid':
                        side_id_field_info['type'] = 'long'
                    self.add_fields_from_metadata_list(
                        output_path, [side_id_field_info], log_level = None)
                    self.update_field_by_join_value(
                        dataset_path = output_path,
                        field_name = side_id_field_info['name'],
                        join_dataset_path = dataset_path,
                        join_field_name = id_field_name,
                        on_field_name = side_oid_field_name,
                        on_join_field_name = oid_field_name, log_level = None)
                self.delete_field(output_path, side_oid_field_name,
                                  log_level = None)
        else:
            self.delete_field(output_path, 'ORIG_FID', log_level = None)
        log_line('end', logline, log_level)
        return output_path

    @log_function
    def convert_table_to_spatial_dataset(self, dataset_path, output_path,
                                         x_field_name, y_field_name,
                                         z_field_name=None,
                                         spatial_reference_id=4326,
                                         dataset_where_sql=None,
                                         log_level='info'):
        """Convert nonspatial coordinate table to a new spatial dataset."""
        logline = "Convert {} to spatial dataset.".format(dataset_path)
        log_line('start', logline, log_level)
        dataset_view_name = unique_name('dataset_view')
        try:
            arcpy.management.MakeXYEventLayer(
                table = dataset_path, out_layer = dataset_view_name,
                in_x_field = x_field_name, in_y_field = y_field_name,
                in_z_field = z_field_name,
                spatial_reference = (
                    arcpy.SpatialReference(spatial_reference_id)))
        except arcpy.ExecuteError:
            logger.exception("ArcPy execution.")
            raise
        self.copy_dataset(dataset_view_name, output_path, dataset_where_sql)
        self.delete_dataset(dataset_view_name)
        log_line('end', logline, log_level)
        return output_path

    @log_function
    def planarize_features(self, dataset_path, output_path,
                           dataset_where_sql=None, log_level='info'):
        """Convert feature geometry to lines - planarizing them.

        This method does not make topological linework. However it does carry
        all attributes with it, rather than just an ID attribute.

        Since this method breaks the new line geometry at intersections, it
        can be useful to break line geometry features at them.
        """
        logline = "Planarize features in {}.".format(dataset_path)
        log_line('start', logline, log_level)
        dataset_view_name = self.create_dataset_view(
            unique_name('dataset_view'), dataset_path, dataset_where_sql,
            log_level = None)
        try:
            arcpy.management.FeatureToLine(
                in_features = dataset_view_name,
                out_feature_class = output_path,
                ##cluster_tolerance,
                attributes = True)
        except arcpy.ExecuteError:
            logger.exception("ArcPy execution.")
            raise
        log_line('end', logline, log_level)
        return output_path

    @log_function
    def project(self, dataset_path, output_path, spatial_reference_id,
                dataset_where_sql=None, log_level='info'):
        """Project dataset features to a new dataset."""
        logline = "Project {} to {}.".format(
            dataset_path, arcpy.SpatialReference(spatial_reference_id).name)
        log_line('start', logline, log_level)
        dataset_metadata = self.dataset_metadata(dataset_path)
        # Project tool cannot output to an in-memory workspace (will throw
        # error 000944). Not a bug. Esri's Project documentation (as of v10.4)
        # specifically states: "The in_memory workspace is not supported as a
        # location to write the output dataset."
        # To avoid all this ado, we'll create a clone dataset & copy features.
        self.create_dataset(
            output_path,
            [field for field in dataset_metadata.fields
             # Geometry & OID taken care of internally.
             if field['type'].lower() not in ['geometry ', 'oid']],
            dataset_metadata['geometry_type'], spatial_reference_id,
            log_level = None)
        self.copy_dataset(dataset_path, output_path, dataset_where_sql,
                          log_level = None)
        log_line('end', logline, log_level)
        return output_path

    @log_function
    def write_rows_to_csvfile(self, rows, output_path, field_names,
                              header=False, file_mode='wb', log_level='info'):
        """Write collected of rows to a CSV-file.

        The rows can be represented by either a dictionary or iterable.
        """
        logline = "Write rows iterable to CSV-file {}".format(output_path)
        log_line('start', logline, log_level)
        with open(output_path, file_mode) as csvfile:
            for index, row in enumerate(rows):
                if index == 0:
                    if isinstance(row, dict):
                        writer = csv.DictWriter(csvfile, field_names)
                        if header:
                            writer.writeheader()
                    elif isinstance(row, collections.Sequence):
                        writer = csv.writer(csvfile)
                        if header:
                            writer.writerow(field_names)
                    else:
                        raise TypeError(
                            "Row objects must be dictionaries or sequences.")
                writer.writerow(row)
        log_line('end', logline, log_level)
        return output_path

    # Generators.

    @log_function
    def field_values(self, dataset_path, field_names,
                     spatial_reference_id=None, dataset_where_sql=None):
        """Generator for tuples of feature field values."""
        with arcpy.da.SearchCursor(
            in_table = dataset_path, field_names = field_names,
            where_clause = dataset_where_sql,
            spatial_reference = (arcpy.SpatialReference(spatial_reference_id)
                                 if spatial_reference_id else None)) as cursor:
            for values in cursor:
                yield values

    @log_function
    def oid_field_value(self, dataset_path, field_name,
                        spatial_reference_id=None, dataset_where_sql=None):
        """Generator for tuples of (OID, field_value)."""
        for oid, value in self.field_values(
            dataset_path, ['oid@', field_name], spatial_reference_id,
            dataset_where_sql):
            yield (oid, value)

    @log_function
    def oid_geometry(self, dataset_path, spatial_reference_id=None,
                     dataset_where_sql=None):
        """Generator for tuples of (OID, geometry)."""
        for oid, value in self.oid_field_value(
            dataset_path, 'shape@', spatial_reference_id, dataset_where_sql):
            yield (oid, value)

    @log_function
    def xref_near_features(self, dataset_path, dataset_id_field_name,
                           xref_path, xref_id_field_name,
                           max_near_distance=None, only_closest=False,
                           include_distance=False, include_rank=False,
                           include_angle=False, include_x_coordinate=False,
                           include_y_coordinate=False, dataset_where_sql=None):
        """Generator for cross-referenced near feature-pairs.

        Yielded objects will include at least the dataset ID & XRef ID.
        Setting max_near_distance to NoneType will generate every possible
        feature cross-reference.
        Setting only_closest to True will generate a cross reference only with
        the closest feature.
        Setting any include_* to True will include that value in the generated
        tuple, in argument order.
        Distance values will match the linear unit of the main dataset.
        Angle values are in decimal degrees.
        """
        dataset_view_name = self.create_dataset_view(
            unique_name('dataset_view'), dataset_path, dataset_where_sql,
            log_level = None)
        temp_near_path = unique_temp_dataset_path('temp_near')
        try:
            arcpy.analysis.GenerateNearTable(
                in_features = dataset_view_name, near_features = xref_path,
                out_table = temp_near_path, search_radius = max_near_distance,
                location = any([include_x_coordinate, include_y_coordinate]),
                angle = include_angle, closest = only_closest,
                method = 'geodesic')
        except arcpy.ExecuteError:
            logger.exception("ArcPy execution.")
            raise
        near_field_names = ['in_fid', 'near_fid']
        if include_distance:
            near_field_names.append('near_dist')
        if include_rank:
            near_field_names.append('near_rank')
        if include_angle:
            near_field_names.append('near_angle')
        if include_x_coordinate:
            near_field_names.append('near_x')
        if include_y_coordinate:
            near_field_names.append('near_y')
        dataset_oid_id_map = self.oid_field_value_map(dataset_view_name,
                                                      dataset_id_field_name)
        xref_oid_id_map = self.oid_field_value_map(xref_path,
                                                   xref_id_field_name)
        with arcpy.da.SearchCursor(in_table = temp_near_path,
                                   field_names = near_field_names) as cursor:
            for row in cursor:
                row_info = dict(zip(cursor.fields, row))
                result = [dataset_oid_id_map[row_info['in_fid']],
                          xref_oid_id_map[row_info['near_fid']]]
                if include_distance:
                    result.append(row_info['near_dist'])
                if include_rank:
                    result.append(row_info['near_rank'])
                if include_angle:
                    result.append(row_info['near_angle'])
                if include_x_coordinate:
                    result.append(row_info['near_x'])
                if include_y_coordinate:
                    result.append(row_info['near_y'])
                yield tuple(result)
        self.delete_dataset(temp_near_path)

    # Mappings.

    @log_function
    def oid_field_value_map(self, dataset_path, field_name,
                            spatial_reference_id=None, dataset_where_sql=None):
        """Return dictionary mapping of field value for the feature OID."""
        return {oid: value for oid, value
                in self.oid_field_value(
                    dataset_path, field_name, spatial_reference_id,
                    dataset_where_sql)}

    @log_function
    def oid_geometry_map(self, dataset_path, spatial_reference_id=None,
                         dataset_where_sql=None):
        """Return dictionary mapping of geometry for the feature OID."""
        return self.oid_field_value_map(
            dataset_path, 'shape@', spatial_reference_id, dataset_where_sql)


# Classes (Toolbox Template).

class Toolbox(object):
    """Define the toolbox.

    Toolbox class is required for constructing and ArcGIS Python toolbox.
    The name of toolbox is the basename of this file.
    """

    def __init__(self):
        self.label = "Toolbox Label"
        # Sets namespace of toolbox when attached to ArcPy (arcpy.{alias}).
        # Attach using arcpy.AddToolbox().
        self.alias = 'alias'
        # List of tool classes associated with this toolbox.
        self.tools = [
            # Add tools here by their class name to make visible in toolbox.
            ToolExample]


class ToolExample(object):
    """Example of an individual tool in an ArcGIS Python toolbox."""

    def __init__(self):
        # Sets how the tool is named within the toolbox.
        self.label = "Label"
        # Sets name of toolset tool will be placed in (optional).
        self.category = None
        # Sets longer description of the tool, shown in the side panel.
        self.description = "Description."
        # Sets whether the tool controls ArcGIS while running or not.
        self.canRunInBackground = False

    def getParameterInfo(self):
        """Load parameters into toolbox."""
        # Create the parameters in a separate place (allows reusability),
        # then add them here. Recommended: use parameter_from_attributes
        # to allow initial definition to be a dictionary/attribute map.
        return []

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        # If tool needs extra licensing, checking here will prevent execution.
        return True

    def updateParameters(self, parameters):
        """Modify parameters before internal validation is performed.

        This method is called whenever a parameter has been changed.
        """
        # Follow the below format for checking for changes. Remove if unused.
        parameter_map = {parameter.name: parameter for parameter in parameters}
        if all([parameter_map['a_parameter'].altered,
                not parameter_map['a_parameter'].hasBeenValidated]):
            # Do something.
            pass
        return

    def updateMessages(self, parameters):
        """Modify messages created by internal validation for each parameter.

        This method is called after internal validation.
        """
        return

    def execute(self, parameters, messages):
        """Procedural code of the tool."""
        # If running ArcPy directly, will need to check-import it.
        global arcpy
        if not arcpy:
            import arcpy
        # value_map contains dictionary with parameter name/value key/values.
        value_map = parameter_value_map(parameters)
        # If using ArcWorkspace directly, will need to instance it.
        workspace = ArcWorkspace()
        # If using ArcETL directly, will need to instance it.
        # Best instanced as context manager.
        with ArcETL() as etl:
            # Do the steps of the tool.
            messages.addMessage("Can do messages, too.")
        return


# Functions & generators.

def log_line(line_type, line, level='info'):
    """Log a line in formatted as expected for the type."""
    if not level:
        return
    if line_type == 'start':
        getattr(logger, level)("Start: {}".format(line))
    elif line_type == 'end':
        getattr(logger, level)("End: {}.".format(line.split()[0]))
    elif line_type == 'feature_count':
        getattr(logger, level)("Feature count: {}.".format(line))
    else:
        raise ValueError("Invalid line_type: {}".format(line_type))
    return


def parameter_from_attributes(attribute_map):
    """Create ArcPy parameter object using an attribute mapping.

    Note that this doesn't check if the attribute exists in the default
    parameter instance. This means that you can attempt to set a new attribute,
    but the result will depend on how the class implements setattr (usually
    this will just attach the new attribute).
    """
    global arcpy
    if not arcpy:
        import arcpy
    parameter = arcpy.Parameter()
    for attribute_name, attribute_value in attribute_map.items():
        setattr(parameter, attribute_name, attribute_value)
    # Filter list doesn't stick using setattr.
    if 'filter.list' in attribute_map:
        parameter.filter.list = attribute_map['filter.list']
    return parameter


def parameter_value_map(parameters):
    """Create value map from list of ArcPy parameter objects."""
    value_map = {}
    for parameter in parameters:
        # Multivalue parameters place their values in .values, not .value.
        # Some values embedded in 'value object' (.value.value), others aren't.
        if parameter.multiValue:
            parameter_values = []
            if parameter.values:
                for value in parameter.values:
                    try:
                        if value.value:
                            parameter_values.append(value.value)
                    except AttributeError:
                        if value:
                            parameter_values.append(value)
            value_map[parameter.name] = parameter_values
        else:
            try:
                value_map[parameter.name] = parameter.value.value
            except AttributeError:
                value_map[parameter.name] = parameter.value
    return value_map


def sexagesimal_angle_to_decimal(degrees, minutes=0, seconds=0, thirds=0,
                                 fourths=0):
    """Convert sexagesimal-parsed angles to a decimal."""
    if degrees is None:
        return None
    # The degrees must be absolute or it won't sum right with subdivisions.
    absolute_decimal = abs(float(degrees))
    try:
        sign_multiplier = abs(float(degrees))/float(degrees)
    except ZeroDivisionError:
        sign_multiplier = 1
    if minutes:
        absolute_decimal += float(minutes)/60
    if seconds:
        absolute_decimal += float(seconds)/3600
    if thirds:
        absolute_decimal += float(thirds)/216000
    if fourths:
        absolute_decimal += float(fourths)/12960000
    return absolute_decimal * sign_multiplier


def unique_ids(data_type=uuid.UUID, string_length=4):
    """Generator for unique IDs."""
    if data_type in (float, int):
        unique_id = data_type()
        while True:
            yield unique_id
            unique_id += 1
    elif data_type in [uuid.UUID]:
        while True:
            yield uuid.uuid4()
    elif data_type in [str]:
        used_ids = set()
        while True:
            unique_id = str(uuid.uuid4())[:string_length]
            while unique_id in used_ids:
                unique_id = str(uuid.uuid4())[:string_length]
            yield unique_id
    else:
        raise NotImplementedError("Unique IDs for type {} not implemented.")


def unique_name(prefix='', suffix='', unique_length=4):
    """Generate unique name."""
    return '{}{}{}'.format(
        prefix,
        next(unique_ids(data_type = str, string_length = unique_length)),
        suffix)


def unique_temp_dataset_path(prefix='', suffix='', unique_length=4,
                             workspace='in_memory'):
    """Create unique temporary dataset path."""
    return os.path.join(workspace, unique_name(prefix, suffix, unique_length))
