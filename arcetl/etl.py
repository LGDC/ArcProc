# -*- coding=utf-8 -*-
import datetime
import inspect
import logging
import os
import random
import string

arcpy = None  # Lazy import.


logger = logging.getLogger(__name__)

# ETL classes.

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
        logger.debug("Called {}".format(debug_call()))
        # Clear the transform dataset.
        if (self.transform_path
            and self.workspace.is_valid_dataset(self.transform_path)):
            self.workspace.delete_dataset(
                self.transform_path, info_log = False
                )
            self.transform_path = None

    def extract(self, extract_path, extract_where_sql=None, schema_only=False):
        """Extract features to transform workspace."""
        logger.debug("Called {}".format(debug_call()))
        logger.info("Start: Extract {}.".format(extract_path))
        # Extract to a new dataset.
        self.transform_path = self.workspace.copy_dataset(
            extract_path, memory_path(), extract_where_sql, schema_only,
            info_log = False
            )
        logger.info("End: Extract.")
        return self.transform_path

    def load(self, load_path, load_where_sql=None, preserve_features=False):
        """Load features from transform workspace to the load-dataset."""
        logger.debug("Called {}".format(debug_call()))
        logger.info("Start: Load {}.".format(load_path))
        if self.workspace.is_valid_dataset(load_path):
            # Load to an existing dataset.
            # Unless preserving features, initialize the target dataset.
            if not preserve_features:
                self.workspace.delete_features(load_path, info_log = False)
            self.workspace.insert_features_from_path(
                load_path, self.transform_path, load_where_sql,
                info_log = False
                )
        else:
            # Load to a new dataset.
            self.workspace.copy_dataset(self.transform_path, load_path,
                                        load_where_sql, info_log = False)
        logger.info("End: Load.")
        return load_path

    def make_asssertion(self, assertion_name, **kwargs):
        """Check whether an assertion is valid or not."""
        logger.debug("Called {}".format(debug_call()))
        raise NotImplementedError

    def transform(self, transform_name, **kwargs):
        """Run transform operation as defined in the workspace."""
        logger.debug("Called {}".format(debug_call()))
        transform = getattr(self.workspace, transform_name)
        # Unless otherwise stated, dataset path is self.transform path.
        if 'dataset_path' not in kwargs:
            kwargs['dataset_path'] = self.transform_path
        # If arguments include output_path, supersede old transform path.
        if 'output_path' in inspect.getargspec(transform).args:
            kwargs['output_path'] = memory_path()
        result = transform(**kwargs)
        if 'output_path' in kwargs:
            # Remove old transform_path (if extant).
            if self.workspace.is_valid_dataset(self.transform_path):
                self.workspace.delete_dataset(
                    self.transform_path, info_log = False
                    )
            # Replace with output_path.
            self.transform_path = result
        return result


class ArcWorkspace(object):
    """Manages an Arc-style workspace with built-in operations."""

    def __init__(self, path=None):
        global arcpy
        if not arcpy:
            import arcpy
        self.path = path if path else os.getcwd()
        # Set arcpy workspace for tools that require it. Otherwise, avoid implied paths.
        arcpy.env.workspace = self.path
        logger.info("Initialized ArcWorkspace instance based on {}.".format(path))

    # General execution methods.

    def execute_sql_statement(self, statement, path_to_database=None, info_log=True):
        """Runs a SQL statement via SDE's SQL execution interface.

        This only works if path resolves to an actual SQL database.
        """
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info("Start: Execute SQL statement.")
        conn = arcpy.ArcSDESQLExecute(path_to_database if path_to_database else self.path)
        try:
            result = conn.execute(statement)
        except AttributeError:
            logger.exception("Incorrect SQL syntax.")
            raise
        del conn  # Yeah, what can you do?
        if info_log:
            logger.info("End: Execute.")
        return result

    # Metadata/property methods.

    def dataset_metadata(self, dataset_path):
        """Return dictionary of dataset's metadata."""
        logger.debug("Called {}".format(debug_call()))
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
            metadata['field_names'] = [field.name for field in arc_description.fields]
            metadata['fields'] = []
            for field in arc_description.fields:
                field_metadata = {
                    'name': field.name,
                    'alias_name': field.aliasName, 'base_name': field.baseName,
                    'type': field.type.lower(), 'length': field.length,
                    'precision': field.precision, 'scale': field.scale,
                    # Leaving out these since they're not necessary for ETL (& often problematic).
                    #'default_value': field.defaultValue, 'is_required': field.required,
                    #'is_editable': field.editable, 'is_nullable': field.isNullable,
                    }
                metadata['fields'].append(field_metadata)
        metadata['is_spatial'] = hasattr(arc_description, 'shapeType')
        if metadata['is_spatial']:
            metadata['geometry_type'] = arc_description.shapeType.lower()
            metadata['spatial_reference_id'] = arc_description.spatialReference.factoryCode
            metadata['geometry_field_name'] = arc_description.shapeFieldName
        return metadata

    def feature_count(self, dataset_path, dataset_where_sql=None):
        """Return the number of features in a dataset."""
        logger.debug("Called {}".format(debug_call()))
        with arcpy.da.SearchCursor(in_table = dataset_path,
                                   field_names = [arcpy.ListFields(dataset_path)[0].name],
                                   where_clause = dataset_where_sql) as cursor:
            feature_count = len([None for row in cursor])
        return feature_count

    def field_metadata(self, dataset_path, field_name):
        """Return dictionary of field's info."""
        logger.debug("Called {}".format(debug_call()))
        try:
            metadata = [
                field for field
                in self.dataset_metadata(dataset_path)['fields']
                if field['name'].lower() == field_name.lower()
                ][0]
        except IndexError:
            raise AttributeError("{} not present on {}".format(field_name, dataset_path))
        return metadata

    def is_valid_dataset(self, dataset_path):
        """Check whether a dataset exists/is valid."""
        logger.debug("Called {}".format(debug_call()))
        if dataset_path and arcpy.Exists(dataset_path):
            return self.dataset_metadata(dataset_path)['is_table']
        else:
            return False

    def workspace_dataset_names(self, workspace_path=None, wildcard=None,
                                include_feature_classes=True,
                                include_rasters=True, include_tables=True,
                                include_feature_datasets=True):
        """Return list of names of workspace's datasets.

        wildcard requires an * to indicate where open; case insensitive.
        """
        logger.debug("Called {}".format(debug_call()))
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

    def compress_geodatabase(self, geodatabase_path=None, disconnect_users=False,
                             info_log=True):
        """Compress geodatabase."""
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info("Start: Compress {}.".format(geodatabase_path))
        if not geodatabase_path:
            geodatabase_path = self.path
        arc_description = arcpy.Describe(geodatabase_path)
        if hasattr(arc_description, 'workspaceType'):
            workspace_type = arc_description.workspaceType
        else:
            workspace_type = None
        if workspace_type == 'LocalDatabase':
            # Local databases cannot disconnect users (connections managed by
            # file access).
            disconnect_users = False
            compress = arcpy.management.CompressFileGeodatabaseData
        elif workspace_type == 'RemoteDatabase':
            compress = arcpy.management.Compress
        else:
            raise ValueError(
                "{} not a valid geodatabase.".format(geodatabase_path)
                )
        if disconnect_users:
            arcpy.AcceptConnections(
                sde_workspace = geodatabase_path, accept_connections = False
                )
            arcpy.DisconnectUser(
                sde_workspace = geodatabase_path, users = 'all'
                )
        compress(geodatabase_path)
        if disconnect_users:
            arcpy.AcceptConnections(
                sde_workspace = geodatabase_path, accept_connections = True
                )
        if info_log:
            logger.info("End: Compress.")
        return geodatabase_path

    def copy_dataset(self, dataset_path, output_path, dataset_where_sql=None,
                     schema_only=False, overwrite=False, info_log=True):
        """Copy dataset."""
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info("Start: Copy {} to {}.".format(dataset_path, output_path))
        metadata = self.dataset_metadata(dataset_path)
        if metadata['is_spatial']:
            create_view = arcpy.management.MakeFeatureLayer
            copy_rows = arcpy.management.CopyFeatures
        elif  metadata['is_table']:
            create_view = arcpy.management.MakeTableView
            copy_rows = arcpy.management.CopyRows
        else:
            raise ValueError("{} unsupported dataset type.".format(dataset_path))
        if overwrite and self.is_valid_dataset(output_path):
            self.delete_dataset(output_path, info_log = False)
        view_name = random_string()
        create_view(dataset_path, view_name,
                    dataset_where_sql if not schema_only else "0 = 1", self.path)
        copy_rows(view_name, output_path)
        self.delete_dataset(view_name, info_log = False)
        if info_log:
            logger.info("End: Copy.")
        return output_path

    def create_dataset(self, dataset_path, field_metadata=[],
                       geometry_type=None, spatial_reference_id=None,
                       info_log=True):
        """Create new dataset."""
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info("Start: Create dataset {}.".format(dataset_path))
        if geometry_type:
            if spatial_reference_id:
                spatial_reference = arcpy.SpatialReference(
                    spatial_reference_id
                    )
            else:
                spatial_reference = arcpy.SpatialReference(4326)
            arcpy.management.CreateFeatureclass(
                out_path = os.path.dirname(dataset_path),
                out_name = os.path.basename(dataset_path),
                geometry_type = geometry_type,
                spatial_reference = spatial_reference
                )
        else:
            arcpy.management.CreateTable(
                out_path = os.path.dirname(dataset_path),
                out_name = os.path.basename(dataset_path)
                )
        if field_metadata:
            if isinstance(field_metadata, dict):
                field_metadata = [field_metadata]
            for field in field_metadata:
                self.add_field(
                    dataset_path, field['name'], field['type'],
                    field.get('length'), field.get('precision'),
                    field.get('scale'), info_log = False
                    )
        return dataset_path

    def create_file_geodatabase(self, geodatabase_path, info_log=True):
        """Create new file geodatabase."""
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info("Start: Create file geodatabase at {}.".format(
                geodatabase_path
                ))
        if os.path.exists(geodatabase_path):
            logger.warning("Geodatabase already exists.")
        else:
            arcpy.management.CreateFileGDB(
                out_folder_path = os.path.dirname(geodatabase_path),
                out_name = os.path.basename(geodatabase_path),
                out_version = 'current'
                )
        if info_log:
            logger.info("End: Create.")
        return geodatabase_path

    def create_geodatabase_xml_backup(self,geodatabase_path, output_path,
                                      include_data=False,
                                      include_metadata=True, info_log=True):
        """Create backup of geodatabase as XML workspace document."""
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info(
                "Start: Create backup for {} in {}.".format(
                    geodatabase_path, output_path
                    )
                )
        arcpy.management.ExportXMLWorkspaceDocument(
            in_data = geodatabase_path, out_file = output_path,
            export_type = 'data' if include_data else 'schema_only',
            storage_type = 'binary', export_metadata = include_metadata
            )
        if info_log:
            logger.info("End: Create.")
        return output_path

    def delete_dataset(self, dataset_path, info_log=True):
        """Delete dataset."""
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info("Start: Delete {}.".format(dataset_path))
        arcpy.management.Delete(dataset_path)
        if info_log:
            logger.info("End: Delete.")
        return dataset_path

    # Schema alteration methods.

    def add_field(self, dataset_path, field_name, field_type, field_length=None,
                  field_precision=None, field_scale=None, field_is_nullable=True,
                  field_is_required=False, info_log=True):
        """Add field to dataset."""
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info("Start: Add field {} to {}.".format(field_name, dataset_path))
        if field_type.lower() == 'string':
            field_type = 'text'
        elif field_type.lower() == 'integer':
            field_type = 'long'
        if field_type.lower() == 'text' and field_length is None:
            field_length = 64
        arcpy.management.AddField(
            dataset_path, field_name, field_type = field_type,
            field_length = field_length, field_precision = field_precision,
            field_scale = field_scale, field_is_nullable = field_is_nullable,
            field_is_required = field_is_required
            )
        if info_log:
            logger.info("End: Add.")
        return field_name

    def add_fields_from_metadata_list(self, dataset_path, metadata_list, info_log=True):
        """Add fields to dataset from a list of metadata dictionaries."""
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info("Start: Add fields to {} from a metadata list.".format(dataset_path))
        for field_info in metadata_list:
            kwargs = {}
            for key, value in field_info.items():
                if 'field_{}'.format(key) in inspect.getargspec(self.add_field).args:
                    kwargs['field_{}'.format(key)] = value
            field_name = self.add_field(dataset_path = dataset_path, info_log=False, **kwargs)
            if info_log:
                logger.info("Added {}.".format(field_name))
        if info_log:
            logger.info("End: Add.")
        return [field_info['name'] for field_info in metadata_list]

    def add_index(self, dataset_path, field_names, index_name=None,
                  is_unique=False, is_ascending=False, info_log=True):
        """Add index to dataset fields."""
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info("Start: Add index to {} for {}.".format(
                dataset_path, field_names
                ))
        field_types = {
            field['type'].lower() for field
            in self.dataset_metadata(dataset_path)['fields']
            if field['name'].lower() in [name.lower() for name in field_names]
            }
        if 'geometry' in field_types:
            if len(field_names) != 1:
                raise RuntimeError("Cannot create a composite spatial index.")
                arcpy.management.AddSpatialIndex(dataset_path)
        else:
            index_name = '_'.join(['ndx'] + field_names)
            arcpy.management.AddIndex(
                dataset_path, field_names, index_name, is_unique, is_ascending
                )
        if info_log:
            logger.info("End: Add.")
        return dataset_path

    def rename_field(self, dataset_path, field_name, new_field_name,
                     info_log=True):
        """Rename field."""
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info("Start: Rename field {}.{} to {}.".format(
                dataset_path, field_name, new_field_name
                ))
        arcpy.management.AlterField(dataset_path, field_name, new_field_name)
        if info_log:
            logger.info("End: Rename.")
        return new_field_name

    def delete_field(self, dataset_path, field_name, info_log=True):
        """Delete field from dataset."""
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info("Start: Delete field {}.".format(field_name))
        arcpy.management.DeleteField(in_table = dataset_path, drop_field = field_name)
        if info_log:
            logger.info("End: Delete.")
        return field_name

    def join_field(self, dataset_path, join_dataset_path, join_field_name,
                   on_field_name, on_join_field_name, info_log=True):
        """Add field and its values from join-dataset."""
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info(
                "Start: Join field {} from {}.".format(
                    join_field_name, join_dataset_path
                    )
                )
        arcpy.management.JoinField(
            dataset_path, in_field = on_field_name,
            join_table = join_dataset_path, join_field = on_join_field_name,
            fields = join_field_name
            )
        if info_log:
            logger.info("End: Join.")
        return join_field_name

    # Feature alteration methods.

    def adjust_features_for_shapefile(self, dataset_path,
                                      datetime_null_replacement=datetime.date.min,
                                      integer_null_replacement=0,
                                      numeric_null_replacement=0.0,
                                      string_null_replacement='',
                                      info_log=True):
        """Adjust features to meet shapefile requirements.

        Adjustments currently made:
        * Convert datetime values to date or time based on
        preserve_time_not_date flag.
        * Convert nulls to replacement value.
        """
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info(
                ("Start: Adjust features in {}"
                 " to meet shapefile requirements.").format(dataset_path)
                )
        dataset = self.dataset_metadata(dataset_path)
        type_function_map = {
            # Blob omitted: Not a valid shapefile type.
            'date': (lambda x: datetime_null_replacement if x is None
                     # Shapefiles can only store dates, not times.
                     else x.date()),
            'double': lambda x: 0.0 if x is None else x,
            # Geometry passed-through: Shapefile loader handles this.
            #'guid': Not valid shapefile type.
            'integer': lambda x: 0 if x is None else x,
            # OID passed-through: Shapefile loader handles this.
            # Raster omitted: Not a valid shapefile type.
            'single': lambda x: 0.0 if x is None else x,
            'smallinteger': lambda x: 0 if x is None else x,
            'string': lambda x: '' if x is None else x,
            }
        for field in dataset['fields']:
            if field['type'].lower() in type_function_map:
                self.update_field_by_function(
                    dataset_path, field['name'],
                    function = type_function_map[field['type']],
                    info_log = False
                    )
        if info_log:
            logger.info("End: Adjust.")
        return dataset_path

    def clip_features(self, dataset_path, clip_dataset_path,
                      dataset_where_sql=None, clip_where_sql=None,
                      info_log=True):
        """Clip feature geometry where overlaps clip dataset geometry."""
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info("Start: Clip {} where geometry overlaps {}.".format(
                    dataset_path, clip_dataset_path
                    ))
            logger.info("Initial feature count: {}.".format(
                self.feature_count(dataset_path)
                ))
        else:
            logger.debug("Initial feature count: {}.".format(
                self.feature_count(dataset_path)
                ))
        view_name = random_string()
        arcpy.management.MakeFeatureLayer(
            dataset_path, view_name, dataset_where_sql, self.path
            )
        clip_view_name = random_string()
        arcpy.management.MakeFeatureLayer(
            clip_dataset_path, clip_view_name, clip_where_sql, self.path
            )
        temp_output_path = memory_path()
        arcpy.analysis.Clip(
            in_features = view_name, clip_features = clip_view_name,
            out_feature_class = temp_output_path
            )
        self.delete_dataset(clip_view_name, info_log = False)
        # Load back into the dataset.
        self.delete_features(view_name, info_log = False)
        self.delete_dataset(view_name, info_log = False)
        self.insert_features_from_path(
            dataset_path, temp_output_path, info_log = False
            )
        self.delete_dataset(temp_output_path, info_log = False)
        if info_log:
            logger.info("Final feature count: {}.".format(
                self.feature_count(dataset_path)
                ))
            logger.info("End: Clip.")
        else:
            logger.debug("Final feature count: {}.".format(
                self.feature_count(dataset_path)
                ))
        return dataset_path

    def convert_polygons_to_lines(self, dataset_path, output_path,
                                  topological=False, id_field_name=None,
                                  info_log=True):
        """Convert geometry from polygons to lines.

        If topological is set to True, shared outlines will be a single,
        separate feature. Note that one cannot pass attributes to a topological
        transformation (as the values would not apply to all adjacent
        features).

        If an id field name is specified, the output dataset will identify the
        input features that defined the line feature with the name & values
        from the provided field. This option will be ignored if the output is
        non-topological lines, as the field will pass over with the rest of the
        attributes.
        """
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info(
                "Start: Convert polygon features in {} to lines.".format(
                    dataset_path)
                )
            logger.info("Initial feature count: {}.".format(
                self.feature_count(dataset_path)
                ))
        else:
            logger.debug("Initial feature count: {}.".format(
                self.feature_count(dataset_path)
                ))
        arcpy.management.PolygonToLine(
            in_features = dataset_path, out_feature_class = output_path,
            neighbor_option = topological
            )
        if topological:
            if id_field_name:
                id_field_info = self.field_metadata(dataset_path,
                                                    id_field_name)
                oid_field_name = (
                    self.dataset_metadata(dataset_path)['oid_field_name']
                    )
            sides = ('left', 'right')
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
                        output_path, [side_id_field_info], info_log = False
                        )
                    self.update_field_by_join_value(
                        dataset_path = output_path,
                        field_name = side_id_field_info['name'],
                        join_dataset_path = dataset_path,
                        join_field_name = id_field_name,
                        on_field_name = side_oid_field_name,
                        on_join_field_name = oid_field_name, info_log = False
                        )
                self.delete_field(output_path, side_oid_field_name,
                                  info_log = False)
        else:
            self.delete_field(output_path, 'ORIG_FID', info_log = False)
        if info_log:
            logger.info("Final feature count: {}.".format(
                self.feature_count(dataset_path)
                ))
            logger.info("End: Convert.")
        else:
            logger.debug("Final feature count: {}.".format(
                self.feature_count(dataset_path)
                ))
        return output_path

    def delete_features(self, dataset_path, dataset_where_sql=None,
                        info_log=True):
        """Delete select features."""
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info(
                "Start: Delete features from {} where {}.".format(
                    dataset_path, dataset_where_sql
                    )
                )
        metadata = self.dataset_metadata(dataset_path)
        # Database-type (also not in-memory) & no sub-selection: use truncate.
        if (metadata['data_type'] in ['FeatureClass', 'Table']
            and metadata['workspace_path'] != 'in_memory'
            and dataset_where_sql is None):
            arcpy.management.TruncateTable(dataset_path)
        # Non-database or with sub-selection options.
        else:
            if metadata['is_spatial']:
                create_view = arcpy.management.MakeFeatureLayer
                delete_rows = arcpy.management.DeleteFeatures
            elif metadata['is_table']:
                create_view = arcpy.management.MakeTableView
                delete_rows = arcpy.management.DeleteRows
            else:
                raise ValueError("{} unsupported dataset type.".format(dataset_path))
            view_name = random_string()
            create_view(dataset_path, view_name, dataset_where_sql, self.path)
            delete_rows(view_name)
            self.delete_dataset(view_name, info_log = False)
        if info_log:
            logger.info("End: Delete.")
        return dataset_path

    def dissolve_features(self, dataset_path, dissolve_field_names,
                          multipart=True, unsplit_lines=False,
                          dataset_where_sql=None, info_log=True):
        """Merge features that share values in given fields."""
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info(
                "Start: Dissolve features on {}.".format(dissolve_field_names)
                )
            logger.info(
                "Initial feature count: {}.".format(
                    self.feature_count(dataset_path)
                    )
                )
        else:
            logger.debug(
                "Initial feature count: {}.".format(
                    self.feature_count(dataset_path)
                    )
                )
        # Set the environment tolerance, so we can be sure the in_memory
        # datasets respect it. 0.003280839895013 is the default for all
        # datasets in our geodatabases.
        arcpy.env.XYTolerance = 0.003280839895013
        view_name = random_string()
        arcpy.management.MakeFeatureLayer(
            dataset_path, view_name, dataset_where_sql, self.path
            )
        temp_dissolved_path = memory_path()
        arcpy.management.Dissolve(
            in_features = view_name, out_feature_class = temp_dissolved_path,
            dissolve_field = dissolve_field_names, multi_part = multipart,
            unsplit_lines = unsplit_lines
            )
        # Delete undissolved features that are now dissolved (in the temp).
        self.delete_features(view_name, info_log = False)
        self.delete_dataset(view_name, info_log = False)
        # Copy the dissolved features (in the temp) to the dataset.
        self.insert_features_from_path(
            dataset_path, temp_dissolved_path, info_log = False
            )
        self.delete_dataset(temp_dissolved_path, info_log = False)
        if info_log:
            logger.info(
                "Final feature count: {}.".format(
                    self.feature_count(dataset_path)
                    )
                )
            logger.info("End: Dissolve.")
        else:
            logger.debug(
                "Final feature count: {}.".format(
                    self.feature_count(dataset_path)
                    )
                )
        return dataset_path

    def erase_features(self, dataset_path, erase_dataset_path,
                       dataset_where_sql=None, erase_where_sql=None,
                       info_log=True):
        """Erase feature geometry where overlaps erase dataset geometry."""
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info("Start: Erase {} where geometry overlaps {}.".format(
                    dataset_path, erase_dataset_path
                    ))
            logger.info("Initial feature count: {}.".format(
                self.feature_count(dataset_path)
                ))
        else:
            logger.debug("Initial feature count: {}.".format(
                self.feature_count(dataset_path)
                ))
        view_name = random_string()
        arcpy.management.MakeFeatureLayer(
            dataset_path, view_name, dataset_where_sql, self.path
            )
        erase_view_name = random_string()
        arcpy.management.MakeFeatureLayer(
            erase_dataset_path, erase_view_name, erase_where_sql, self.path
            )
        temp_output_path = memory_path()
        arcpy.analysis.Erase(
            in_features = view_name, erase_features = erase_view_name,
            out_feature_class = temp_output_path
            )
        self.delete_dataset(erase_view_name, info_log = False)
        # Load back into the dataset.
        self.delete_features(view_name, info_log = False)
        self.delete_dataset(view_name, info_log = False)
        self.insert_features_from_path(
            dataset_path, temp_output_path, info_log = False
            )
        self.delete_dataset(temp_output_path, info_log = False)
        if info_log:
            logger.info("Final feature count: {}.".format(
                self.feature_count(dataset_path)
                ))
            logger.info("End: Erase.")
        else:
            logger.debug("Final feature count: {}.".format(
                self.feature_count(dataset_path)
                ))
        return dataset_path

    def keep_features_by_location(self, dataset_path, location_path,
                                  dataset_where_sql=None,
                                  location_where_sql=None, info_log=True):
        """Keep features where geometry overlaps location feature geometry."""
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info("Start: Keep {} where geometry overlaps {}.".format(
                    dataset_path, location_path
                    ))
            logger.info("Initial feature count: {}.".format(
                self.feature_count(dataset_path)
                ))
        else:
            logger.debug("Initial feature count: {}.".format(
                self.feature_count(dataset_path)
                ))
        view_name = random_string()
        arcpy.management.MakeFeatureLayer(
            dataset_path, view_name, dataset_where_sql, self.path
            )
        location_view_name = random_string()
        arcpy.management.MakeFeatureLayer(
            location_path, location_view_name, location_where_sql, self.path
            )
        arcpy.management.SelectLayerByLocation(
            in_layer = view_name, overlap_type = 'intersect',
            select_features = location_view_name,
            selection_type = 'new_selection'
            )
        self.delete_dataset(location_view_name, info_log = False)
        # Switch selection & delete features not overlapping location.
        arcpy.management.SelectLayerByLocation(
            in_layer = view_name, selection_type = 'switch_selection'
            )
        self.delete_features(view_name, info_log = False)
        self.delete_dataset(view_name, info_log = False)
        if info_log:
            logger.info("Final feature count: {}.".format(
                self.feature_count(dataset_path)
                ))
            logger.info("End: Keep.")
        else:
            logger.debug("Final feature count: {}.".format(
                self.feature_count(dataset_path)
                ))
        return dataset_path

    def identity_features(self, dataset_path, field_name, identity_dataset_path,
                          identity_field_name, replacement_value=None,
                          dataset_where_sql=None, chunk_size=4096,
                          info_log=True):
        """Assign unique identity value to features, splitting where necessary.

        replacement_value is a value that will substitute as the identity
        value.
        This method has a 'chunking' routine in order to avoid an
        unhelpful output error that occurs when the inputs are rather large.
        For some reason, the identity will 'succeed' with and empty output
        warning, but not create an output dataset. Running the identity against
        smaller sets of data generally avoids this conundrum.
        """
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info("Start: Identity features with {}.{}.".format(identity_dataset_path,
                                                                      identity_field_name))
            logger.info("Initial feature count: {}.".format(self.feature_count(dataset_path)))
        else:
            logger.debug("Initial feature count: {}.".format(self.feature_count(dataset_path)))
        # Create a temporary copy of the identity dataset.
        temp_identity_path = self.copy_dataset(identity_dataset_path, memory_path(),
                                               info_log = False)
        # Create neutral/unique field for holding identity value (avoids collisions).
        temp_field_metadata = self.field_metadata(temp_identity_path, identity_field_name)
        temp_field_metadata['name'] = random_string()
        # Cannot add OID-type field, so push to a long-type.
        if temp_field_metadata['type'].lower() == 'oid':
            temp_field_metadata['type'] = 'long'
        self.add_fields_from_metadata_list(temp_identity_path, [temp_field_metadata],
                                           info_log = False)
        self.update_field_by_expression(temp_identity_path, temp_field_metadata['name'],
                                        expression = '!{}!'.format(identity_field_name),
                                        info_log = False)
        # Get an iterable of all object IDs in the dataset.
        with arcpy.da.SearchCursor(dataset_path, ['oid@'], dataset_where_sql) as cursor:
            # Sorting is important, allows us to create view with ID range instead of list.
            objectids = sorted((row[0] for row in cursor))
        while objectids:
            chunk_objectids = objectids[:chunk_size]
            objectids = objectids[chunk_size:]
            from_objectid, to_objectid = chunk_objectids[0], chunk_objectids[-1]
            logger.debug("Chunk: Feature object IDs {} to {}".format(from_objectid, to_objectid))
            # Create the temp output of the identity.
            view_name = random_string()
            view_where_clause = "{0} >= {1} and {0} <= {2}".format(
                    self.dataset_metadata(dataset_path)['oid_field_name'],
                    from_objectid, to_objectid
                    )
            if dataset_where_sql:
                view_where_clause += " and ({})".dataset_where_sql
            view_where_clause
            arcpy.management.MakeFeatureLayer(
                in_features = dataset_path, out_layer = view_name,
                # ArcPy where clauses cannot use 'between'.
                where_clause = view_where_clause, workspace = self.path
                )
            # Create temporary dataset with the identity values.
            temp_output_path = memory_path()
            arcpy.analysis.Identity(
                in_features = view_name, identity_features = temp_identity_path,
                out_feature_class = temp_output_path, join_attributes = 'all', relationship = False
                )
            # Push the identity (or replacement) value from the temp field to the update field.
            if replacement_value:
                expression = '{} if !{}! else None'.format(repr(replacement_value),
                                                           temp_field_metadata['name'])
            else:
                # Identity puts empty string identity feature not present. Fix to null.
                expression = "!{0}! if !{0}! else None".format(temp_field_metadata['name'])
            self.update_field_by_expression(
                temp_output_path, field_name, expression, info_log = False
                )
            # Replace original chunk features with identity features.
            self.delete_features(view_name, info_log = False)
            self.delete_dataset(view_name, info_log = False)
            self.insert_features_from_path(dataset_path, temp_output_path, info_log = False)
            self.delete_dataset(temp_output_path, info_log = False)
        self.delete_dataset(temp_identity_path, info_log = False)
        if info_log:
            logger.info("Final feature count: {}.".format(self.feature_count(dataset_path)))
            logger.info("End: Identity.")
        else:
            logger.debug("Final feature count: {}.".format(self.feature_count(dataset_path)))
        return dataset_path

    def insert_features_from_iterables(self, dataset_path,
                                       insert_dataset_iterables, field_names,
                                       info_log=True):
        """Insert features from a collection of iterables."""
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info(" ".join([
                "Start: Insert features into {}".format(dataset_path),
                "from a collection of iterables."
                ]))
        # Create generator if insert_dataset_iterables is a generator function.
        if inspect.isgeneratorfunction(insert_dataset_iterables):
            insert_dataset_iterables = insert_dataset_iterables()
        with arcpy.da.InsertCursor(dataset_path, field_names) as cursor:
            for row in insert_dataset_iterables:
                cursor.insertRow(row)
        if info_log:
            logger.info("End: Insert.")
        return dataset_path

    def insert_features_from_path(self, dataset_path, insert_dataset_path,
                                  insert_where_sql=None, info_log=True):
        """Insert features from a dataset referred to by a system path."""
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info(
                "Start: Insert features to {} from {}.".format(
                    dataset_path, insert_dataset_path
                    )
                )
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
                    dataset_metadata[field_name_type].lower()
                    )
                insert_dataset_field_names.remove(
                    insert_dataset_metadata[field_name_type].lower()
                    )
        field_maps = arcpy.FieldMappings()
        for field_name in dataset_field_names:
            if field_name in insert_dataset_field_names:
                field_map = arcpy.FieldMap()
                field_map.addInputField(insert_dataset_path, field_name)
                field_maps.addFieldMap(field_map)
        insert_dataset_metadata = self.dataset_metadata(insert_dataset_path)
        if dataset_metadata['is_spatial']:
            create_view = arcpy.management.MakeFeatureLayer
        elif dataset_metadata['is_table']:
            create_view = arcpy.management.MakeTableView
        else:
            raise ValueError(
                "{} unsupported dataset type.".format(dataset_path)
                )
        insert_view_name = random_string()
        create_view(insert_dataset_path, insert_view_name,
                    insert_where_sql, self.path)
        arcpy.management.Append(inputs = insert_view_name,
                                target = dataset_path, schema_type = 'no_test',
                                field_mapping = field_maps)
        self.delete_dataset(insert_view_name, info_log = False)
        if info_log:
            logger.info("End: Insert.")
        return dataset_path

    def overlay_features(self, dataset_path, field_name, overlay_dataset_path,
                         overlay_field_name, replacement_value=None,
                         overlay_most_coincident=False,
                         overlay_central_coincident=False,
                         dataset_where_sql=None, chunk_size=4096,
                         info_log=True):
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
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info("Start: Overlay features with {}.{}.".format(
                overlay_dataset_path, overlay_field_name
                ))
            logger.info("Initial feature count: {}.".format(
                self.feature_count(dataset_path)
                ))
        else:
            logger.debug("Initial feature count: {}.".format(
                self.feature_count(dataset_path)
                ))
        # Check flags & set details for spatial join call.
        if overlay_most_coincident:
            raise NotImplementedError(
                "overlay_most_coincident is not yet implemented."
                )
        elif overlay_central_coincident:
            join_operation = 'join_one_to_many'
            match_option = 'have_their_center_in'
        else:
            join_operation = 'join_one_to_many'
            match_option = 'intersect'
        # Create a temporary copy of the overlay dataset.
        temp_overlay_path = self.copy_dataset(
            overlay_dataset_path, memory_path(), info_log = False
            )
        # Create neutral field for holding overlay value (avoids collisions).
        temp_field_metadata = self.field_metadata(
            temp_overlay_path, overlay_field_name
            )
        temp_field_metadata['name'] = random_string()
        # Cannot add OID-type field, so push to a long-type.
        if temp_field_metadata['type'].lower() == 'oid':
            temp_field_metadata['type'] = 'long'
        self.add_fields_from_metadata_list(
            temp_overlay_path, [temp_field_metadata], info_log = False
            )
        self.update_field_by_expression(
            temp_overlay_path, temp_field_metadata['name'],
            expression = '!{}!'.format(overlay_field_name), info_log = False
            )
        # Get an iterable of all object IDs in the dataset.
        with arcpy.da.SearchCursor(
            dataset_path, ['oid@'], dataset_where_sql
            ) as cursor:
            # Sorting is important, allows us to create view with ID range
            # instead of list.
            objectids = sorted((row[0] for row in cursor))
        while objectids:
            chunk_objectids = objectids[:chunk_size]
            objectids = objectids[chunk_size:]
            from_objectid, to_objectid = (chunk_objectids[0],
                                          chunk_objectids[-1])
            logger.debug("Chunk: Feature object IDs {} to {}".format(
                from_objectid, to_objectid
                ))
            # Create the temp output of the overlay.
            view_name = random_string()
            view_where_clause = "{0} >= {1} and {0} <= {2}".format(
                    self.dataset_metadata(dataset_path)['oid_field_name'],
                    from_objectid, to_objectid
                    )
            if dataset_where_sql:
                view_where_clause += " and ({})".dataset_where_sql
            arcpy.management.MakeFeatureLayer(
                in_features = dataset_path, out_layer = view_name,
                # ArcPy where clauses cannot use 'between'.
                where_clause = view_where_clause, workspace = self.path
                )
            temp_output_path = memory_path()
            arcpy.analysis.SpatialJoin(
                target_features = view_name, join_features = temp_overlay_path,
                out_feature_class = temp_output_path,
                join_operation = join_operation, join_type = 'keep_all',
                match_option = match_option
                )
            # Push the overlay (or replacement) value from the temp field to
            # the update field.
            if replacement_value:
                expression = '{} if !{}! else None'.format(
                    repr(replacement_value), temp_field_metadata['name']
                    )
            else:
                expression = "!{0}!".format(temp_field_metadata['name'])
            self.update_field_by_expression(
                temp_output_path, field_name, expression, info_log = False
                )
            # Replace original chunk features with overlay features.
            self.delete_features(view_name, info_log = False)
            self.delete_dataset(view_name, info_log = False)
            self.insert_features_from_path(
                dataset_path, temp_output_path, info_log = False
                )
            self.delete_dataset(temp_output_path, info_log = False)
        self.delete_dataset(temp_overlay_path, info_log = False)
        if info_log:
            logger.info("Final feature count: {}.".format(
                self.feature_count(dataset_path)
                ))
            logger.info("End: Overlay.")
        else:
            logger.debug("Final feature count: {}.".format(
                self.feature_count(dataset_path)
                ))
        return dataset_path

    def planarize_features(self, dataset_path, output_path, info_log=True):
        """Convert feature geometry to lines - planarizing them.

        This method does not make topological linework. However it does carry
        all attributes with it, rather than just an ID attribute.

        Since this method breaks the new line geometry at intersections, it
        can be useful to break line geometry features at them.
        """
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info(
                "Start: Planarize features in {}.".format(
                    dataset_path)
                )
            logger.info("Initial feature count: {}.".format(
                self.feature_count(dataset_path)
                ))
        else:
            logger.debug("Initial feature count: {}.".format(
                self.feature_count(dataset_path)
                ))
        arcpy.management.FeatureToLine(
            in_features = dataset_path, out_feature_class = output_path,
            ##cluster_tolerance,
            attributes = True
            )
        if info_log:
            logger.info("Final feature count: {}.".format(
                self.feature_count(dataset_path)
                ))
            logger.info("End: Planarize.")
        else:
            logger.debug("Final feature count: {}.".format(
                self.feature_count(dataset_path)
                ))
        return output_path

    def union_features(self, dataset_path, field_name, union_dataset_path, union_field_name,
                       replacement_value=None, dataset_where_sql=None, info_log=True):
        """Assign unique union value to each feature, splitting where necessary.

        replacement_value is a value that will substitute as the union value.
        """
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info("Start: Union features with {}.{}.".format(union_dataset_path,
                                                                   union_field_name))
            logger.info("Initial feature count: {}.".format(self.feature_count(dataset_path)))
        else:
            logger.debug("Initial feature count: {}.".format(self.feature_count(dataset_path)))
        # Create a temporary copy of the union dataset.
        temp_union_path = self.copy_dataset(union_dataset_path, memory_path(), info_log = False)
        # Create neutral/unique field for holding union value (avoids collisions).
        temp_field_metadata = self.field_metadata(temp_union_path, union_field_name)
        temp_field_metadata['name'] = random_string()
        # Cannot add OID-type field, so push to a long-type.
        if temp_field_metadata['type'].lower() == 'oid':
            temp_field_metadata['type'] = 'long'
        self.add_fields_from_metadata_list(temp_union_path, [temp_field_metadata], info_log = False)
        self.update_field_by_expression(temp_union_path, temp_field_metadata['name'],
                                        expression = '!{}!'.format(union_field_name),
                                        info_log = False)
        # Create the temp output of the union.
        view_name = random_string()
        arcpy.management.MakeFeatureLayer(dataset_path, view_name, dataset_where_sql, self.path)
        temp_output_path = memory_path()
        arcpy.analysis.Union(
            in_features = [view_name, temp_union_path], out_feature_class = temp_output_path,
            join_attributes = 'all', gaps = False
            )
        self.delete_dataset(temp_union_path, info_log = False)
        # Union puts empty string instead of null where identity feature not present; fix.
        self.update_field_by_expression(
            temp_output_path, temp_field_metadata['name'],
            expression = "None if !{0}! == '' else !{0}!".format(temp_field_metadata['name']),
            info_log = False
            )
        # Push the union (or replacement) value from the temp field to the update field.
        if replacement_value:
            expression = '{} if !{}! else None'.format(repr(replacement_value),
                                                       temp_field_metadata['name'])
        else:
            expression = '!{}!'.format(temp_field_metadata['name'])
        self.update_field_by_expression(temp_output_path, field_name, expression, info_log = False)
        # Replace original features with union features.
        self.delete_features(view_name, info_log = False)
        self.delete_dataset(view_name, info_log = False)
        self.insert_features_from_path(dataset_path, temp_output_path, info_log = False)
        self.delete_dataset(temp_output_path, info_log = False)
        if info_log:
            logger.info("Final feature count: {}.".format(self.feature_count(dataset_path)))
            logger.info("End: Union.")
        else:
            logger.debug("Final feature count: {}.".format(self.feature_count(dataset_path)))
        return dataset_path

    def update_field_by_coded_value_domain(self, dataset_path, field_name, code_field_name,
                                           domain_name, domain_workspace_path=None,
                                           dataset_where_sql=None, info_log=True):
        """Update field values using another field's values and a coded-values domain."""
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info("Start: Update field {} values using domain {}.".format(
                field_name, domain_name
                ))
        workspace_path = domain_workspace_path if domain_workspace_path else self.path
        code_description = [domain for domain in arcpy.da.ListDomains(workspace_path)
                            if domain.name == domain_name][0].codedValues
        cursor_field_names = (field_name, code_field_name)
        with arcpy.da.UpdateCursor(dataset_path, cursor_field_names, dataset_where_sql) as cursor:
            for old_description, code in cursor:
                new_description = code_description.get(code) if code else None
                if new_description != old_description:
                    cursor.updateRow((new_description, code))
        if info_log:
            logger.info("End: Update.")
        return field_name

    def update_field_by_constructor_method(self, dataset_path, field_name,
                                           constructor, method_name,
                                           field_as_first_arg=True,
                                           arg_field_names=[],
                                           kwarg_field_names=[],
                                           dataset_where_sql=None,
                                           info_log=True):
        """Update field values by passing them to a constructed object method.

        wraps ArcWorkspace.update_field_by_function.
        """
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info(" ".join([
                "Start: Update {} field values".format(field_name),
                "using the method {}".format(method_name),
                "from the object constructed by {}.".format(
                    constructor.__name__
                    )
                ]))
        function = getattr(constructor(), method_name)
        self.update_field_by_function(
            dataset_path, field_name, function, field_as_first_arg,
            arg_field_names, kwarg_field_names, dataset_where_sql,
            info_log = False
            )
        if info_log:
            logger.info("End: Update.")
        return field_name

    def update_field_by_expression(self, dataset_path, field_name, expression,
                                   dataset_where_sql=None, info_log=True):
        """Update field values using a (single) code-expression."""
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info(
                "Start: Update field {} using expression <{}>.".format(
                    field_name, expression
                    )
                )
        dataset_metadata = self.dataset_metadata(dataset_path)
        if dataset_metadata['is_spatial']:
            create_view = arcpy.management.MakeFeatureLayer
        elif dataset_metadata['is_table']:
            create_view = arcpy.management.MakeTableView
        else:
            raise ValueError(
                "{} unsupported dataset type.".format(dataset_path)
                )
        view_name = random_string()
        create_view(dataset_path, view_name, dataset_where_sql, self.path)
        arcpy.management.CalculateField(
            in_table = view_name, field = field_name, expression = expression,
            expression_type = 'python_9.3'
            )
        self.delete_dataset(view_name, info_log = False)
        if info_log:
            logger.info("End: Update.")
        return field_name

    def update_field_by_feature_matching(self, dataset_path, field_name, identifier_field_names,
                                         update_value_type, flag_value=None, sort_field_names=[],
                                         dataset_where_sql=None, info_log=True):
        """Update field values by aggregating information about matching features."""
        valid_update_value_types = ['flag-value', 'match-count', 'sort-order']
        raise NotImplementedError

    def update_field_by_function(self, dataset_path, field_name, function,
                                 field_as_first_arg=True, arg_field_names=[],
                                 kwarg_field_names=[], dataset_where_sql=None,
                                 info_log=True):
        """Update field values by passing them to a function.

        field_as_first_arg flag indicates that the function will consume the
        field's value as the first argument.
        arg_field_names indicate fields whose values will be positional
        arguments passed to the function.
        kwarg_field_names indicate fields who values will be passed as keyword
        arguments (field name as key).
        """
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info(
                "Start: Update {} field values using function {}.".format(
                    field_name, function.__name__
                    )
                )
        cursor_field_names = (
            [field_name] + list(arg_field_names) + list(kwarg_field_names)
            )
        with arcpy.da.UpdateCursor(
            dataset_path, cursor_field_names, dataset_where_sql
            ) as cursor:
            for row in cursor:
                args = row[1:len(arg_field_names) + 1]
                if field_as_first_arg:
                    args.insert(0, row[0])
                kwargs = dict(zip(
                    kwarg_field_names, row[len(arg_field_names) + 1:]
                    ))
                new_value = function(*args, **kwargs)
                if row[0] != new_value:
                    cursor.updateRow([new_value] + list(row[1:]))
        if info_log:
            logger.info("End: Update.")
        return field_name

    def update_field_by_geometry(self, dataset_path, field_name,
                                 geometry_property_cascade, update_units=None,
                                 spatial_reference_id=None,
                                 dataset_where_sql=None, info_log=True):
        """Update field values by cascading through a geometry's attributes.

        If the spatial reference ID is not specified, the spatial reference of
        the dataset is used.
        """
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info(
                "Start: Update field {} values using {} geometry properties.".format(
                    field_name, geometry_property_cascade
                    )
                )
        if update_units:
            raise NotImplementedError("update_units not yet implemented.")
        with arcpy.da.UpdateCursor(
            in_table = dataset_path,
            field_names = (field_name, 'shape@'),
            where_clause = dataset_where_sql,
            spatial_reference = (arcpy.SpatialReference(spatial_reference_id)
                                 if spatial_reference_id else None)
            ) as cursor:
            for field_value, geometry in cursor:
                if geometry is None:
                    new_value = None
                else:
                    new_value = geometry
                    # Cascade down the geometry properties.
                    for geometry_property in geometry_property_cascade:
                        geometry_property = geometry_property.lower()
                        if geometry_property in ['area']:
                            try:
                                new_value = new_value.area
                            except TypeError:
                                raise
                        elif geometry_property in ['centroid']:
                            new_value = new_value.centroid
                        elif geometry_property in ['length']:
                            new_value = new_value.length
                        elif geometry_property in ['x', 'x-coordinate']:
                            new_value = new_value.X
                        elif geometry_property in ['y', 'y-coordinate']:
                            new_value = new_value.Y
                        elif geometry_property in ['z', 'z-coordinate']:
                            new_value = new_value.Z
                        elif geometry_property in ['x-minimum', 'xmin']:
                            new_value = new_value.extent.XMin
                        elif geometry_property in ['y-minimum', 'ymin']:
                            new_value = new_value.extent.YMin
                        elif geometry_property in ['z-minimum', 'zmin']:
                            new_value = new_value.extent.ZMin
                        elif geometry_property in ['x-maximum', 'xmax']:
                            new_value = new_value.extent.XMax
                        elif geometry_property in ['y-maximum', 'ymax']:
                            new_value = new_value.extent.YMax
                        elif geometry_property in ['z-maximum', 'zmax']:
                            new_value = new_value.extent.ZMax
                if new_value != field_value:
                    cursor.updateRow((new_value, geometry))
        if info_log:
            logger.info("End: Update.")
        return field_name

    def update_field_by_joined_value(self, dataset_path, field_name, join_dataset_path,
                                     join_field_name, on_field_pairs, dataset_where_sql=None,
                                     info_log=True):
        """Update field values by referencing a joinable field."""
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info("Start: Update field {} values with joined value {}.{}>.".format(
                field_name, join_dataset_path,  join_field_name
                ))
        # Build join-reference.
        cursor_field_names = (join_field_name,) + tuple(pair[1] for pair in on_field_pairs)
        with arcpy.da.SearchCursor(join_dataset_path, cursor_field_names) as cursor:
            join_value = {row[1:]: row[0] for row in cursor}
        cursor_field_names = (field_name,) + tuple(pair[0] for pair in on_field_pairs)
        with arcpy.da.UpdateCursor(dataset_path, cursor_field_names, dataset_where_sql) as cursor:
            for row in cursor:
                new_value = join_value.get(tuple(row[1:]))
                if new_value != row[0]:
                    cursor.updateRow([new_value,] + list(row[1:]))
        if info_log:
            logger.info("End: Update.")
        return field_name

    def update_field_by_near_feature(self, dataset_path, field_name,
                                     near_dataset_path, near_field_name,
                                     replacement_value=None,
                                     distance_field_name=None,
                                     angle_field_name=None,
                                     x_coordinate_field_name=None,
                                     y_coordinate_field_name=None,
                                     max_search_distance=None, near_rank=1,
                                     dataset_where_sql=None, info_log=True):
        """Update field by finding near-feature value.

        One can optionally update ancillary fields with analysis properties by
        indicating the following fields: distance_field_name, angle_field_name,
        x_coordinate_field_name, y_coordinate_field_name.
        """
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info("Start: Update field {} with near-value {}.{}.".format(
                field_name, near_dataset_path,  near_field_name
                ))
        # Create a temporary copy of the near dataset.
        temp_near_path = self.copy_dataset(
            near_dataset_path, memory_path(), info_log = False
            )
        dataset_oid_field_name = (
            self.dataset_metadata(dataset_path)['oid_field_name']
            )
        temp_near_oid_field_name = (
            self.dataset_metadata(temp_near_path)['oid_field_name']
            )
        # Create neutral field for holding near value (avoids collisions).
        temp_field_metadata = self.field_metadata(
            temp_near_path, near_field_name
            )
        temp_field_metadata['name'] = random_string()
        # Cannot add OID-type field, so push to a long-type.
        if temp_field_metadata['type'].lower() == 'oid':
            temp_field_metadata['type'] = 'long'
        self.add_fields_from_metadata_list(
            temp_near_path, [temp_field_metadata], info_log = False
            )
        self.update_field_by_expression(
            temp_near_path, temp_field_metadata['name'],
            expression = '!{}!'.format(near_field_name), info_log = False
            )
        # Create the temp output of the near features.
        view_name = random_string()
        arcpy.management.MakeFeatureLayer(
            dataset_path, view_name, dataset_where_sql, self.path
            )
        temp_output_path = memory_path()
        arcpy.analysis.GenerateNearTable(
            in_features = dataset_path, near_features = temp_near_path,
            out_table = temp_output_path, search_radius = max_search_distance,
            location = any([x_coordinate_field_name, y_coordinate_field_name]),
            angle = any([angle_field_name]),
            closest = 'all', closest_count = near_rank,
            # Would prefer geodesic, but that forces XY values to lon-lat.
            method ='planar'
            )
        self.delete_dataset(view_name, info_log = False)
        # Remove near rows not matching chosen rank.
        self.delete_features(
            temp_output_path,
            dataset_where_sql = "near_rank <> {}".format(near_rank),
            info_log = False
            )
        # Join near values to the near output.
        self.join_field(
            temp_output_path, temp_near_path,
            join_field_name = temp_field_metadata['name'],
            on_field_name = 'near_fid',
            on_join_field_name = temp_near_oid_field_name, info_log = False
            )
        self.delete_dataset(temp_near_path, info_log = False)
        # Apply replacement value, if set.
        if replacement_value:
            expression = '{} if !{}! else None'.format(
                repr(replacement_value), temp_field_metadata['name']
                )
            self.update_field_by_expression(
                temp_output_path, temp_field_metadata['name'], expression,
                info_log = False
                )
        # Update values in original dataset.
        self.update_field_by_joined_value(
            dataset_path, field_name,
            join_dataset_path = temp_output_path,
            join_field_name = temp_field_metadata['name'],
            on_field_pairs = [(dataset_oid_field_name, 'in_fid')],
            dataset_where_sql = dataset_where_sql, info_log = False
            )
        # Update ancillary near property fields.
        if distance_field_name:
            self.update_field_by_joined_value(
                dataset_path, distance_field_name,
                join_dataset_path = temp_output_path,
                join_field_name = 'near_dist',
                on_field_pairs = [(dataset_oid_field_name, 'in_fid')],
                dataset_where_sql = dataset_where_sql, info_log = False
                )
        if angle_field_name:
            self.update_field_by_joined_value(
                dataset_path, angle_field_name,
                join_dataset_path = temp_output_path,
                join_field_name = 'near_angle',
                on_field_pairs = [(dataset_oid_field_name, 'in_fid')],
                dataset_where_sql = dataset_where_sql, info_log = False
                )
        if x_coordinate_field_name:
            self.update_field_by_joined_value(
                dataset_path, x_coordinate_field_name,
                join_dataset_path = temp_output_path,
                join_field_name = 'near_x',
                on_field_pairs = [(dataset_oid_field_name, 'in_fid')],
                dataset_where_sql = dataset_where_sql, info_log = False
                )
        if y_coordinate_field_name:
            self.update_field_by_joined_value(
                dataset_path, y_coordinate_field_name,
                join_dataset_path = temp_output_path,
                join_field_name = 'near_y',
                on_field_pairs = [(dataset_oid_field_name, 'in_fid')],
                dataset_where_sql = dataset_where_sql, info_log = False
                )
        self.delete_dataset(temp_output_path, info_log = False)
        if info_log:
            logger.info("End: Update.")
        return field_name

    def update_field_by_overlay(self, dataset_path, field_name, overlay_dataset_path,
                                overlay_field_name, replacement_value=None,
                                overlay_most_coincident=False, overlay_central_coincident=False,
                                dataset_where_sql=None, info_log=True):
        """Update field by finding overlay feature value.

        Since only one value will be selected in the overlay, operations with multiple overlaying
        features will respect the geoprocessing environment's merge rule. This rule will generally
        defer to the 'first' feature's value.

        Please note that only one overlay flag at a time can be used. If mutliple are set to True,
        the first one referenced in the code will be used. If no overlay flags are set, the
        operation will perform a basic intersection check, and the result will be at the whim of
        the geoprocessing environment's merge rule for the update field.
        """
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info("Start: Update field {} with overlay value {}.{}.".format(
                field_name, overlay_dataset_path,  overlay_field_name
                ))
        # Check flags & set details for spatial join call.
        if overlay_most_coincident:
            raise NotImplementedError(
                "overlay_most_coincident is not yet implemented."
                )
        elif overlay_central_coincident:
            join_operation = 'join_one_to_one'
            match_option = 'have_their_center_in'
        else:
            join_operation = 'join_one_to_one'
            match_option = 'intersect'
        # Create a temporary copy of the overlay dataset.
        temp_overlay_path = self.copy_dataset(overlay_dataset_path, memory_path(), info_log = False)
        # Create neutral field for holding overlay value (avoids collisions).
        temp_field_metadata = self.field_metadata(temp_overlay_path, overlay_field_name)
        temp_field_metadata['name'] = random_string()
        # Cannot add OID-type field, so push to a long-type.
        if temp_field_metadata['type'].lower() == 'oid':
            temp_field_metadata['type'] = 'long'
        self.add_fields_from_metadata_list(temp_overlay_path, [temp_field_metadata],
                                           info_log = False)
        self.update_field_by_expression(temp_overlay_path, temp_field_metadata['name'],
                                        expression = '!{}!'.format(overlay_field_name),
                                        info_log = False)
        # Create the temp output of the overlay.
        view_name = random_string()
        arcpy.management.MakeFeatureLayer(dataset_path, view_name, dataset_where_sql, self.path)
        temp_output_path = memory_path()
        arcpy.analysis.SpatialJoin(
            target_features = view_name, join_features = temp_overlay_path,
            out_feature_class = temp_output_path, join_operation = join_operation,
            join_type = 'keep_common', match_option = match_option
            )
        self.delete_dataset(view_name, info_log = False)
        self.delete_dataset(temp_overlay_path, info_log = False)
        # Apply replacement value, if set.
        if replacement_value:
            expression = '{} if !{}! else None'.format(
                repr(replacement_value), temp_field_metadata['name']
                )
            self.update_field_by_expression(
                temp_output_path, temp_field_metadata['name'], expression,
                info_log = False
                )
        # Update values in original dataset.
        self.update_field_by_joined_value(
            dataset_path, field_name,
            join_dataset_path = temp_output_path, join_field_name = temp_field_metadata['name'],
            on_field_pairs = [(self.dataset_metadata(dataset_path)['oid_field_name'],
                               'target_fid')],
            dataset_where_sql = dataset_where_sql, info_log = False
            )
        self.delete_dataset(temp_output_path, info_log = False)
        if info_log:
            logger.info("End: Update.")
        return field_name

    def update_fields_by_geometry_node_ids(self, dataset_path,
                                           from_id_field_name,
                                           to_id_field_name, info_log=True):
        """Update fields with node IDs based on feature geometry.

        Method assumes the IDs are numeric.
        """
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info(" ".join([
                "Start: Update node ID fields {} & {}",
                "based on feature geometry."
                ]).format(from_id_field_name, to_id_field_name))
        # Get next available node ID.
        node_ids = set()
        with arcpy.da.SearchCursor(
            dataset_path, [from_id_field_name, to_id_field_name]
            ) as cursor:
            for row in cursor:
                node_ids.update(row)
        # Remove missing node ID instance.
        node_ids.discard(None)
        next_open_node_id = max(node_ids) + 1 if node_ids else 1
        # Build node XY mapping.
        with arcpy.da.SearchCursor(
            dataset_path,
            ['oid@', from_id_field_name, to_id_field_name, 'shape@']
            ) as cursor:
            node_xy_map= {}
            # {node_xy: {'node_id': int(), 'f_oids': set(), 't_oids': set()},}
            for oid, fnode_id, tnode_id, geometry in cursor:
                fnode_xy = geometry.firstPoint.X, geometry.firstPoint.Y
                tnode_xy = geometry.lastPoint.X, geometry.lastPoint.Y
                # Add the XY if not yet present.
                for node_id, xy, oid_set_key in [
                    (fnode_id, fnode_xy, 'f_oids'),
                    (tnode_id, tnode_xy, 't_oids')
                    ]:
                    if xy not in node_xy_map:
                        # Add XY with the node ID.
                        node_xy_map[xy] = {
                            'node_id': None, 'f_oids': set(), 't_oids': set(),
                            }
                    # Choose lowest non-missing ID to perpetuate at the XY.
                    try:
                        node_xy_map[xy]['node_id'] = min(
                            x for x in [node_xy_map[xy]['node_id'], node_id]
                            if x is not None
                            )
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
                    node_xy_map[new_xy]['t_oids']
                    )
                )
            # If ID already present in node_id_map, re-ID one of the nodes.
            if new_node_id in node_id_map:
                old_node_id = new_node_id
                old_xy = node_id_map[old_node_id]['node_xy']
                old_feature_count = node_id_map[old_node_id]['feature_count']
                # If new node has more links, re-ID old node.
                if new_feature_count > old_feature_count:
                    node_xy_map[old_xy]['node_id'] = next_open_node_id
                    node_id_map[next_open_node_id] = (
                        node_id_map.pop(old_node_id)
                        )
                    next_open_node_id += 1
                # Re-ID new node if old node has more links (or tequal counts).
                else:
                    node_xy_map[new_xy]['node_id'] = next_open_node_id
                    new_node_id = next_open_node_id
                    next_open_node_id += 1
            # Now add the new node.
            node_id_map[new_node_id] = {'node_xy': new_xy,
                                        'feature_count': new_feature_count,}
        # Build a feature-node mapping from node_xy_map.
        feature_nodes = {}
        # {feature_oid: {'fnode': int(), 'tnode': int()},}
        for xy in node_xy_map:
            node_id = node_xy_map[xy]['node_id']
            # If feature object ID is missing in feature_nodes: add.
            for feature_oid in node_xy_map[xy]['f_oids'].union(
                node_xy_map[xy]['t_oids']
                ):
                if feature_oid not in feature_nodes:
                    feature_nodes[feature_oid] = {}
            for feature_oid in node_xy_map[xy]['f_oids']:
                feature_nodes[feature_oid]['fnode'] = node_id
            for feature_oid in node_xy_map[xy]['t_oids']:
                feature_nodes[feature_oid]['tnode'] = node_id
        # Push changes to features.
        with arcpy.da.UpdateCursor(
            dataset_path, ['oid@', from_id_field_name, to_id_field_name]
            ) as cursor:
            for oid, old_fnode_id, old_tnode_id in cursor:
                new_fnode_id = feature_nodes[oid]['fnode']
                new_tnode_id = feature_nodes[oid]['tnode']
                if any([
                    old_fnode_id != new_fnode_id, old_tnode_id != new_tnode_id
                    ]):
                    cursor.updateRow([oid, new_fnode_id, new_tnode_id])
        if info_log:
            logger.info("End: Update.")
        return from_id_field_name, to_id_field_name

    # Analysis/extraction methods.

    def generate_facility_service_rings(self, dataset_path, output_path,
                                        network_path, cost_attribute,
                                        ring_width, max_distance,
                                        restriction_attributes=[],
                                        travel_from_facility=False,
                                        detailed_rings=False,
                                        overlap_facilities=True,
                                        id_field_name=None,
                                        dataset_where_sql=None, info_log=True):
        """Create facility service ring features using a network dataset."""
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info(
                "Start: Generate service rings for facilities in {}".format(
                    dataset_path
                    )
                )
        # Get Network Analyst license.
        if arcpy.CheckOutExtension('Network') != 'CheckedOut':
            raise RuntimeError("Unable to check out Network Analyst license.")
        # Create break value range.
        break_values = range(ring_width, max_distance + 1, ring_width)
        view_name = random_string()
        arcpy.management.MakeFeatureLayer(
            in_features = dataset_path, out_layer = view_name,
            where_clause = dataset_where_sql
            )
        arcpy.na.MakeServiceAreaLayer(
            in_network_dataset = network_path,
            out_network_analysis_layer = 'service_area',
            impedance_attribute = cost_attribute,
            travel_from_to = (
                'travel_from' if travel_from_facility else 'travel_to'
                ),
            default_break_values = ' '.join(
                str(x) for x in range(ring_width, max_distance + 1, ring_width)
                ),
            polygon_type = (
                'detailed_polys' if detailed_rings else 'simple_polys'
                ),
            merge = 'no_merge' if overlap_facilities else 'no_overlap',
            nesting_type = 'rings',
            UTurn_policy = 'allow_dead_ends_and_intersections_only',
            restriction_attribute_name = restriction_attributes,
            # The trim seems to override the non-overlapping part in larger
            # analyses.
            ##polygon_trim = True, poly_trim_value = ring_width,
            hierarchy = 'no_hierarchy'
            )
        arcpy.na.AddLocations(
            in_network_analysis_layer = "service_area",
            sub_layer = "Facilities",
            in_table = view_name,
            field_mappings = 'Name {} #'.format(id_field_name),
            search_tolerance = max_distance, match_type = 'match_to_closest',
            append = 'clear', snap_to_position_along_network = 'no_snap',
            exclude_restricted_elements = True,
            )
        arcpy.na.Solve(
            in_network_analysis_layer="service_area",
            ignore_invalids = True, terminate_on_solve_error = True
            )
        # Copy output to temp feature class.
        output_path = self.copy_dataset(
            'service_area/Polygons', output_path = memory_path(),
            info_log = False
            )
        id_field_metadata = self.field_metadata(dataset_path, id_field_name)
        self.add_fields_from_metadata_list(
            output_path, [id_field_metadata], info_log = False
            )
        type_extract_function_map = {
            'short': (lambda x: int(x.split(' : ')[0]) if x else None),
            'long': (lambda x: int(x.split(' : ')[0]) if x else None),
            'double': (lambda x: float(x.split(' : ')[0]) if x else None),
            'single': (lambda x: float(x.split(' : ')[0]) if x else None),
            'string': (lambda x: x.split(' : ')[0] if x else None),
            }
        self.delete_dataset('service_area')
        self.update_field_by_function(
            output_path, id_field_name,
            function = type_extract_function_map[id_field_metadata['type']],
            field_as_first_arg = False, arg_field_names = ['Name'],
            info_log = False
            )
        self.delete_dataset(view_name, info_log = False)
        if info_log:
            logger.info("Final feature count: {}.".format(
                self.feature_count(dataset_path)
                ))
            logger.info("End: Generate.")
        else:
            logger.debug("Final feature count: {}.".format(
                self.feature_count(dataset_path)
                ))
        return output_path

    def select_features_to_lists(self, dataset_path, field_names,
                                 dataset_where_sql=None, info_log=True):
        """Return features as list of lists."""
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info(
                "Start: Select {} feature into a list of lists.".format(
                    dataset_path
                    )
                )
        with arcpy.da.SearchCursor(
            dataset_path, field_names, dataset_where_sql
            ) as cursor:
            features = [list(feature) for feature in cursor]
        if info_log:
            logger.info("End: Select.")
        return features


# Helper functions.

def debug_call(with_argument_values=True):
    """Return a debug string of the call to the object this is placed in."""
    frame = inspect.currentframe().f_back
    argvalues = inspect.getargvalues(frame)
    if with_argument_values:
        return "{}{}".format(frame.f_code.co_name, inspect.formatargvalues(*argvalues))
    else:
        return "{}{}".format(frame.f_code.co_name, tuple(argvalues[0]))


def memory_path(prefix='', suffix='', random_length=4):
    """Creates a memory workspace path to use."""
    name = '{}{}{}'.format(prefix, random_string(random_length), suffix)
    return os.path.join('in_memory', name)


def random_string(length=16):
    """Generates a random string of the given length."""
    return  ''.join(random.choice(string.ascii_letters) for x in range(length))
