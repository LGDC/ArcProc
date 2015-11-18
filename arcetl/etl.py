# -*- coding=utf-8 -*-
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
        # Clear the transform dataset.
        if self.transform_path and self.workspace.is_valid_dataset(self.transform_path):
            self.workspace.delete_dataset(self.transform_path, info_log = False)
            self.transform_path = None

    def extract(self, extract_path, extract_where_sql=None, schema_only=False):
        """Extract features to transform workspace."""
        logger.info("Start: Extract {}.".format(extract_path))
        # Extract to a new dataset.
        self.transform_path = self.workspace.copy_dataset(extract_path, memory_path(),
                                                          extract_where_sql, schema_only,
                                                          info_log = False)
        logger.info("End: Extract.")
        return self.transform_path

    def load(self, load_path, load_where_sql=None, preserve_features=False):
        """Load features from transform workspace to the load-dataset."""
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
        raise NotImplementedError

    def transform(self, transform_name, **kwargs):
        """Run transform operation as defined in the workspace."""
        transform = getattr(self.workspace, transform_name)
        # Unless otherwise stated, the dataset path to be the instance's transform path.
        if 'dataset_path' not in kwargs:
            kwargs['dataset_path'] = self.transform_path
        # If the function arguments include an output path, supersede the old transform path.
        if 'output_path' in inspect.getargspec(transform).args:
            kwargs['output_path'] = memory_path()
        result = transform(**kwargs)
        if 'output_path' in kwargs:
            self.workspace.delete_dataset(self.transform_path, info_log = False)
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
        """Return dictionary of the dataset's metadata."""
        logger.debug("Called {}".format(debug_call()))
        metadata = {}
        arc_description = arcpy.Describe(dataset_path)
        metadata['name'] = arc_description.name
        metadata['path'] = arc_description.catalogPath
        metadata['data_type'] = arc_description.dataType
        metadata['workspace_path'] = arc_description.path
        metadata['is_table'] = True if hasattr(arc_description, 'hasOID') else False
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
        metadata['is_spatial'] = True if hasattr(arc_description, 'shapeType') else False
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
            metadata = [field for field in self.dataset_metadata(dataset_path)['fields']
                         if field['name'].lower() == field_name.lower()][0]
        except IndexError:
            raise AttributeError("{} not present on {}".format(field_name, dataset_path))
        return metadata

    def is_valid_dataset(self, dataset_path):
        """Check whether a dataset exists/is valid."""
        logger.debug("Called {}".format(debug_call()))
        if arcpy.Exists(dataset_path):
            return self.dataset_metadata(dataset_path)['is_table']
        else:
            return False

    # Workspace management methods.

    def copy_dataset(self, dataset_path, output_path, dataset_where_sql=None, schema_only=False,
                     overwrite=False, info_log=True):
        """Copy dataset."""
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info("Start: Copy {} to {}.".format(dataset_path, output_path))
        dataset_metadata = self.dataset_metadata(dataset_path)
        if dataset_metadata['is_spatial']:
            create_view = arcpy.management.MakeFeatureLayer
            copy_rows = arcpy.management.CopyFeatures
        elif  dataset_metadata['is_table']:
            create_view = arcpy.management.MakeTableView
            copy_rows = arcpy.management.CopyRows
        else:
            raise ValueError("{} unsupported dataset type.".format(dataset_path))
        if overwrite and self.is_valid_dataset(copypath):
            self.delete_dataset(output_path, info_log = False)
        view_name = random_string()
        create_view(dataset_path, view_name,
                    dataset_where_sql if not schema_only else "0 = 1", self.path)
        copy_rows(view_name, output_path)
        self.delete_dataset(view_name, info_log = False)
        if info_log:
            logger.info("End: Copy.")
        return output_path

    def create_dataset(self, dataset_path, field_metadata=[], geometry_type=None,
                       spatial_reference_id=None, info_log=True):
        """Create new dataset."""
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info("Start: Create dataset {}.".format(dataset_path))
        if geometry_type:
            if spatial_reference_id:
                spatial_reference = arcpy.SpatialReference(spatial_reference_id)
            else:
                spatial_reference = arcpy.SpatialReference(4326)
            arcpy.management.CreateFeatureclass(out_path = os.path.dirname(dataset_path),
                                                out_name = os.path.basename(dataset_path),
                                                geometry_type = geometry_type,
                                                spatial_reference = spatial_reference)
        else:
            arcpy.management.CreateTable(out_path = os.path.dirname(dataset_path),
                                         out_name = os.path.basename(dataset_path))
        if field_metadata:
            if isinstance(field_metadata, dict):
                field_metadata = [field_metadata]
            for field in field_metadata:
                self.add_field(dataset_path, field['name'], field['type'], field.get('length'),
                               field.get('precision'), field.get('scale'), info_log = False)
        return dataset_path

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
            logger.info("Start: Join field {} from {}.".format(join_field_name, join_dataset_path))
        arcpy.management.JoinField(dataset_path, in_field = on_field_name,
                                   join_table = join_dataset_path, join_field = on_join_field_name,
                                   fields = join_field_name)
        if info_log:
            logger.info("End: Join.")
        return join_field_name

    # Feature alteration methods.

    def convert_polygons_to_lines(self, dataset_path, output_path, topological=False,
                                  id_field_name=None, info_log=True):
        """Convert geometry from polygons to lines.

        If topological is set to True, shared outlines will be a single, separate feature. Note
        that one cannot pass attributes to a topological transformation (as the values would not
        apply to all adjacent features).

        If an id field name is specified, the output dataset will identify the input features that
        defined the line feature with the name & values from the provided field. This option will
        be ignored if the output is non-topological lines, as the field will pass over with the
        rest of the attributes.
        """
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info("Start: Convert polygon features in {} to lines.".format(dataset_path))
        arcpy.management.PolygonToLine(in_features = dataset_path, out_feature_class = output_path,
                                       neighbor_option = topological)
        if topological:
            if id_field_name:
                id_field_info = self.field_metadata(dataset_path, id_field_name)
                oid_field_name = self.dataset_metadata(dataset_path)['oid_field_name']
            sides = ('left', 'right')
            for side in sides:
                side_oid_field_name = '{}_FID'.format(side.upper())
                if id_field_name:
                    side_id_field_info = id_field_info.copy()
                    side_id_field_info['name'] = '{}_{}'.format(side, id_field_name)
                    # Cannot create an OID-type field, so force to long.
                    if side_id_field_info['type'].lower() == 'oid':
                        side_id_field_info['type'] = 'long'
                    self.add_fields_from_metadata_list(output_path, [side_id_field_info],
                                                       info_log = False)
                    self.update_field_by_join_value(
                        dataset_path = output_path, field_name = side_id_field_info['name'],
                        join_dataset_path = dataset_path, join_field_name = id_field_name,
                        on_field_name = side_oid_field_name,
                        on_join_field_name = oid_field_name,
                        info_log = False
                        )
                self.delete_field(output_path, side_oid_field_name, info_log = False)
        else:
            self.delete_field(output_path, 'ORIG_FID', info_log = False)
        if info_log:
            logger.info("End: Convert.")
        return output_path

    def delete_features(self, dataset_path, dataset_where_sql=None,
                        info_log=True):
        """Delete select features."""
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info("Start: Delete features from {}.".format(dataset_path))
        dataset_metadata = self.dataset_metadata(dataset_path)
        # Database-type (also not in-memory) & no sub-selection: use truncate.
        if (dataset_metadata['data_type'] in ['FeatureClass', 'Table']
            and dataset_metadata['workspace_path'] != 'in_memory'
            and dataset_where_sql is None):
            arcpy.management.TruncateTable(dataset_path)
        # Non-database or with sub-selection options.
        else:
            if dataset_metadata['is_spatial']:
                create_view = arcpy.management.MakeFeatureLayer
                delete_rows = arcpy.management.DeleteFeatures
            elif dataset_metadata['is_table']:
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

    def dissolve_features(self, dataset_path, dissolve_field_names, multipart=True,
                          unsplit_lines=False, dataset_where_sql=None, info_log=True):
        """Merge features that share values in given fields."""
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info("Start: Dissolve features on {}.".format(dissolve_field_names))
            logger.info("Initial feature count: {}.".format(self.feature_count(dataset_path)))
        else:
            logger.debug("Initial feature count: {}.".format(self.feature_count(dataset_path)))
        # Set the environment tolerance, so we can be sure the in_memory datasets respect it.
        # 0.003280839895013 is the default for all datasets in our geodatabases.
        arcpy.env.XYTolerance = 0.003280839895013
        view_name = random_string()
        arcpy.management.MakeFeatureLayer(dataset_path, view_name, dataset_where_sql, self.path)
        temp_dissolved_path = memory_path()
        arcpy.management.Dissolve(in_features = view_name, out_feature_class = temp_dissolved_path,
                                  dissolve_field = dissolve_field_names, multi_part = multipart,
                                  unsplit_lines = unsplit_lines)
        # Delete undissolved features that are now dissolved (in the temp).
        self.delete_features(view_name, info_log = False)
        self.delete_dataset(view_name, info_log = False)
        # Copy the dissolved features (in the temp) to the dataset.
        self.insert_features_from_path(dataset_path, temp_dissolved_path, info_log = False)
        self.delete_dataset(temp_dissolved_path, info_log = False)
        if info_log:
            logger.info("Final feature count: {}.".format(self.feature_count(dataset_path)))
            logger.info("End: Dissolve.")
        else:
            logger.debug("Final feature count: {}.".format(self.feature_count(dataset_path)))
        return dataset_path

    def erase_features(self, dataset_path, erase_dataset_path, dataset_where_sql=None,
                       erase_where_sql=None, info_log=True):
        """Erase feature geometry where overlaps erase dataset geometry."""
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info("Start: Erase where geometry overlaps {}.".format(erase_dataset_path))
            logger.info("Initial feature count: {}.".format(self.feature_count(dataset_path)))
        else:
            logger.debug("Initial feature count: {}.".format(self.feature_count(dataset_path)))
        # Pass only features that meet the dataset_where_sql.
        view_name = random_string()
        arcpy.management.MakeFeatureLayer(dataset_path, view_name, dataset_where_sql, self.path)
        # Pass only erase features that meet the erase_where_sql.
        erase_view_name = random_string()
        arcpy.management.MakeFeatureLayer(erase_dataset_path, erase_view_name, erase_where_sql,
                                          self.path)
        temp_output_path = memory_path()
        arcpy.analysis.Erase(in_features = view_name, erase_features = erase_view_name,
                             out_feature_class = temp_output_path)
        self.delete_dataset(erase_view_name, info_log = False)
        # Load back into the dataset.
        self.delete_features(view_name, info_log = False)
        self.delete_dataset(view_name, info_log = False)
        self.insert_features_from_path(dataset_path, temp_output_path, info_log = False)
        self.delete_dataset(temp_output_path, info_log = False)
        if info_log:
            logger.info("Final feature count: {}.".format(self.feature_count(dataset_path)))
            logger.info("End: Erase.")
        else:
            logger.debug("Final feature count: {}.".format(self.feature_count(dataset_path)))
        return dataset_path

    def identity_features(self, dataset_path, field_name, identity_dataset_path,
                          identity_field_name, replacement_value=None,
                          dataset_where_sql=None, chunk_size=4096,
                          info_log=True):
        """Assign unique identity value to features, splitting where necessary.

        replacement_value is a value that will substitute as the identity
        value. This method has a 'chunking' routine in order to avoid an
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
            arcpy.management.MakeFeatureLayer(
                in_features = dataset_path, out_layer = view_name,
                # ArcPy where clauses cannot use 'between'.
                where_clause = "{0} >= {1} and {0} <= {2}".format(
                    self.dataset_metadata(dataset_path)['oid_field_name'],
                    from_objectid, to_objectid
                    ),
                workspace = self.path
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

    def insert_features_from_iterables(self, dataset_path, insert_dataset_iterables, field_names,
                                       info_log=True):
        """Insert features from a collection of iterables."""
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info("Start: Insert features into {} from a collection of iterables.".format(
                dataset_path
                ))
        with arcpy.da.InsertCursor(dataset_path, field_names) as cursor:
            for row in insert_dataset_iterables:
                cursor.insertRow(row)
        if info_log:
            logger.info("End: Insert.")
        return dataset_path

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

    def update_field_by_expression(self, dataset_path, field_name, expression,
                                   dataset_where_sql=None, info_log=True):
        """Update field values using a (single) code-expression."""
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info("Start: Update field {} values using expression <{}>.".format(field_name,
                                                                                      expression))
        dataset_metadata = self.dataset_metadata(dataset_path)
        if dataset_metadata['is_spatial']:
            create_view = arcpy.management.MakeFeatureLayer
        elif dataset_metadata['is_table']:
            create_view = arcpy.management.MakeTableView
        else:
            raise ValueError("{} unsupported dataset type.".format(dataset_path))
        view_name = random_string()
        create_view(dataset_path, view_name, dataset_where_sql, self.path)
        arcpy.management.CalculateField(in_table = view_name, field = field_name,
                                        expression = expression, expression_type = 'python_9.3')
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

    def update_field_by_function(self, dataset_path, field_name, function, field_as_first_arg=True,
                                 arg_field_names=[], kwarg_field_names=[], dataset_where_sql=None,
                                 info_log=True):
        """Update field values by passing them to a function.

        arg_field_names indicate fields that will be positional arguments passed to the function.
        kwarg_field_names indicate fields that will be passed as keyword args (field name as key).
        Helper fields will be passed to the function as keyword arguments {name: value,}.
        field_as_first_arg flag indicates that the function will consume the field as the first
        argument.
        """
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info("Start: Update field {} values using function {}.".format(
                field_name, function.__name__
                ))
        cursor_field_names = [field_name] + list(arg_field_names) + list(kwarg_field_names)
        with arcpy.da.UpdateCursor(dataset_path, cursor_field_names, dataset_where_sql) as cursor:
            for row in cursor:
                args = row[1:1 + len(arg_field_names)]
                if field_as_first_arg:
                    args.insert(0, row[0])
                kwargs = dict(zip(kwarg_field_names, row[1 + len(arg_field_names):]))
                new_value = function(*args, **kwargs)
                if new_value != row[0]:
                    cursor.updateRow([new_value] + list(row[1:]))
        if info_log:
            logger.info("End: Update.")
        return field_name

    def update_field_by_geometry(self, dataset_path, field_name, geometry_property_cascade,
                                 update_units=None, spatial_reference_id=None,
                                 dataset_where_sql=None, info_log=True):
        """Update field values by cascading through a geometry's attributes.

        If the spatial reference ID is not specified, the spatial reference of the dataset is used.
        """
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info("Start: Update field {} values using {} property of the geometry.".format(
                field_name, geometry_property_cascade
                ))
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
                    # Cascade down the geometry properties until new_value is the final one.
                    for geometry_property in geometry_property_cascade:
                        if geometry_property.lower() in ('area',):
                            new_value = new_value.getArea(units = update_units)
                        elif geometry_property.lower() in ('centroid',):
                            new_value = new_value.centroid
                        elif geometry_property.lower() in ('length',):
                            new_value = new_value.getLength (units = update_units)
                        elif geometry_property.lower() in ('x', 'x-coordinate',):
                            new_value = new_value.X
                        elif geometry_property.lower() in ('y', 'y-coordinate',):
                            new_value = new_value.Y
                        elif geometry_property.lower() in ('z', 'z-coordinate',):
                            new_value = new_value.Z
                        elif geometry_property.lower() in ('x-minimum', 'xmin',):
                            new_value = new_value.extent.XMin
                        elif geometry_property.lower() in ('y-minimum', 'ymin',):
                            new_value = new_value.extent.YMin
                        elif geometry_property.lower() in ('z-minimum', 'zmin',):
                            new_value = new_value.extent.ZMin
                        elif geometry_property.lower() in ('x-maximum', 'xmax',):
                            new_value = new_value.extent.XMax
                        elif geometry_property.lower() in ('y-maximum', 'ymax',):
                            new_value = new_value.extent.YMax
                        elif geometry_property.lower() in ('z-maximum', 'zmax',):
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
        # Push the overlay (or replacement) value from the temp field to the update field.
        if replacement_value:
            expression = '{} if !{}! else None'.format(repr(replacement_value),
                                                       temp_field_metadata['name'])
        else:
            expression = '!{}!'.format(temp_field_metadata['name'])
        self.update_field_by_expression(temp_output_path, field_name, expression, info_log = False)
        # Replace values in original dataset.
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

    # Analysis/extraction methods.

    def select_features_to_lists(self, dataset_path, field_names, dataset_where_sql=None,
                                 info_log=True):
        """Return features as list of lists."""
        logger.debug("Called {}".format(debug_call()))
        if info_log:
            logger.info("Start: Select {} feature into a list of lists.".format(dataset_path))
        with arcpy.da.SearchCursor(dataset_path, field_names, dataset_where_sql) as cursor:
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


def memory_path():
    """Creates a memory workspace path to use."""
    return os.path.join('in_memory', random_string())


def random_string(length=16):
    """Generates a random string of the given length."""
    return  ''.join(random.choice(string.ascii_letters) for x in range(length))
