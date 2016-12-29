"""Interfaces for ArcObjects."""
import logging
import uuid

import arcpy

from arcetl import helpers


LOG = logging.getLogger(__name__)


class ArcExtension(object):
    """Context manager for an ArcGIS extension."""

    def __init__(self, name, activate_on_init=True):
        self.name = name
        # For now assume name & code are same.
        self.code = name
        self.activated = None
        self.result_activated_map = {'CheckedIn': False, 'CheckedOut': True,
                                     'Failed': False, 'NotInitialized': False,
                                     'Unavailable': False}
        self.result_log_level_map = {
            'CheckedIn': helpers.log_level('info'),
            'CheckedOut': helpers.log_level('info'),
            'Failed': helpers.log_level('warning'),
            'NotInitialized': helpers.log_level('warning'),
            'Unavailable': helpers.log_level('warning'),
            }
        self.result_log_message_map = {
            'CheckedIn': "{} extension deactivated.".format(self.code),
            'CheckedOut': "{} extension activated.".format(self.code),
            'NotInitialized': "No desktop license set.",
            'Unavailable': "Extension unavailable.",
            'Failed': "System failure."
            }
        if activate_on_init:
            self.activate()

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.deactivate()

    def _exec_activation(self, exec_function):
        """Execute extension (de)activation & return boolean of state."""
        result = exec_function(self.code)
        LOG.log(self.result_log_level_map.get(result, 0),
                self.result_log_message_map[result])
        return self.result_activated_map[result]

    def activate(self):
        """Activate extension."""
        self.activated = self._exec_activation(arcpy.CheckOutExtension)
        return self.activated

    def deactivate(self):
        """Deactivate extension."""
        self.activated = self._exec_activation(arcpy.CheckInExtension)
        return not self.activated


class DatasetView(object):
    """Context manager for an ArcGIS dataset view (feature layer/table view)."""

    def __init__(self, dataset_path, dataset_where_sql=None, view_name=None,
                 force_nonspatial=False):
        """Initialize instance.

        Args:
            dataset_path (str): The path of the dataset.
            dataset_where_sql (str): The SQL where-clause for dataset
                subselection.
            view_name (str): The name of the view to create.
            force_nonspatial (bool): The flag that forces a nonspatial view.
        """
        self.name = view_name if view_name else helpers.unique_name('view')
        self.dataset_path = dataset_path
        self.dataset_meta = dataset_metadata(dataset_path)
        self.is_spatial = all((self.dataset_meta['is_spatial'],
                               not force_nonspatial))
        self._where_sql = dataset_where_sql
        self.activated = self.create()
        return

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.discard()

    @property
    def where_sql(self):
        """SQL where-clause property of dataset view."""
        return self._where_sql

    @where_sql.setter
    def where_sql(self, value):
        if self.activated:
            arcpy.management.SelectLayerByAttribute(
                in_layer_or_view=self.name, selection_type='new_selection',
                where_clause=value
                )
        self._where_sql = value
        return

    @where_sql.deleter
    def where_sql(self):
        if self.activated:
            arcpy.management.SelectLayerByAttribute(
                in_layer_or_view=self.name, selection_type='clear_selection',
                )
        self._where_sql = None
        return

    def as_chunks(self, chunk_size):
        """Generate 'chunks' of the view's data as new DatasetView.

        Yields DatasetView with context management, i.e. view will be discarded
        when generator moves to next chunk-view.

        Args:
            chunk_size (int): Number of features in each chunk-view.
        Yields:
            DatasetView.
        """
        # ArcPy where clauses cannot use 'between'.
        chunk_where_sql_template = ("{oid_field_name} >= {from_oid}"
                                    " and {oid_field_name} <= {to_oid}")
        if self.where_sql:
            chunk_where_sql_template += " and ({})".format(self.where_sql)
        # Get iterable of all object IDs in dataset.
        with arcpy.da.SearchCursor(in_table=self.dataset_path,
                                   field_names=('oid@',),
                                   where_clause=self.where_sql) as cursor:
            # Sorting is important: allows selection by ID range.
            oids = sorted(oid for oid, in cursor)
        while oids:
            chunk_where_sql = chunk_where_sql_template.format(
                oid_field_name=self.dataset_meta['oid_field_name'],
                from_oid=min(oids), to_oid=max(oids[:chunk_size])
                )
            with DatasetView(self.name, chunk_where_sql) as chunk_view:
                yield chunk_view
            # Remove chunk from set.
            oids = oids[chunk_size:]

    def create(self):
        """Create view."""
        if self.is_spatial:
            function = arcpy.management.MakeFeatureLayer
        else:
            function = arcpy.management.MakeTableView
        function(self.dataset_path, self.name, where_clause=self.where_sql,
                 workspace=self.dataset_meta['workspace_path'])
        return

    def discard(self):
        """Discard view."""
        if arcpy.Exists(self.name):
            arcpy.management.Delete(self.name)
        self.activated = False
        return


class TempDatasetCopy(object):
    """Context manager for a temporary dataset copy."""

    def __init__(self, dataset_path, dataset_where_sql=None, output_path=None,
                 force_nonspatial=False):
        """Initialize instance.

        Notes:
            To make a temp dataset without copying template rows:
            `dataset_where_sql="0=1"`

        Args:
            dataset_path (str): The path of the dataset to copy.
            dataset_where_sql (str): The SQL where-clause for dataset
                subselection.
            output_path (str): The path of the dataset to create.
            force_nonspatial (bool): The flag that forces a nonspatial copy.
        """
        self.path = (output_path if output_path
                     else helpers.unique_temp_dataset_path('temp'))
        self.dataset_path = dataset_path
        self.dataset_meta = dataset_metadata(dataset_path)
        self.is_spatial = all((self.dataset_meta['is_spatial'],
                               not force_nonspatial))
        self.where_sql = dataset_where_sql
        self.activated = self.create()
        return

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.discard()

    def create(self):
        """Create dataset."""
        if self.is_spatial:
            function = arcpy.management.CopyFeatures
        else:
            function = arcpy.management.CopyRows
        with DatasetView(self.dataset_path, dataset_where_sql=self.where_sql,
                         force_nonspatial=not self.is_spatial) as dataset_view:
            function(dataset_view.name, self.path)
        return

    def discard(self):
        """Discard dataset."""
        if arcpy.Exists(self.path):
            arcpy.management.Delete(self.path)
        self.activated = False
        return


def _domain_object_metadata(domain_object):
    """Return dictionary of metadata from ArcPy domain object."""
    meta = {
        'arc_object': domain_object,
        'name': getattr(domain_object, 'name'),
        'description': getattr(domain_object, 'description'),
        'owner': getattr(domain_object, 'owner'),
        #'domain_type': getattr(domain_object, 'domainType'),
        'is_coded_value': getattr(domain_object, 'domainType') == 'CodedValue',
        'is_range': getattr(domain_object, 'domainType') == 'Range',
        #'merge_policy': getattr(domain_object, 'mergePolicy'),
        #'split_policy': getattr(domain_object, 'splitPolicy'),
        'code_description_map': getattr(domain_object, 'codedValues', {}),
        'range': getattr(domain_object, 'range', tuple()),
        'type': getattr(domain_object, 'type'),
        }
    return meta


def _field_object_metadata(field_object):
    """Return dictionary of metadata from ArcPy field object."""
    meta = {
        'arc_object': field_object,
        'name': getattr(field_object, 'name'),
        'alias_name': getattr(field_object, 'aliasName'),
        'base_name': getattr(field_object, 'baseName'),
        'type': getattr(field_object, 'type').lower(),
        'length': getattr(field_object, 'length'),
        'precision': getattr(field_object, 'precision'),
        'scale': getattr(field_object, 'scale'),
        }
    return meta


def dataset_metadata(dataset_path):
    """Return dictionary of dataset metadata.

    Args:
        dataset_path (str): Path of dataset.
    Returns:
        dict.
    """
    arc_object = arcpy.Describe(dataset_path)
    meta = {
        'arc_object': arc_object,
        'name': getattr(arc_object, 'name'),
        'path': getattr(arc_object, 'catalogPath'),
        'data_type': getattr(arc_object, 'dataType'),
        'workspace_path': getattr(arc_object, 'path'),
        # Do not use getattr! Tables sometimes don't have OIDs.
        'is_table': hasattr(arc_object, 'hasOID'),
        'is_versioned': getattr(arc_object, 'isVersioned', False),
        'oid_field_name': getattr(arc_object, 'OIDFieldName', None),
        'is_spatial': hasattr(arc_object, 'shapeType'),
        'geometry_type': getattr(arc_object, 'shapeType', None),
        'geometry_field_name': getattr(arc_object, 'shapeFieldName', None),
        }
    meta['field_names'] = tuple(field.name for field
                                in getattr(arc_object, 'fields', ()))
    meta['fields'] = tuple(_field_object_metadata(field) for field
                           in getattr(arc_object, 'fields', ()))
    meta['user_field_names'] = tuple(
        name for name in meta['field_names']
        if name != meta['oid_field_name']
        and '{}.'.format(meta['geometry_field_name']) not in name
        )
    meta['user_fields'] = tuple(
        field for field in meta['fields']
        if field['name'] != meta['oid_field_name']
        and '{}.'.format(meta['geometry_field_name']) not in field['name']
        )
    if hasattr(arc_object, 'spatialReference'):
        meta['spatial_reference'] = getattr(arc_object, 'spatialReference')
        meta['spatial_reference_id'] = getattr(meta['spatial_reference'],
                                               'factoryCode')
    else:
        meta['spatial_reference'] = None
        meta['spatial_reference_id'] = None
    return meta


def domain_metadata(domain_name, workspace_path):
    """Return dictionary of dataset metadata.

    Args:
        dataset_path (str): Path of dataset.
    Returns:
        dict.
    """
    meta = _domain_object_metadata(
        next(domain for domain in arcpy.da.ListDomains(workspace_path)
             if domain.name.lower() == domain_name.lower())
        )
    return meta


def field_metadata(dataset_path, field_name):
    """Return dictionary of field metadata.

    Field name is case-insensitive.

    Args:
        dataset_path (str): Path of dataset.
        field_name (str): Name of field.
    Returns:
        dict.
    """
    try:
        meta = _field_object_metadata(
            arcpy.ListFields(dataset=dataset_path, wild_card=field_name)[0]
            )
    except IndexError:
        raise AttributeError(
            "Field {} not present on {}".format(field_name, dataset_path)
            )
    return meta


def linear_unit_string(measure, spatial_reference_source):
    """Return unit of measure as a linear unit string."""
    linear_unit = getattr(spatial_reference(spatial_reference_source),
                          'linearUnitName', 'Unknown'),
    return '{} {}'.format(measure, linear_unit)


def python_type(type_description):
    """Return instance of Python type from Arc type description."""
    instance = {
        'double': float, 'single': float,
        'integer': int, 'long': int, 'short': int, 'smallinteger': int,
        'guid': uuid.UUID,
        'string': str, 'text': str,
        }
    return instance[type_description]


def spatial_reference_metadata(item):
    """Return dictionary of spatial reference metadata."""
    ##TODO: Finish stub.
    ##https://pro.arcgis.com/en/pro-app/arcpy/classes/spatialreference.htm
    arc_object = spatial_reference(item)
    meta = {
        'arc_object': arc_object,
        'spatial_reference_id': arc_object.factoryCode,
        'angular_unit': getattr(arc_object, 'angularUnitName', None),
        'linear_unit': getattr(arc_object, 'linearUnitName', None),
        }
    return meta


def spatial_reference(item):
    """Return ArcPy spatial reference object from a Python reference.

    Args:
        item (int): Spatial reference ID.
             (str): Path of reference dataset/file.
             (arcpy.Geometry): Reference geometry object.
    Returns:
        arcpy.SpatialReference.
    """
    if item is None:
        arc_object = None
    elif isinstance(item, int):
        arc_object = arcpy.SpatialReference(item)
    elif isinstance(item, arcpy.Geometry):
        arc_object = getattr(item, 'spatialReference')
    else:
        arc_object = getattr(arcpy.Describe(item), 'spatialReference')
    return arc_object


def workspace_metadata(workspace_path):
    """Return dictionary of workspace metadata.

    Args:
        workspace_path (str): Path of workspace.
    Returns:
        dict.
    """
    ##TODO: Finish stub.
    ##http://pro.arcgis.com/en/pro-app/arcpy/functions/workspace-properties.htm
    arc_object = arcpy.Describe(workspace_path)
    prog_id = getattr(arc_object, 'workspaceFactoryProgID', '')
    meta = {
        'arc_object': arc_object,
        'name': getattr(arc_object, 'name'),
        'path': getattr(arc_object, 'catalogPath'),
        'data_type': getattr(arc_object, 'dataType'),
        'is_geodatabase': any(['AccessWorkspace' in prog_id,
                               'FileGDBWorkspace' in prog_id,
                               'SdeWorkspace' in prog_id]),
        'is_folder': prog_id == '',
        'is_file_geodatabase': 'FileGDBWorkspace' in prog_id,
        'is_enterprise_database': 'SdeWorkspace' in prog_id,
        'is_personal_geodatabase': 'AccessWorkspace' in prog_id,
        'is_in_memory': 'InMemoryWorkspace' in prog_id,
        'domain_names': tuple(getattr(arc_object, 'domains', ())),
        }
    meta['domains'] = tuple(_domain_object_metadata(domain)
                            for domain in arcpy.da.ListDomains(meta['path']))
    return meta
