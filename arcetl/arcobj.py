"""Interfaces for ArcObjects."""
import datetime
import logging
import uuid

import arcpy

from arcetl import geometry
from arcetl.helpers import log_level, unique_name, unique_path


LOG = logging.getLogger(__name__)


class ArcExtension(object):
    """Context manager for an ArcGIS extension.

    Attributes:
        name (str): Name of the extension. Currently, name is same as code.
        code (str): Internal code for the extension.
        activated (bool): Flag to indicate extension is activated or not.

    """

    _result = {
        'CheckedIn': {'activated': False,
                      'message': "Extension deactivated.",
                      'log_level': log_level('info')},
        'CheckedOut': {'activated': True,
                       'message': "Extension activated.",
                       'log_level': log_level('info')},
        'Failed': {'activated': False,
                   'message': "System failure.",
                   'log_level': log_level('warning')},
        'NotInitialized': {'activated': False,
                           'message': "No desktop license set.",
                           'log_level': log_level('warning')},
        'Unavailable': {'activated': False,
                        'message': "Extension unavailable.",
                        'log_level': log_level('warning')},
    }
    """dict: Information mapped to each extension result string."""

    def __init__(self, name):
        """Initialize instance.

        Args:
            name (str): Name of the extension.

        """
        self.name = name
        # For now assume name & code are same.
        self.code = name
        self.activated = False

    def __enter__(self):
        self.activate()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.deactivate()

    def _exec_activation(self, exec_function):
        """Execute extension (de)activation & return boolean of state.

        Args:
            exec_function: Function or method to call for (de)activation.

        Returns:
            bool: Indicator that extension is activated (True) or deactivated/
                failure (False).

        """
        result = self._result[exec_function(self.code)]
        LOG.log(result['log_level'], result['message'])
        return result['activated']

    def activate(self):
        """Activate extension.

        Returns:
            bool: Indicator that extension is activated or not.

        """
        self.activated = self._exec_activation(arcpy.CheckOutExtension)
        return self.activated

    def deactivate(self):
        """Deactivate extension.

        Returns:
            bool: Indicator that extension is deactivated or not.

        """
        self.activated = self._exec_activation(arcpy.CheckInExtension)
        return not self.activated


class DatasetView(object):
    """Context manager for an ArcGIS dataset view (feature layer/table view).

    Attributes:
        name (str): Name of view.
        dataset_path (str): Path of dataset.
        field_names (list): Collection of field names to include in view.
        dataset_meta (dict): Metadata dictionary for dataset.
        is_spatial (bool): Flag indicating if the view is spatial.

    """

    def __init__(self, dataset_path, dataset_where_sql=None, **kwargs):
        """Initialize instance.

        Args:
            dataset_path (str): Path of dataset.
            dataset_where_sql (str): SQL where-clause for dataset subselection.
            **kwargs: Arbitrary keyword arguments. See below.

        Keyword Args:
            view_name (str): Name of view. Default is None (auto-generate name).
            field_names (iter): Collection of field names to include in view. If
                field_names not specified, all fields will be included.
            force_nonspatial (bool): Flag that forces a nonspatial view. Default is
                False.

        """
        self.name = kwargs.get('view_name', unique_name('view'))
        self.dataset_path = dataset_path
        self.dataset_meta = dataset_metadata(dataset_path)
        self.is_spatial = all((self.dataset_meta['is_spatial'],
                               not kwargs.get('force_nonspatial', False)))
        self.field_names = list(kwargs.get('field_names',
                                           self.dataset_meta['field_names']))
        self._where_sql = dataset_where_sql

    def __enter__(self):
        self.create()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.discard()

    @property
    def count(self):
        """int: Number of features in the view."""
        return int(arcpy.management.GetCount(self.name).getOutput(0))

    @property
    def exists(self):
        """bool: Flag indicating the view currently exists."""
        return arcpy.Exists(self.name)

    @property
    def field_info(self):
        """arcpy.FieldInfo: Field info object for view's field settings."""
        field_info = arcpy.FieldInfo()
        for field_name in self.dataset_meta['field_names']:
            visible = ('VISIBLE' if field_name.lower()
                       in (fn.lower() for fn in self.field_names) else 'HIDDEN')
            field_info.addField(field_name, field_name, visible, 'NONE')
        return field_info

    @property
    def where_sql(self):
        """str: SQL where-clause property of dataset view subselection.

        Setting this property will change the view's dataset subselection.

        """
        return self._where_sql

    @where_sql.setter
    def where_sql(self, value):
        if self.exists:
            arcpy.management.SelectLayerByAttribute(
                in_layer_or_view=self.name, selection_type='new_selection',
                where_clause=value
                )
        self._where_sql = value

    @where_sql.deleter
    def where_sql(self):
        if self.exists:
            arcpy.management.SelectLayerByAttribute(
                in_layer_or_view=self.name, selection_type='clear_selection',
                )
        self._where_sql = None

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
        """Create view.

        Returns:
            bool: True if view was created, False otherwise.

        """
        function = (arcpy.management.MakeFeatureLayer if self.is_spatial
                    else arcpy.management.MakeTableView)
        function(self.dataset_path, self.name, where_clause=self.where_sql,
                 workspace=self.dataset_meta['workspace_path'],
                 field_info=self.field_info)
        return self.exists

    def discard(self):
        """Discard view.

        Returns:
            bool: True if view was discarded, False otherwise.

        """
        if self.exists:
            arcpy.management.Delete(self.name)
        return not self.exists


class Editor(object):
    """Context manager for editing features.

    Attributes:
        workspace_path (str):  Path for the editing workspace

    """

    def __init__(self, workspace_path, use_edit_session=True):
        """Initialize instance.

        Args:
            workspace_path (str): Path for the editing workspace.
            use_edit_session (bool): Flag directing edits to be made in an
                edit session. Default is True.

        """
        self._editor = (arcpy.da.Editor(workspace_path) if use_edit_session else None)
        self.workspace_path = workspace_path

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.stop(save_changes=False if exception_type else True)

    @property
    def active(self):
        """bool: Flag indicating whether edit session is active."""
        if self._editor:
            _active = self._editor.isEditing
        else:
            _active = False
        return _active

    def start(self):
        """Start an active edit session.

        Returns:
            bool: Indicator that session is active.

        """
        if self._editor and not self._editor.isEditing:
            self._editor.startEditing(with_undo=True, multiuser_mode=True)
            self._editor.startOperation()
        return self.active

    def stop(self, save_changes=True):
        """Stop an active edit session.

        Args:
            save_changes (bool): Flag indicating whether edits should be
                saved.

        Returns:
            bool: Indicator that session is not active.

        """
        if self._editor and self._editor.isEditing:
            if save_changes:
                self._editor.stopOperation()
            else:
                self._editor.abortOperation()
            self._editor.stopEditing(save_changes)
        return not self.active


class TempDatasetCopy(object):
    """Context manager for a temporary copy of a dataset.

    Attributes:
        path (str): Path of the dataset copy.
        dataset_path (str): Path of the original dataset.
        dataset_meta (dict): Metadata dictionary for the original dataset.
        is_spatial (bool): Flag indicating if the view is spatial.
        where_sql (str): SQL where-clause property of copy subselection.
        activated (bool): Flag indicating whether the temporary copy is
            activated.

    """

    def __init__(self, dataset_path, dataset_where_sql=None, output_path=None,
                 force_nonspatial=False):
        """Initialize instance.

        Note:
            To make a temp dataset without copying template rows:
            `dataset_where_sql="0=1"`

        Args:
            dataset_path (str): Path of the dataset to copy.
            dataset_where_sql (str): SQL where-clause for dataset
                subselection.
            output_path (str): Path of the dataset to create.
            force_nonspatial (bool): Flag that forces a nonspatial copy.
        """
        self.path = output_path if output_path else unique_path('temp')
        self.dataset_path = dataset_path
        self.dataset_meta = dataset_metadata(dataset_path)
        self.is_spatial = all((self.dataset_meta['is_spatial'],
                               not force_nonspatial))
        self.where_sql = dataset_where_sql

    def __enter__(self):
        self.create()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.discard()

    @property
    def exists(self):
        """bool: Flag indicating the dataset currently exists."""
        return arcpy.Exists(self.path)

    def create(self):
        """Create dataset."""
        function = (arcpy.management.CopyFeatures if self.is_spatial
                    else arcpy.management.CopyRows)
        with DatasetView(self.dataset_path, dataset_where_sql=self.where_sql,
                         force_nonspatial=not self.is_spatial) as dataset_view:
            function(dataset_view.name, self.path)
        return self.exists

    def discard(self):
        """Discard dataset."""
        if self.exists:
            arcpy.management.Delete(self.path)
        return not self.exists


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
        'range': getattr(domain_object, 'range', ()),
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
        dataset_path (str): Path of the dataset.

    Returns:
        dict: Metadata for dataset.

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
        'geom_type': getattr(arc_object, 'shapeType', None),
        'geometry_field_name': getattr(arc_object, 'shapeFieldName', None),
        'geom_field_name': getattr(arc_object, 'shapeFieldName', None),
    }
    meta['field_token'] = {}
    if meta['oid_field_name']:
        meta['field_token'][meta['oid_field_name']] = 'oid@'
    if meta['geom_field_name']:
        meta['field_token'].update({
            meta['geom_field_name']: 'shape@',
            meta['geom_field_name'] + '_Area': 'shape@area',
            meta['geom_field_name'] + '_Length': 'shape@length',
            meta['geom_field_name'] + '.STArea()': 'shape@area',
            meta['geom_field_name'] + '.STLength()': 'shape@length',
        })
    meta['field_names'] = tuple(field.name for field
                                in getattr(arc_object, 'fields', ()))
    meta['field_names_tokenized'] = tuple(meta['field_token'].get(name, name)
                                          for name in meta['field_names'])
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
        meta['spatial_reference_id'] = getattr(meta['spatial_reference'], 'factoryCode')
    else:
        meta['spatial_reference'] = None
        meta['spatial_reference_id'] = None
    return meta


def domain_metadata(domain_name, workspace_path):
    """Return dictionary of dataset metadata.

    Args:
        domain_name (str): Name of the domain.
        workspace_path (str): Path of the workspace domain is in.

    Returns:
        dict: Metadata for domain.

    """
    domain_object = next(
        domain for domain in arcpy.da.ListDomains(workspace_path)
        if domain.name.lower() == domain_name.lower()
        )
    meta = _domain_object_metadata(domain_object)
    return meta


def field_metadata(dataset_path, field_name):
    """Return dictionary of field metadata.

    Note:
        Field name is case-insensitive.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.

    Returns:
        dict: Metadata for field.

    """
    try:
        field_object = arcpy.ListFields(dataset=dataset_path,
                                        wild_card=field_name)[0]
    except IndexError:
        ##TODO: Replace with new FieldNotPresentError/NoSuchFieldError.
        raise AttributeError(
            "Field {} not present on {}".format(field_name, dataset_path)
            )
    meta = _field_object_metadata(field_object)
    return meta


def linear_unit(measure_string, spatial_reference_item):
    """Calculate linear unit of measure in reference units from string.

    Args:
        unit_string (str): String description of linear unit of measure.
        spatial_reference_item: Item from which the linear unit's spatial
            reference will be derived.

    Returns:
        float: Unit of measure in spatial reference's units.

    """
    s_measure, s_unit = measure_string.split(' ')
    sref_unit = getattr(spatial_reference(spatial_reference_item),
                        'linearUnitName', 'Unknown')
    meter_measure = float(s_measure) * geometry.RATIO['meter'][s_unit.lower()]
    measure = meter_measure / geometry.RATIO['meter'][sref_unit.lower()]
    return measure


def linear_unit_string(measure, spatial_reference_item):
    """Return linear unit of measure as a linear unit string.

    Args:
        measure (float, int, str): Count of measure.
        spatial_reference_item: Item from which the linear unit's spatial
            reference will be derived.

    Returns:
        str: Linear unit as a string.

    """
    sref_unit = getattr(spatial_reference(spatial_reference_item),
                        'linearUnitName', 'Unknown')
    return '{} {}'.format(measure, sref_unit)


def python_type(type_description):
    """Return object representing the Python type.

    Args:
        type_description (str): Arc-style type description/code.

    Returns:
        Python object representing the type.

    """
    instance = {
        'date': datetime.datetime,
        'double': float, 'single': float,
        'integer': int, 'long': int, 'short': int, 'smallinteger': int,
        'geometry': arcpy.Geometry,
        'guid': uuid.UUID,
        'string': str, 'text': str,
    }
    return instance[type_description.lower()]


def spatial_reference(item):
    """Return ArcPy spatial reference object from a Python reference.

    Args:
        item (int): Spatial reference ID.
             (str): Path of reference dataset/file.
             (arcpy.Geometry): Reference geometry object.
             (arcpy.SpatialReference): Spatial reference object.

    Returns:
        arcpy.SpatialReference.

    """
    if item is None:
        arc_object = None
    elif isinstance(item, arcpy.SpatialReference):
        arc_object = item
    elif isinstance(item, int):
        arc_object = arcpy.SpatialReference(item)
    elif isinstance(item, arcpy.Geometry):
        arc_object = getattr(item, 'spatialReference')
    else:
        arc_object = arcpy.SpatialReference(
            getattr(getattr(arcpy.Describe(item), 'spatialReference'), 'factoryCode')
        )
    return arc_object


def spatial_reference_metadata(item):
    """Return dictionary of spatial reference metadata.

    Args:
        item (int): Spatial reference ID.
             (str): Path of reference dataset/file.
             (arcpy.Geometry): Reference geometry object.

    Returns:
        dict: Metadata for the derived spatial reference.

    """
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


def workspace_metadata(workspace_path):
    """Return dictionary of workspace metadata.

    Args:
        workspace_path (str): Path of the workspace.

    Returns:
        dict: Metadata for workspace.

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
        'is_geodatabase': any(('AccessWorkspace' in prog_id,
                               'FileGDBWorkspace' in prog_id,
                               'SdeWorkspace' in prog_id)),
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
