"""Interfaces for ArcObjects."""
try:
    from contextlib import ContextDecorator
except ImportError:
    # Py2.
    from contextlib2 import ContextDecorator
import datetime
import logging
import uuid

import arcpy

from arcproc import geometry
from arcproc.helpers import unique_name, unique_path


LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""

arcpy.SetLogHistory(False)


class ArcExtension(ContextDecorator):
    """Context manager for an ArcGIS extension.

    Attributes:
        name (str): Name of extension. Currently, name is same as code.
        code (str): Internal code for extension.
        activated (bool): Flag to indicate extension is activated or not.
    """

    _result = {
        "CheckedIn": {
            "activated": False,
            "message": "Extension deactivated.",
            "log_level": logging.INFO,
        },
        "CheckedOut": {
            "activated": True,
            "message": "Extension activated.",
            "log_level": logging.INFO,
        },
        "Failed": {
            "activated": False,
            "message": "System failure.",
            "log_level": logging.WARNING,
        },
        "NotInitialized": {
            "activated": False,
            "message": "No desktop license set.",
            "log_level": logging.WARNING,
        },
        "Unavailable": {
            "activated": False,
            "message": "Extension unavailable.",
            "log_level": logging.WARNING,
        },
    }
    """dict: Information mapped to each extension result string."""

    def __init__(self, name):
        """Initialize instance.

        Args:
            name (str): Name of extension.
        """
        # For now assume name & code are same.
        self.name = name
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
            bool: True if extension is activated, False if deactivated/failure.
        """
        result = self._result[exec_function(self.code)]
        LOG.log(result["log_level"], result["message"])
        return result["activated"]

    def activate(self):
        """Activate extension.

        Returns:
            bool: Indicator that extension is activated (or not).
        """
        self.activated = self._exec_activation(arcpy.CheckOutExtension)
        return self.activated

    def deactivate(self):
        """Deactivate extension.

        Returns:
            bool: Indicator that extension is deactivated (or not).
        """
        self.activated = self._exec_activation(arcpy.CheckInExtension)
        return not self.activated


class DatasetView(ContextDecorator):
    """Context manager for an ArcGIS dataset view (feature layer/table view).

    Attributes:
        name (str): Name of view.
        dataset_path (str): Path of dataset.
        dataset_meta (dict): Metadata dictionary for dataset.
        field_names (list): Collection of field names to include in view.
        is_spatial (bool): True if view is spatial, False if not.
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
        self.name = kwargs.get("view_name", unique_name("view"))
        self.dataset_path = dataset_path
        self.dataset_meta = dataset_metadata(dataset_path)
        self.field_names = list(
            kwargs.get("field_names", self.dataset_meta["field_names"])
        )
        self.is_spatial = all(
            [self.dataset_meta["is_spatial"], not kwargs.get("force_nonspatial", False)]
        )
        self._where_sql = dataset_where_sql

    def __enter__(self):
        self.create()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.discard()

    @property
    def count(self):
        """int: Number of features in view."""
        return int(arcpy.management.GetCount(self.name).getOutput(0))

    @property
    def exists(self):
        """bool: True if view currently exists, False otherwise."""
        return arcpy.Exists(self.name)

    @property
    def field_info(self):
        """arcpy.FieldInfo: Field info object for field settings for the view."""
        cmp_field_names = [name.lower() for name in self.field_names]
        field_info = arcpy.FieldInfo()
        for field_name in self.dataset_meta["field_names"]:
            visible = "VISIBLE" if field_name.lower() in cmp_field_names else "HIDDEN"
            field_info.addField(field_name, field_name, visible, "NONE")
        return field_info

    @property
    def where_sql(self):
        """str: SQL where-clause property of dataset view subselection.

        Setting this property will change dataset subselection for the view.
        """
        return self._where_sql

    @where_sql.setter
    def where_sql(self, value):
        if self.exists:
            arcpy.management.SelectLayerByAttribute(
                in_layer_or_view=self.name,
                selection_type="new_selection",
                where_clause=value,
            )
        self._where_sql = value

    @where_sql.deleter
    def where_sql(self):
        if self.exists:
            arcpy.management.SelectLayerByAttribute(
                in_layer_or_view=self.name, selection_type="clear_selection"
            )
        self._where_sql = None

    def as_chunks(self, chunk_size):
        """Generate "chunks" of view features in new DatasetView.

        DatasetView yielded under context management, i.e. view will be discarded
        when generator moves to next chunk-view.

        Args:
            chunk_size (int): Number of features in each chunk-view.

        Yields:
            DatasetView.
        """
        # ArcPy where clauses cannot use `between`.
        where_sql_template = (
            "{oid_field_name} >= {from_oid} and {oid_field_name} <= {to_oid}"
        )
        if self.where_sql:
            where_sql_template += " and ({})".format(self.where_sql)
        # Get iterable of all object IDs in dataset.
        cursor = arcpy.da.SearchCursor(
            in_table=self.dataset_path,
            field_names=["oid@"],
            where_clause=self.where_sql,
        )
        with cursor:
            # Sorting is important: allows selection by ID range.
            oids = sorted(oid for oid, in cursor)
        while oids:
            chunk_where_sql = where_sql_template.format(
                oid_field_name=self.dataset_meta["oid_field_name"],
                from_oid=min(oids),
                to_oid=max(oids[:chunk_size]),
            )
            with DatasetView(self.name, chunk_where_sql) as chunk_view:
                yield chunk_view

            # Remove chunk from set.
            oids = oids[chunk_size:]

    def create(self):
        """Create view.

        Returns:
            bool: True if view created, False otherwise.
        """
        if self.is_spatial:
            _create = arcpy.management.MakeFeatureLayer
        else:
            _create = arcpy.management.MakeTableView
        _create(
            self.dataset_path,
            self.name,
            where_clause=self.where_sql,
            workspace=self.dataset_meta["workspace_path"],
            field_info=self.field_info,
        )
        return self.exists

    def discard(self):
        """Discard view.

        Returns:
            bool: True if view discarded, False otherwise.
        """
        if self.exists:
            arcpy.management.Delete(self.name)
        return not self.exists


class Editor(ContextDecorator):
    """Context manager for editing features.

    Attributes:
        workspace_path (str):  Path for the editing workspace
    """

    def __init__(self, workspace_path, use_edit_session=True):
        """Initialize instance.

        Args:
            workspace_path (str): Path for the editing workspace.
            use_edit_session (bool): True if edits are to be made in an edit session,
                False otherwise.
        """
        self._editor = arcpy.da.Editor(workspace_path) if use_edit_session else None
        self.workspace_path = workspace_path

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.stop(save_changes=(False if exception_type else True))

    @property
    def active(self):
        """bool: Flag indicating whether edit session is active."""
        return self._editor.isEditing if self._editor else False

    def start(self):
        """Start an active edit session.

        Returns:
            bool: True if session is active, False otherwise.
        """
        if self._editor and not self._editor.isEditing:
            self._editor.startEditing(with_undo=True, multiuser_mode=True)
            self._editor.startOperation()
        return self.active

    def stop(self, save_changes=True):
        """Stop an active edit session.

        Args:
            save_changes (bool): True if edits should be saved, False otherwise.

        Returns:
            bool: True if session not active, False otherwise.
        """
        if self._editor and self._editor.isEditing:
            if save_changes:
                self._editor.stopOperation()
            else:
                self._editor.abortOperation()
            self._editor.stopEditing(save_changes)
        return not self.active


class TempDatasetCopy(ContextDecorator):
    """Context manager for a temporary copy of a dataset.

    Attributes:
        path (str): Path of the dataset copy.
        dataset_path (str): Path of the original dataset.
        dataset_meta (dict): Metadata dictionary for the original dataset.
        field_names (list): Field names to include in copy.
        is_spatial (bool): Flag indicating if the view is spatial.
        where_sql (str): SQL where-clause property of copy subselection.
    """

    def __init__(self, dataset_path, dataset_where_sql=None, **kwargs):
        """Initialize instance.

        Note:
            To make a temp dataset without copying any template rows:
            `dataset_where_sql="0=1"`

        Args:
            dataset_path (str): Path of dataset to copy.
            dataset_where_sql (str): SQL where-clause for dataset subselection.
            **kwargs: Arbitrary keyword arguments. See below.

        Keyword Args:
            output_path (str): Path of the dataset to create.  Default is None (auto-
                generate path)
            field_names (iter): Field names to include in copy. If field_names not
                specified, all fields will be included.
            force_nonspatial (bool): True to force a nonspatial copy, False otherwise.
                Default is False.
        """
        self.path = kwargs.get("output_path", unique_path("temp"))
        self.dataset_path = dataset_path
        self.dataset_meta = dataset_metadata(dataset_path)
        self.field_names = list(
            kwargs.get("field_names", self.dataset_meta["field_names"])
        )
        self.is_spatial = all(
            [self.dataset_meta["is_spatial"], not kwargs.get("force_nonspatial", False)]
        )
        self.where_sql = dataset_where_sql

    def __enter__(self):
        self.create()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.discard()

    @property
    def exists(self):
        """bool: True if dataset currently exists, False otherwise."""
        return arcpy.Exists(self.path)

    def create(self):
        """Create dataset.

        Returns:
            bool: True if copy created, False otherwise.
        """
        if self.is_spatial:
            _create = arcpy.management.CopyFeatures
        else:
            _create = arcpy.management.CopyRows
        view = DatasetView(
            self.dataset_path,
            dataset_where_sql=self.where_sql,
            field_names=self.field_names,
            force_nonspatial=(not self.is_spatial),
        )
        with view:
            _create(view.name, self.path)
        return self.exists

    def discard(self):
        """Discard dataset.

        Returns:
            bool: True if copy discarded, False otherwise.
        """
        if self.exists:
            arcpy.management.Delete(self.path)
        return not self.exists


def _dataset_object_metadata(dataset_object):
    """Return mapping of dataset metadata key to value.

    Args:
        dataset_object: ArcPy geoprocessing describe data object for dataset.

    Returns:
        dict.
    """
    meta = {"object": dataset_object}
    meta["name"] = getattr(meta["object"], "name", None)
    meta["path"] = getattr(meta["object"], "catalogPath")
    meta["data_type"] = getattr(meta["object"], "dataType")
    meta["workspace_path"] = getattr(meta["object"], "path")
    # Do not use getattr here! Tables sometimes don"t have OIDs.
    meta["is_table"] = hasattr(meta["object"], "hasOID")
    meta["is_versioned"] = getattr(meta["object"], "isVersioned", False)
    meta["oid_field_name"] = getattr(meta["object"], "OIDFieldName", None)
    meta["is_spatial"] = hasattr(meta["object"], "shapeType")
    meta["geometry_type"] = getattr(meta["object"], "shapeType", None)
    meta["geom_type"] = meta["geometry_type"]
    meta["geometry_field_name"] = getattr(meta["object"], "shapeFieldName", None)
    meta["geom_field_name"] = meta["geometry_field_name"]
    for key in ["area", "length"]:
        meta[key + "_field_name"] = getattr(meta["object"], key + "FieldName", None)
        if meta[key + "_field_name"] == "":
            meta[key + "_field_name"] = None
    meta["field_token"] = {}
    system_field_tokens = {
        "oid": "oid@",
        "geom": "shape@",
        "area": "shape@area",
        "length": "shape@length",
    }
    for key, token in system_field_tokens.items():
        if meta[key + "_field_name"]:
            meta["field_token"][meta[key + "_field_name"]] = token
    meta["fields"] = [
        _field_object_metadata(field) for field in getattr(meta["object"], "fields", [])
    ]
    meta["field_names"] = [field["name"] for field in meta["fields"]]
    meta["field_names_tokenized"] = [
        meta["field_token"].get(name, name) for name in meta["field_names"]
    ]
    meta["user_fields"] = [
        field
        for field in meta["fields"]
        if field["name"]
        not in [meta[key + "_field_name"] for key in system_field_tokens]
    ]
    meta["user_field_names"] = [field["name"] for field in meta["user_fields"]]
    if hasattr(meta["object"], "spatialReference"):
        # Must do the SpatialReference call, to convert "geoprocessing spatial reference
        # object" (which cannot be used as spatial reference in ArcPy GP tools). to
        # "SpatialReference object" (which can).
        meta["spatial_reference"] = arcpy.SpatialReference(
            getattr(getattr(meta["object"], "spatialReference"), "factoryCode")
        )
        meta["spatial_reference_id"] = getattr(meta["spatial_reference"], "factoryCode")
    else:
        meta["spatial_reference"] = None
        meta["spatial_reference_id"] = None
    return meta


def _domain_object_metadata(domain_object):
    """Return mapping of domain metadata key to value.

    Args:
        domain_object: Workspace domain object.

    Returns:
        dict.
    """
    meta = {"object": domain_object}
    meta["name"] = getattr(meta["object"], "name")
    meta["description"] = getattr(meta["object"], "description")
    meta["owner"] = getattr(meta["object"], "owner")
    meta["is_coded_value"] = getattr(meta["object"], "domainType") == "CodedValue"
    meta["is_range"] = getattr(meta["object"], "domainType") == "Range"
    # meta["merge_policy"] = getattr(meta["object"], "mergePolicy")
    # meta["split_policy"] = getattr(meta["object"], "splitPolicy")
    meta["code_description_map"] = getattr(meta["object"], "codedValues", {})
    meta["range"] = getattr(meta["object"], "range", [])
    meta["type"] = getattr(meta["object"], "type")
    return meta


def _field_object_metadata(field_object):
    """Return mapping of field metadata key to value.

    Args:
        field_object (arcpy.Field): ArcPy field object.

    Returns:
        dict.
    """
    meta = {"object": field_object}
    meta["name"] = getattr(meta["object"], "name")
    meta["alias_name"] = getattr(meta["object"], "aliasName")
    meta["base_name"] = getattr(meta["object"], "baseName")
    meta["type"] = getattr(meta["object"], "type").lower()
    meta["length"] = getattr(meta["object"], "length")
    meta["precision"] = getattr(meta["object"], "precision")
    meta["scale"] = getattr(meta["object"], "scale")
    meta["default_value"] = getattr(meta["object"], "defaultValue")
    return meta


def _workspace_object_metadata(workspace_object):
    """Return mapping of workspace metadata key to value.

    Args:
        workspace_object: ArcPy geoprocessing describe data object for workspace.

    Returns:
        dict.
    """
    meta = {"object": workspace_object}
    meta["factory_prog_id"] = getattr(meta["object"], "workspaceFactoryProgID", "")
    meta["name"] = getattr(meta["object"], "name")
    meta["path"] = getattr(meta["object"], "catalogPath")
    meta["data_type"] = getattr(meta["object"], "dataType")
    meta["is_geodatabase"] = any(
        [
            "AccessWorkspace" in meta["factory_prog_id"],
            "FileGDBWorkspace" in meta["factory_prog_id"],
            "SdeWorkspace" in meta["factory_prog_id"],
        ]
    )
    meta["is_folder"] = meta["factory_prog_id"] == ""
    meta["is_file_geodatabase"] = "FileGDBWorkspace" in meta["factory_prog_id"]
    meta["is_enterprise_database"] = "SdeWorkspace" in meta["factory_prog_id"]
    meta["is_personal_geodatabase"] = "AccessWorkspace" in meta["factory_prog_id"]
    meta["is_in_memory"] = "InMemoryWorkspace" in meta["factory_prog_id"]
    meta["domains"] = [
        _domain_object_metadata(domain) for domain in arcpy.da.ListDomains(meta["path"])
    ]
    meta["domain_names"] = [domain["name"] for domain in meta["domains"]]
    return meta


def dataset_metadata(dataset_path):
    """Return mapping of dataset metadata key to value.

    Args:
        dataset_path (str): Path of the dataset.

    Returns:
        dict.
    """
    return _dataset_object_metadata(arcpy.Describe(dataset_path))


def domain_metadata(domain_name, workspace_path):
    """Return mapping dataset metadata key to value.

    Args:
        domain_name (str): Name of domain.
        workspace_path (str): Path of workspace domain is in.

    Returns:
        dict.
    """
    domain_object = next(
        domain
        for domain in arcpy.da.ListDomains(workspace_path)
        if domain.name.lower() == domain_name.lower()
    )
    return _domain_object_metadata(domain_object)


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
        field_object = arcpy.ListFields(dataset=dataset_path, wild_card=field_name)[0]
    except IndexError:
        raise AttributeError(
            "Field {} not present on {}".format(field_name, dataset_path)
        )

    return _field_object_metadata(field_object)


def linear_unit(measure_string, spatial_reference_item):
    """Return linear unit of measure in reference units from string.

    Args:
        unit_string (str): String description of linear unit of measure.
        spatial_reference_item: Item from which the linear unit"s spatial
            reference will be derived.

    Returns:
        float: Unit of measure in spatial reference"s units.

    """
    str_measure, str_unit = measure_string.split(" ")
    reference_unit = getattr(
        spatial_reference(spatial_reference_item), "linearUnitName", "Unknown"
    )
    meter_measure = float(str_measure) * geometry.RATIO["meter"][str_unit.lower()]
    measure = meter_measure / geometry.RATIO["meter"][reference_unit.lower()]
    return measure


def linear_unit_string(measure, spatial_reference_item):
    """Return linear unit of measure as a string description.

    Args:
        measure (float, int, str): Count of measure.
        spatial_reference_item: Item from which spatial reference for the linear unit
            will be derived.

    Returns:
        str.
    """
    reference_unit = getattr(
        spatial_reference(spatial_reference_item), "linearUnitName", "Unknown"
    )
    return "{} {}".format(measure, reference_unit)


def python_type(type_description):
    """Return object representing the Python type.

    Args:
        type_description (str): Arc-style type description/code.

    Returns:
        Python object representing the type.
    """
    instance = {
        "date": datetime.datetime,
        "double": float,
        "single": float,
        "integer": int,
        "long": int,
        "short": int,
        "smallinteger": int,
        "geometry": arcpy.Geometry,
        "guid": uuid.UUID,
        "string": str,
        "text": str,
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
        reference_object = None
    elif isinstance(item, arcpy.SpatialReference):
        reference_object = item
    elif isinstance(item, int):
        reference_object = arcpy.SpatialReference(item)
    elif isinstance(item, arcpy.Geometry):
        reference_object = getattr(item, "spatialReference")
    else:
        reference_object = arcpy.SpatialReference(
            getattr(getattr(arcpy.Describe(item), "spatialReference"), "factoryCode")
        )
    return reference_object


def spatial_reference_metadata(item):
    """Return mapping of spatial reference metadata key to value.

    Args:
        item (int): Spatial reference ID.
             (str): Path of reference dataset/file.
             (arcpy.Geometry): Reference geometry object.

    Returns:
        dict.
    """
    meta = {"object": spatial_reference(item)}
    meta["spatial_reference_id"] = getattr(meta["object"], "factoryCode", None)
    meta["angular_unit"] = getattr(meta["object"], "angularUnitName", None)
    meta["linear_unit"] = getattr(meta["object"], "linearUnitName", None)
    return meta


def workspace_metadata(workspace_path):
    """Return mapping of workspace metadata key to value.

    Args:
        workspace_path (str): Path of workspace.

    Returns:
        dict.
    """
    workspace_object = arcpy.Describe(workspace_path)
    meta = _workspace_object_metadata(workspace_object)
    return meta
