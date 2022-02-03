"""Interfaces for ArcObjects."""
try:
    from contextlib import ContextDecorator
except ImportError:
    # Py2.
    from contextlib2 import ContextDecorator
import datetime
import logging
from pathlib import Path
import uuid

import arcpy

from arcproc import geometry
from arcproc.helpers import unique_name
from arcproc.metadata import Dataset, SpatialReference


LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""

arcpy.SetLogHistory(False)


class DatasetView(ContextDecorator):
    """Context manager for an ArcGIS dataset view (feature layer/table view).

    Attributes:
        name (str): Name of view.
        dataset (arcproc.metadata.Dataset): Metadata instance for dataset.
        dataset_path (pathlib.Path, str): Path of dataset.
        field_names (list): Collection of field names to include in view.
        is_spatial (bool): True if view is spatial, False if not.
    """

    def __init__(self, dataset_path, dataset_where_sql=None, **kwargs):
        """Initialize instance.

        Args:
            dataset_path (pathlib.Path, str): Path of dataset.
            dataset_where_sql (str): SQL where-clause for dataset subselection.
            **kwargs: Arbitrary keyword arguments. See below.

        Keyword Args:
            view_name (str): Name of view. Default is None (auto-generate name).
            field_names (iter): Collection of field names to include in view. If
                field_names not specified or None, all fields will be included.
            force_nonspatial (bool): Flag that forces a nonspatial view. Default is
                False.
        """
        dataset_path = Path(dataset_path)
        self.name = kwargs.get("view_name", unique_name("view"))
        self.dataset = Dataset(dataset_path)
        self.dataset_path = dataset_path
        if kwargs.get("field_names") is None:
            self.field_names = self.dataset.field_names
        else:
            self.field_names = list(kwargs["field_names"])
        self.is_spatial = self.dataset.is_spatial and not kwargs.get(
            "force_nonspatial", False
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
        for field_name in self.dataset.field_names:
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
                selection_type="NEW_SELECTION",
                where_clause=value,
            )
        self._where_sql = value

    @where_sql.deleter
    def where_sql(self):
        if self.exists:
            arcpy.management.SelectLayerByAttribute(
                in_layer_or_view=self.name, selection_type="CLEAR_SELECTION"
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
        # ArcPy where clauses cannot use `BETWEEN`.
        where_sql_template = (
            "{oid_field_name} >= {from_oid} AND {oid_field_name} <= {to_oid}"
        )
        if self.where_sql:
            where_sql_template += f" AND ({self.where_sql})"
        # Get iterable of all object IDs in dataset.
        # ArcPy2.8.0: Convert to str.
        cursor = arcpy.da.SearchCursor(
            in_table=str(self.dataset_path),
            field_names=["OID@"],
            where_clause=self.where_sql,
        )
        with cursor:
            # Sorting is important: allows selection by ID range.
            oids = sorted(oid for oid, in cursor)
        while oids:
            chunk_where_sql = where_sql_template.format(
                oid_field_name=self.dataset.oid_field_name,
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
            func = arcpy.management.MakeFeatureLayer
        else:
            func = arcpy.management.MakeTableView
        kwargs = {
            "where_clause": self.where_sql,
            # ArcPy2.8.0: Convert to str.
            "workspace": str(self.dataset.workspace_path),
            "field_info": self.field_info,
        }
        # ArcPy2.8.0: Convert to str.
        func(str(self.dataset_path), self.name, **kwargs)
        return self.exists

    def discard(self):
        """Discard view.

        Returns:
            bool: True if view discarded, False otherwise.
        """
        if self.exists:
            arcpy.management.Delete(self.name)
        return not self.exists


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
        SpatialReference(spatial_reference_item), "linear_unit", "Unknown"
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
        SpatialReference(spatial_reference_item), "linear_unit", "Unknown"
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
