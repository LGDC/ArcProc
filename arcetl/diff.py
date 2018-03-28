"""Diff operations."""
from collections import defaultdict
import logging
import sys

import arcpy


from arcetl import arcobj
from arcetl import attributes
from arcetl import dataset
from arcetl import geometry
from arcetl import helpers

if sys.version_info.major <= 3:
    basestring = str


LOG = logging.getLogger(__name__)


class Differ(object):
    """Object for tracking feature differences between dataset versions.

    Currently, the geometry diff only works for point geometry. Other
    geometry types will compare the centroid's location.

    Attributes:
        field (dict): Mapping of field type to name(s).
        path (dict): Mapping of dataset type to path(s).
        where_sql (dict): Mapping of dataset type to SQL where clause.
        spatial (dict): Mapping of spatial properties to values.

    """

    _diff_fields_metadata = (
        # {'name': {id_field_name},
        {'name': 'diff_type', 'type': 'text', 'length': 9},
        {'name': 'description', 'type': 'text', 'length': 64},
        {'name': 'init_str', 'type': 'text', 'length': 128},
        {'name': 'new_str', 'type': 'text', 'length': 128},
        )
    """tuple of dicts: Collection of diff table's field metadata."""
    diff_field_names = tuple(field['name'] for field in _diff_fields_metadata)
    """tuple: Ordered collection of diff table's field names."""
    _diff_type_description = {
        'added': "Feature added between init & new dataset.",
        'removed': "Feature removed between init &  new dataset.",
        'geometry': "Feature moved further than {tolerance}.",
        'attribute': "Value in `{field_name}` field changed.",
        'overlay': "Value from `{dataset_path}.{field_name}` overlay changed.",
        }
    """dict: Description string based on diff type."""
    diff_types = tuple(_type for _type in _diff_type_description)
    """tuple: Collection of diff types."""

    def __init__(self, init_dataset_path, new_dataset_path,
                 identifier_field_name, cmp_field_names=None, **kwargs):
        """Initialize instance.

        Args:
            init_dataset_path (str): Path of the initial dataset.
            new_dataset_path (str): Path of the new dataset.
            identifier_field_name (str): Name of the field used to identify a
                feature.
            diff_field_names (iter): Collection of fields to diff attributes
                between datasets. Defaults to None.
            **kwargs: Arbitrary keyword arguments. See below.

        Keyword Args:
            tolerance (float, str): Tolerance for spatial difference. If
                numeric, units will be in init data's units. Default is 0.
            overlay_path_field_map (dict): Mapping of overlay paths to the
                attribute field name to overlay. Default is None.
            init_dataset_where_sql (str): SQL where-clause for inital dataset
                subselection. Default is None.
            new_dataset_where_sql (str): SQL where-clause for new dataset
                subselection. Default is None.

        """
        self.field = {
            'id': identifier_field_name,
            'cmps': set(cmp_field_names) if cmp_field_names else set(),
            }
        self.field['loads'] = {self.field['id'],
                               'shape@xy'} | self.field['cmps']
        self.path = {
            'init': init_dataset_path,
            'new': new_dataset_path,
            }
        self.where_sql = {tag: kwargs.get(tag + '_dataset_where_sql')
                          for tag in ('init', 'new')}
        if 'overlay_path_field_map' in kwargs:
            self.field['overlay'], self.path['overlay'] = [], []
            for path, field in kwargs['overlay_path_field_map'].items():
                self.field['overlay'].append(path)
                self.path['overlay'].append(field)
            self.field['overlay'] = tuple(self.field['overlay'])
            self.path['overlay'] = tuple(self.path['overlay'])
        self.spatial = {
            'reference': arcobj.spatial_reference(init_dataset_path),
            }
        if kwargs.get('tolerance'):
            if isinstance(kwargs['tolerance'], basestring):
                self.spatial['tolerance'] = arcobj.linear_unit(
                    kwargs['tolerance'], self.spatial['reference'],
                    )
            else:
                self.spatial['tolerance'] = kwargs['tolerance']
        else:
            self.spatial['tolerance'] = 0
        self.spatial['tolerance_str'] = arcobj.linear_unit_string(
            self.spatial['tolerance'], self.spatial['reference'],
            )
        self._diffs = {}
        self._displacement_links = ()
        self._id_attr = defaultdict(dict)
        self._ids = {}

    def __enter__(self):
        self.load()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        pass

    @property
    def diffs_added(self):
        """tuple of dict: Diff info-dicts for added features."""
        if 'added' not in self._diffs:
            self._diffs['added'] = tuple(self.info_diff(_id, 'added')
                                         for _id in self.ids_added)
        return self._diffs['added']

    @property
    def diffs_attribute(self):
        """tuple of dict: Diff info-dicts for attribute changes."""
        if 'attribute' not in self._diffs:
            _diffs = []
            for _id in self.ids_persisted:
                for field in self.field['cmps']:
                    init_attr = self._id_attr['init'][_id][field]
                    new_attr = self._id_attr['new'][_id][field]
                    if init_attr == new_attr:
                        continue
                    _diffs.append(
                        self.info_diff(_id, 'attribute', init_attr, new_attr,
                                       field_name=field)
                        )
            self._diffs['attribute'] = tuple(_diffs)
        return self._diffs['attribute']

    @property
    def diffs_geometry(self):
        """tuple of dict: Diff info-dicts for geometry changes."""
        if 'geometry' not in self._diffs:
            _diffs = []
            for _id in self.ids_persisted:
                init_coord = self._id_attr['init'][_id]['shape@xy']
                new_coord = self._id_attr['new'][_id]['shape@xy']
                distance = geometry.coordinate_distance(init_coord, new_coord)
                if distance <= self.spatial['tolerance']:
                    continue
                _diffs.append(
                    self.info_diff(_id, 'geometry', init_coord, new_coord,
                                   tolerance=self.spatial['tolerance_str'])
                    )
            self._diffs['geometry'] = tuple(_diffs)
        return self._diffs['geometry']

    @property
    def diffs_overlay(self):
        """tuple of dict: Diff info-dicts for overlay relationship changes."""
        if 'overlay' not in self._diffs:
            _diffs = []
            for _id in self.ids_persisted:
                for over_field, over_path in zip(self.field['overlay'],
                                                 self.path['overlay']):
                    init_attr = self._id_attr['init'][_id][over_path,
                                                           over_field]
                    new_attr = self._id_attr['new'][_id][over_path,
                                                         over_field]
                    if init_attr == new_attr:
                        continue
                    _diffs.append(
                        self.info_diff(_id, 'overlay', init_attr, new_attr,
                                       dataset_path=over_path,
                                       field_name=over_field)
                        )
            self._diffs['overlay'] = tuple(_diffs)
        return self._diffs['overlay']

    @property
    def diffs_removed(self):
        """tuple of dict: Diff info-dicts for removed features."""
        if 'removed' not in self._diffs:
            self._diffs['removed'] = tuple(self.info_diff(_id, 'removed')
                                           for _id in self.ids_removed)
        return self._diffs['removed']

    @property
    def displacement_links(self):
        """tuple of dicts: Displacement link features for geometry changes."""
        if not self._displacement_links:
            _links = []
            for _id in self.ids_persisted:
                init_coord = self._id_attr['init'][_id]['shape@xy']
                new_coord = self._id_attr['new'][_id]['shape@xy']
                distance = geometry.coordinate_distance(init_coord, new_coord)
                if distance <= self.spatial['tolerance']:
                    continue
                _links.append(
                    self.info_displacement(_id, init_coord, new_coord)
                    )
            self._displacement_links = tuple(_links)
        return self._displacement_links

    @property
    def ids_added(self):
        """set: Feature IDs added between init & new datasets."""
        if 'added' not in self._ids:
            self._ids['added'] = (set(self._id_attr['new'])
                                  - set(self._id_attr['init']))
        return self._ids['added']

    @property
    def ids_persisted(self):
        """set: Feature IDs persisted between init & new datasets."""
        if 'persisted' not in self._ids:
            self._ids['persisted'] = (set(self._id_attr['new'])
                                      & set(self._id_attr['init']))
        return self._ids['persisted']

    @property
    def ids_removed(self):
        """set: Feature IDs removed between init & new datasets."""
        if 'removed' not in self._ids:
            self._ids['removed'] = (set(self._id_attr['init'])
                                    - set(self._id_attr['new']))
        return self._ids['removed']

    def load(self):
        """Load diff review data."""
        # Reset caches.
        for cache in (self._diffs, self._id_attr, self._ids):
            cache.clear()
        self._displacement_links = ()
        for tag in ('init', 'new'):
            g_attrs = attributes.as_dicts(
                dataset_path=self.path[tag], field_names=self.field['loads'],
                dataset_where_sql=self.where_sql[tag],
                spatial_reference_item=self.spatial['reference'],
                )
            for attr in g_attrs:
                _id = attr[self.field['id']]
                self._id_attr[tag][_id] = attr
        # Add overlay attributes.
        for over_field, over_path in zip(self.field['overlay'],
                                         self.path['overlay']):
            for tag in ('init', 'new'):
                with arcobj.DatasetView(self.path[tag],
                                        self.where_sql[tag]) as view:
                    join_kwargs = {
                        'target_features': view.name,
                        'join_features': over_path,
                        'out_feature_class': helpers.unique_path(),
                        'field_mapping': arcpy.FieldMappings(),
                        }
                    for path, field in ((view.name, self.field['id']),
                                        (over_path, over_field)):
                        field_map = arcpy.FieldMap()
                        field_map.addInputField(path, field)
                        join_kwargs['field_mapping'].addFieldMap(field_map)
                    arcpy.analysis.SpatialJoin(**join_kwargs)
                for _id, attr in attributes.as_iters(
                        join_kwargs['out_feature_class'],
                        field_names=(self.field['id'], over_field),
                    ):
                    if _id in self._id_attr[tag]:
                        self._id_attr[tag][_id][over_path, over_field] = attr
                arcpy.management.Delete(join_kwargs['out_feature_class'])

    def displacement_links_create(self, dataset_path, **kwargs):
        """Create new displacement links dataset.

        Args:
            dataset_path (str): Path to create dataset at.

        Keyword Args:
            spatial_reference_item: Item from which the output geometry's
                spatial reference will be derived. Default is init dataset's
                spatial reference.
            log_level (str): Level to log the function at. Default is 'info'.

        Returns:
            str: Path of the dataset created.

        """
        log_level = helpers.log_level(kwargs.get('log_level', 'info'))
        LOG.log(log_level, "Start: Create displacement links dataset %s.",
                dataset_path)
        id_field_meta = arcobj.field_metadata(dataset_path, self.field['id'])
        for key in id_field_meta:
            if key not in ('name', 'type', 'length', 'precision', 'scale'):
                del id_field_meta[key]
        dataset.create(
            dataset_path, field_metadata_list=(id_field_meta,),
            geometry_type='polyline',
            spatial_reference_item=kwargs.get('spatial_reference_item',
                                              self.spatial['reference']),
            log_level=None,
            )
        LOG.log(log_level, "End: Create.")
        return dataset_path

    def info_diff(self, feature_id, diff_type, init_value=None,
                  new_value=None, **kwargs):
        """Create info-dictionary for given feature's diff of a type.

        Keyword arguments are generally related to customizing description
            values.

        Args:
            feature_id: ID for feature.
            diff_type (str): Type of diff to create row for. Can be
                'removed', 'added', 'moved', 'attribute', 'overlay'.
            init_value: Value of the attribute/overlay on init dataset.
                Default is None.
            new_value: Value of the attribute/overlay on new dataset. Default
                is None.
            **kwargs: Arbitrary keyword arguments. See below.

        Keyword Args:
            dataset_path (str): Path of a relevant dataset.
            field_name (str): Name of a relevant field.
            field_name (str): Name of field diff occurs in.
            tolerance (str): String representation of chosen tolerance.

        Returns:
            dict: Diff information.

        """
        return {
            self.field['id']: feature_id,
            'diff_type': diff_type,
            'description': self._diff_type_description[diff_type].format(
                **kwargs
                ),
            'init_str': str(init_value) if init_value is not None else None,
            'new_str': str(new_value) if new_value is not None else None,
            }

    def info_displacement(self, feature_id, init_coord, new_coord):
        """Create info-dictionary for given feature's displacement.

        Args:
            feature_id: ID for feature.
            init_coord: Coordinate for the init dataset.
            init_coord: Coordinate for the new dataset.

        Returns:
            dict: Displacement information.

        """
        return {
            self.field['id']: feature_id,
            'shape@': arcpy.Polyline(
                arcpy.Array(arcpy.Point(init_coord), arcpy.Point(new_coord)),
                arcobj.spatial_reference(self.spatial['reference'])
                ),
            }

    def table_create(self, dataset_path, **kwargs):
        """Create new diff table.

        Args:
            dataset_path (str): Path to create table at.

        Keyword Args:
            spatial_reference_item: Item from which the output geometry's
                spatial reference will be derived. Default is init dataset's
                spatial reference.
            log_level (str): Level to log the function at. Default is 'info'.

        Returns:
            str: Path of the dataset created.

        """
        log_level = helpers.log_level(kwargs.get('log_level', 'info'))
        LOG.log(log_level, "Start: Create diff table %s.", dataset_path)
        id_field_meta = arcobj.field_metadata(dataset_path, self.field['id'])
        for key in id_field_meta:
            if key not in ('name', 'type', 'length', 'precision', 'scale'):
                del id_field_meta[key]
        field_metadata_list = (id_field_meta,) + self._diff_fields_metadata
        dataset.create(
            dataset_path, field_metadata_list,
            spatial_reference_item=kwargs.get('spatial_reference_item',
                                              self.spatial['reference']),
            log_level=None,
            )
        LOG.log(log_level, "End: Create.")
        return dataset_path
