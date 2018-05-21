"""Diff operations."""
from collections import defaultdict
from itertools import chain
import logging
import sys

import arcpy

from arcetl import arcobj
from arcetl import attributes
from arcetl import dataset
from arcetl import features
from arcetl import geometry
from arcetl.helpers import leveled_logger, unique_path

if sys.version_info.major >= 3:
    basestring = str
    """Defining a basestring type instance for Py3+."""


LOG = logging.getLogger(__name__)
"""logging.Logger: Toolbox-level logger."""


class Differ(object):
    """Object for tracking feature differences between dataset versions.

    Currently, the geometry diff only works for point geometry. Other geometry types
    will compare the centroid's location.

    Attributes:
        field_name (dict): Mapping of field tag to name(s).
        path (dict): Mapping of dataset tag to path(s).
        where_sql (dict): Mapping of dataset tag to SQL where clause.
        spatial (dict): Mapping of spatial properties to values.
        ids (dict): Mapping of IDs type to set of IDs that are of that type.
        diffs (dict): Mapping of difference type to list of information about change.

    """

    data_tags = ['init', 'new']
    """list of str: Tags for data types."""
    _diff_fields_metadata = [
        # {'name': {id_field_name},  # Added on init.
        {'name': 'diff_type', 'type': 'text', 'length': 9},
        {'name': 'description', 'type': 'text', 'length': 64},
        {'name': 'init_str', 'type': 'text', 'length': 128},
        {'name': 'new_str', 'type': 'text', 'length': 128},
    ]
    """list of dicts: Collection of diff table's field metadata."""
    diff_field_names = [field['name'] for field in _diff_fields_metadata]
    """list of str: Ordered collection of diff table's field names."""
    _diff_type_description = {
        'added': "Feature added between init & new dataset.",
        'removed': "Feature removed between init &  new dataset.",
        'geometry': "Feature moved further than {tolerance}.",
        'attribute': "Value in `{field_name}` field changed.",
        'overlay': "Value from `{dataset_path}.{field_name}` overlay changed.",
    }
    """dict: Description string based on diff type."""
    diff_types = [_type for _type in _diff_type_description]
    """list of str: Collection of diff types."""
    id_types = ['added', 'persisted', 'removed']
    """list of str: Collection of ID types."""

    def __init__(
        self,
        init_dataset_path,
        new_dataset_path,
        identifier_field_name,
        cmp_field_names=None,
        **kwargs
    ):
        """Initialize instance.

        Args:
            init_dataset_path (str): Path of the initial dataset.
            new_dataset_path (str): Path of the new dataset.
            identifier_field_name (str): Name of the field used to identify a feature.
            cmp_field_names (iter): Collection of fields to compate attributes between
                datasets for differences. Default is None.
            **kwargs: Arbitrary keyword arguments. See below.

        Keyword Args:
            tolerance (float, str): Tolerance for spatial difference. If numeric, units
                will be in init data's units. Default is 0.
            overlay_path_field_map (dict): Mapping of overlay paths to the attribute
                field name to overlay. Default is None.
            init_dataset_where_sql (str): SQL where-clause for inital dataset
                subselection. Default is None.
            new_dataset_where_sql (str): SQL where-clause for new dataset subselection.
                Default is None.

        """
        self.field_name = {
            'id': identifier_field_name,
            'cmps': set(cmp_field_names) if cmp_field_names else set(),
        }
        self.field_name['loads'] = (
            {self.field_name['id'], 'shape@xy'} | self.field_name['cmps']
        )
        self.path = {'init': init_dataset_path, 'new': new_dataset_path}
        self.where_sql = {
            tag: kwargs.get(tag + '_dataset_where_sql') for tag in self.data_tags
        }
        self.field_name['overlay'], self.path['overlay'] = [], []
        if 'overlay_path_field_map' in kwargs:
            for path, field_name in kwargs['overlay_path_field_map'].items():
                self.path['overlay'].append(path)
                self.field_name['overlay'].append(field_name)
        self.spatial = {'reference': arcobj.spatial_reference(init_dataset_path)}
        if kwargs.get('tolerance'):
            if isinstance(kwargs['tolerance'], basestring):
                self.spatial['tolerance'] = arcobj.linear_unit(
                    kwargs['tolerance'], self.spatial['reference']
                )
            else:
                self.spatial['tolerance'] = kwargs['tolerance']
        else:
            self.spatial['tolerance'] = 0
        self.spatial['tolerance_str'] = arcobj.linear_unit_string(
            self.spatial['tolerance'], self.spatial['reference']
        )
        self._id_attr = defaultdict(dict)
        """defaultdict: Internal mapping of feature IDs to attribute information."""
        self.ids = {key: None for key in self.id_types}
        self.diffs = {key: None for key in self.diff_types}
        self.displacement_links = None
        # Add the dataset-specific ID field metadata to the diff fields metadata.
        _meta = arcobj.field_metadata(init_dataset_path, self.field_name['id'])
        _meta = {
            key: val
            for key, val in _meta.items()
            if key in ['name', 'type', 'length', 'precision', 'scale']
        }
        self._diff_fields_metadata.insert(0, _meta)
        self.diff_field_names.insert(0, _meta['name'])

    def __enter__(self):
        return self.load().eval()

    def __exit__(self, exception_type, exception_value, traceback):
        pass

    def create_displacement_links(self, dataset_path, **kwargs):
        """Create new displacement links dataset.

        Args:
            dataset_path (str): Path to create dataset at.

        Keyword Args:
            spatial_reference_item: Item from which the output geometry's spatial
            reference will be derived. Default is init dataset's spatial reference.
            log_level (str): Level to log the function at. Default is 'info'.

        Returns:
            str: Path of the dataset created.

        """
        kwargs.setdefault('spatial_reference_item', self.spatial['reference'])
        log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
        log("Start: Create displacement links dataset %s.", dataset_path)
        dataset.create(
            dataset_path,
            field_metadata_list=self._diff_fields_metadata[0],
            geometry_type='polyline',
            spatial_reference_item=kwargs['spatial_reference_item'],
            log_level=None,
        )
        features.insert_from_dicts(
            dataset_path,
            insert_features=self.displacement_links,
            field_names=self.diff_field_names[0],
            log_level=None,
        )
        log("End: Create.")
        return dataset_path

    def create_table(self, dataset_path, **kwargs):
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
        kwargs.setdefault('spatial_reference_item', self.spatial['reference'])
        log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
        log("Start: Create diff table %s.", dataset_path)
        dataset.create(
            dataset_path,
            self._diff_fields_metadata,
            spatial_reference_item=kwargs['spatial_reference_item'],
            log_level=None,
        )
        features.insert_from_dicts(
            dataset_path,
            insert_features=chain(*self.diffs.values()),
            field_names=self.diff_field_names,
            log_level=None,
        )
        log("End: Create.")
        return dataset_path

    def diff_info(
        self, feature_id, diff_type, init_value=None, new_value=None, **kwargs
    ):
        """Create info-dictionary for given feature's diff of a type.

        Keyword arguments are generally related to customizing description
            values.

        Args:
            feature_id: ID for feature.
            diff_type (str): Type of diff to create row for (see diff_types property).
            init_value: Value of the attribute/overlay on init dataset. Default is
                None.
            new_value: Value of the attribute/overlay on new dataset. Default is None.
            **kwargs: Arbitrary keyword arguments. See below.

        Keyword Args:
            dataset_path (str): Path of a relevant dataset.
            field_name (str): Name of a relevant field.
            field_name (str): Name of field diff occurs in.
            tolerance (str): String representation of chosen tolerance.

        Returns:
            dict: Diff information.

        """
        diff = {
            self.field_name['id']: feature_id,
            'diff_type': diff_type,
            'description': self._diff_type_description[diff_type].format(**kwargs),
            'init_str': str(init_value) if init_value is not None else None,
            'new_str': str(new_value) if new_value is not None else None
        }
        return diff

    def displacement_link(self, feature_id, init_coord, new_coord):
        """Create link feature representing given feature's displacement.

        Args:
            feature_id: ID for feature.
            init_coord: Coordinate for the init dataset.
            init_coord: Coordinate for the new dataset.

        Returns:
            dict: Displacement link.

        """
        link = {
            self.field_name['id']: feature_id,
            'shape@': arcpy.Polyline(
                arcpy.Array(arcpy.Point(init_coord), arcpy.Point(new_coord)),
                self.spatial['reference'],
            ),
        }
        return link

    def eval(self):
        """Evaluate differences between datasets.

        Populates properties: ids & diffs.

        Returns:
            arcetl.diff.Differ: Reference to the instance.

        """
        self.ids['added'] = set(self._id_attr['new']) - set(self._id_attr['init'])
        self.ids['persisted'] = set(self._id_attr['new']) & set(self._id_attr['init'])
        self.ids['removed'] = set(self._id_attr['init']) - set(self._id_attr['new'])
        # Init diffs.
        for _type in self.diff_types:
            if _type in ['added', 'removed']:
                self.diffs[_type] = [
                    self.diff_info(_id, _type) for _id in self.ids[_type]
                ]
            else:
                self.diffs[_type] = []
        self.displacement_links = []
        for _id in self.ids['persisted']:
            # Check for attribute diffs.
            for field_name in self.field_name['cmps']:
                init_val = self._id_attr['init'][_id][field_name]
                new_val = self._id_attr['new'][_id][field_name]
                if init_val != new_val:
                    diff = self.diff_info(
                        _id, 'attribute', init_val, new_val, field_name=field_name
                    )
                    self.diffs['attribute'].append(diff)
            # Check for geometry diff.
            init_val = self._id_attr['init'][_id]['shape@xy']
            new_val = self._id_attr['new'][_id]['shape@xy']
            coord_dist = geometry.coordinate_distance(init_val, new_val)
            if coord_dist > self.spatial['tolerance']:
                diff = self.diff_info(
                    _id,
                    'geometry',
                    init_val,
                    new_val,
                    tolerance=self.spatial['tolerance_str'],
                )
                self.diffs['geometry'].append(diff)
                link = self.displacement_link(_id, init_val, new_val)
                self.displacement_links.append(link)
            # Check for overlay diffs.
            for over_field_name, over_path in zip(
                self.field_name['overlay'], self.path['overlay']
            ):
                init_val = self._id_attr['init'][_id][over_path, over_field_name]
                new_val = self._id_attr['new'][_id][over_path, over_field_name]
                if init_val != new_val:
                    diff = self.diff_info(
                        _id,
                        'overlay',
                        init_val,
                        new_val,
                        dataset_path=over_path,
                        field_name=over_field_name,
                    )
                self.diffs['overlay'].append(diff)
        return self

    def load(self):
        """Load diff review data.

        Returns:
            arcetl.diff.Differ: Reference to the instance.

        """
        # Clear old attributes.
        self._id_attr.clear()
        for tag in self.data_tags:
            g_attrs = attributes.as_dicts(
                dataset_path=self.path[tag],
                field_names=self.field_name['loads'],
                dataset_where_sql=self.where_sql[tag],
                spatial_reference_item=self.spatial['reference'],
            )
            for attr in g_attrs:
                _id = attr[self.field_name['id']]
                self._id_attr[tag][_id] = attr
        # Add overlay attributes.
        for over_field_name, over_path in zip(
            self.field_name['overlay'], self.path['overlay']
        ):
            for tag in self.data_tags:
                view = arcobj.DatasetView(self.path[tag], self.where_sql[tag])
                with view:
                    join_kwargs = {
                        'target_features': view.name,
                        'join_features': over_path,
                        'out_feature_class': unique_path(),
                        'field_mapping': arcpy.FieldMappings(),
                    }
                    for path, field_name in [
                        (view.name, self.field_name['id']), (over_path, over_field_name)
                    ]:
                        field_map = arcpy.FieldMap()
                        field_map.addInputField(path, field_name)
                        join_kwargs['field_mapping'].addFieldMap(field_map)
                    arcpy.analysis.SpatialJoin(**join_kwargs)
                for _id, attr in attributes.as_iters(
                    join_kwargs['out_feature_class'],
                    field_names=[self.field_name['id'], over_field_name],
                ):
                    if _id in self._id_attr[tag]:
                        # Use path/field name for attribute key.
                        self._id_attr[tag][_id][over_path, over_field_name] = attr
                arcpy.management.Delete(join_kwargs['out_feature_class'])
        return self
