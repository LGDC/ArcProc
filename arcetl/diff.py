"""Diff operations."""
from collections import Counter, defaultdict
from itertools import chain
import logging

import arcpy

from arcetl.arcobj import DatasetView, field_metadata, same_value, spatial_reference
from arcetl import attributes
from arcetl import dataset
from arcetl import features
from arcetl.helpers import freeze_values, unique_path

LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""


##TODO: New differ that minimizes memory load. Maybe generator method?
##TODO: Table/nonspatial diff support (also means nonspatial diff output).
class Differ(object):
    """Object for tracking feature differences between dataset versions.

    Attributes:
        ids (dict):  Mapping of feature diff type to feature IDs of that type.
        diffs (dict): Mapping of difference type to list of feature information about
            any changes.
    """

    _dataset_tags = ["init", "new"]
    """list of str: Tags for dataset types."""
    _diff_type_description = {
        "added": "Feature added between init & new dataset.",
        "removed": "Feature removed between init &  new dataset.",
        "geometry": "Feature geometry changed.",
        "attribute": "Value in `{field_name}` field changed.",
        "overlay": "Value from `{dataset_path}.{field_name}` overlay changed.",
    }
    """dict: Description string based on diff type."""
    _feature_diff_types = ["added", "persisted", "removed"]
    """list of str: Tags for feature diff types."""
    diff_types = [key for key in _diff_type_description]
    """list of str: Tags for attibute diff types."""

    def __init__(
        self,
        init_dataset_path,
        new_dataset_path,
        id_field_names,
        cmp_field_names=None,
        **kwargs
    ):
        """Initialize instance.

        Args:
            init_dataset_path (str): Path of initial dataset.
            new_dataset_path (str): Path of new dataset.
            id_field_names (iter): Field names used to identify a feature.
            cmp_field_names (iter): Collection of fields to compate attributes between
                datasets for differences.
            **kwargs: Arbitrary keyword arguments. See below.

        Keyword Args:
            overlay_path_fields_map (dict): Mapping of overlay path to attribute field
                names to overlay. Default is None.
            init_dataset_where_sql (str): SQL where-clause for inital dataset
                subselection. Default is None.
            new_dataset_where_sql (str): SQL where-clause for new dataset subselection.
                Default is None.
        """
        self._keys = {
            "id": list(id_field_names),
            "cmp": list(cmp_field_names) if cmp_field_names else [],
        }
        """dict: Mapping of field tag to names."""
        self._keys["load"] = self._keys["id"] + ["shape@"] + self._keys["cmp"]
        self._dataset = {
            "init": {"path": init_dataset_path},
            "new": {"path": new_dataset_path},
            "overlays": [],
        }
        """dict: Mapping of dataset tag to info about dataset."""
        for tag in self._dataset_tags:
            self._dataset[tag]["where_sql"] = kwargs.get(tag + "_dataset_where_sql")
            self._dataset[tag]["spatial_reference"] = spatial_reference(
                self._dataset[tag]["path"]
            )
        for path, field_names in kwargs.get("overlay_path_fields_map", {}).items():
            self._dataset["overlays"].append({"path": path, "keys": list(field_names)})
        # Collect field metadata for diff table.
        self._diff_field_metas = [
            {"name": "diff_type", "type": "text", "length": 9},
            {"name": "description", "type": "text", "length": 64},
            {"name": "init_repr", "type": "text", "length": 255},
            {"name": "new_repr", "type": "text", "length": 255},
        ]
        """list of dicts: Diff table field metadata."""
        for id_key in self._keys["id"]:
            meta = {
                key: val
                for key, val in field_metadata(
                    self._dataset["init"]["path"], id_key
                ).items()
                if key in {"name", "type", "length", "precision", "scale"}
            }
            self._diff_field_metas.append(meta)
        self._keys["diff"] = [field["name"] for field in self._diff_field_metas]
        # Init containers.
        self._id_attr = defaultdict(dict)
        """defaultdict: Mapping of feature ID to information of attributes."""
        self.ids = {key: None for key in self._feature_diff_types}
        self.diffs = {key: None for key in self.diff_types}
        self._displacement_links = []
        """list: Representations of displacement links for the geometry diffs."""

    def __enter__(self):
        return self.extract().eval()

    def __exit__(self, exception_type, exception_value, traceback):
        pass

    def diff_info(self, feature_id, diff_tag, values=None, geometries=None, **kwargs):
        """Create info-dictionary for diff of given feature.

        Keyword arguments are generally related to customizing description values.

        Args:
            feature_id: ID values for feature.
            diff_tag (str): Type of diff to create row for (see diff_types property).
            values (list): Init & new values for attribute/overlay.
            geometries (list of arcpy.Geometry): Init & new feature geometries.
            **kwargs: Arbitrary keyword arguments. See below.

        Keyword Args:
            dataset_path (str): Path of relevant dataset.
            field_name (str): Name of relevant field.

        Returns:
            dict: Diff information.
        """
        diff = {
            "diff_type": diff_tag,
            "description": self._diff_type_description[diff_tag].format(**kwargs),
            "init_repr": str(values[0]) if values is not None else None,
            "new_repr": str(values[1]) if values is not None else None,
        }
        if geometries:
            diff["shape@"] = (
                geometries[0]
                if same_value(*geometries)
                else geometries[0].union(geometries[1]).convexHull()
            )
        else:
            diff["shape@"] = None
        for i, id_key in enumerate(self._keys["id"]):
            diff[id_key] = feature_id[i]
        return diff

    def displacement_link(self, feature_id, geometries):
        """Create link feature representing displacement of given feature.

        Args:
            feature_id: ID values for feature.
            geometries (list of arcpy.Geometry): Init & new feature geometries.

        Returns:
            dict: Displacement link.
        """
        points = [geom.centroid for geom in geometries]
        # If centroids same, create "kick-out" midpoint to define valid line.
        if same_value(*points):
            mid = arcpy.Point(X=(points[0].X + 1), Y=(points[0].Y + 1), Z=points[0].Z)
            points.insert(1, mid)
        link = {
            "shape@": arcpy.Polyline(
                arcpy.Array(*points), self._dataset["init"]["spatial_reference"]
            )
        }
        for i, id_key in enumerate(self._keys["id"]):
            link[id_key] = feature_id[i]
        return link

    def eval(self):
        """Evaluate differences between datasets.

        Populates properties: ids & diffs.

        Returns:
            arcetl.diff.Differ: Reference to the instance.
        """
        self.ids["added"] = set(self._id_attr["new"]) - set(self._id_attr["init"])
        self.ids["persisted"] = set(self._id_attr["new"]) & set(self._id_attr["init"])
        self.ids["removed"] = set(self._id_attr["init"]) - set(self._id_attr["new"])
        # Init containers.
        for tag in self.diff_types:
            if tag in ["added", "removed"]:
                self.diffs[tag] = [
                    self.diff_info(id_val, tag) for id_val in self.ids[tag]
                ]
            else:
                self.diffs[tag] = []
        self._displacement_links = []
        for id_val in self.ids["persisted"]:
            # Check for geometry diff.
            geoms = [
                self._id_attr["init"][id_val]["shape@"],
                self._id_attr["new"][id_val]["shape@"],
            ]
            if not same_value(*geoms):
                diff = self.diff_info(id_val, "geometry", geometries=geoms)
                self.diffs["geometry"].append(diff)
                link = self.displacement_link(id_val, geometries=geoms)
                self._displacement_links.append(link)
            # Check for attribute diffs.
            for key in self._keys["cmp"]:
                vals = [
                    self._id_attr["init"][id_val][key],
                    self._id_attr["new"][id_val][key],
                ]
                if not same_value(*vals):
                    diff = self.diff_info(
                        id_val, "attribute", values=vals, field_name=key
                    )
                    self.diffs["attribute"].append(diff)
            # Check for overlay diffs.
            for overlay in self._dataset["overlays"]:
                for key in overlay["keys"]:
                    vals = [
                        self._id_attr["init"][id_val][(overlay["path"], key)],
                        self._id_attr["new"][id_val][(overlay["path"], key)],
                    ]
                    if not same_value(*vals):
                        diff = self.diff_info(
                            id_val,
                            "overlay",
                            values=vals,
                            dataset_path=overlay["path"],
                            field_name=key,
                        )
                        self.diffs["overlay"].append(diff)
        return self

    def extract(self):
        """Extract review features.

        Returns:
            arcetl.diff.Differ: Reference to instance.
        """
        # Clear old attributes.
        self._id_attr.clear()
        for tag in self._dataset_tags:
            feats = attributes.as_dicts(
                dataset_path=self._dataset[tag]["path"],
                field_names=self._keys["load"],
                dataset_where_sql=self._dataset[tag]["where_sql"],
                spatial_reference_item=self._dataset["init"]["spatial_reference"],
            )
            for feat in feats:
                id_val = tuple(freeze_values(*(feat[key] for key in self._keys["id"])))
                self._id_attr[tag][id_val] = feat
        # Add overlay attributes.
        for tag in self._dataset_tags:
            view = DatasetView(
                dataset_path=self._dataset[tag]["path"],
                dataset_where_sql=self._dataset[tag]["where_sql"],
                field_names=self._keys["id"],
            )
            with view:
                for overlay in self._dataset["overlays"]:
                    field_maps = arcpy.FieldMappings()
                    for path, keys in [
                        (view.name, self._keys["id"]),
                        (overlay["path"], overlay["keys"]),
                    ]:
                        for key in keys:
                            field_map = arcpy.FieldMap()
                            field_map.addInputField(path, key)
                            field_maps.addFieldMap(field_map)
                    output_path = unique_path()
                    arcpy.analysis.SpatialJoin(
                        target_features=view.name,
                        join_features=overlay["path"],
                        out_feature_class=output_path,
                        field_mapping=field_maps,
                    )
                    for feat in attributes.as_dicts(
                        output_path, field_names=(self._keys["id"] + overlay["keys"])
                    ):
                        id_val = tuple(
                            freeze_values(*(feat[key] for key in self._keys["id"]))
                        )
                        if id_val in self._id_attr[tag]:
                            for key in overlay["keys"]:
                                # Use (path, field name) for attribute key.
                                self._id_attr[tag][id_val][
                                    (overlay["path"], key)
                                ] = feat[key]
                    arcpy.management.Delete(output_path)
        return self

    def load_diffs(self, dataset_path, preserve_features=False, **kwargs):
        """Load diff features to dataset.

        Args:
            dataset_path (str): Path of dataset to load.
            preserve_features (bool): Flag to indicate whether to remove features in
                the load-dataset before adding the transformed features.

        Keyword Args:
        use_edit_session (bool): Updates are done in an edit session if True. Default is
            False.

        Returns:
            collections.Counter: Counts for each update type.
        """
        kwargs.setdefault("use_edit_session", False)
        LOG.info("Start: Load diffs to %s.", dataset_path)
        feature_count = Counter()
        if dataset.is_valid(dataset_path):
            if not preserve_features:
                feature_count.update(
                    features.delete(dataset_path, log_level=None, **kwargs)
                )
        else:
            dataset.create(
                dataset_path,
                field_metadata_list=self._diff_field_metas,
                geometry_type="polygon",
                spatial_reference_item=self._dataset["init"]["spatial_reference"],
                log_level=None,
            )
        feature_count.update(
            features.insert_from_dicts(
                dataset_path,
                insert_features=chain(*self.diffs.values()),
                field_names=self._keys["diff"],
                log_level=None,
                **kwargs
            )
        )
        for key in ["deleted", "inserted"]:
            LOG.info("%s features %s.", feature_count[key], key)
        LOG.info("End: Load.")
        return feature_count

    def load_displacement_links(self, dataset_path, preserve_features=False, **kwargs):
        """Load displacement links to dataset.

        Args:
            dataset_path (str): Path to create dataset at.
            preserve_features (bool): Flag to indicate whether to remove features in
                the load-dataset before adding the transformed features.

        Keyword Args:
        use_edit_session (bool): Updates are done in an edit session if True. Default is
            False.

        Returns:
            collections.Counter: Counts for each update type.
        """
        kwargs.setdefault("use_edit_session", False)
        LOG.info("Start: Load displacement links to %s.", dataset_path)
        feature_count = Counter()
        if dataset.is_valid(dataset_path):
            if not preserve_features:
                feature_count.update(
                    features.delete(dataset_path, log_level=None, **kwargs)
                )
        else:
            dataset.create(
                dataset_path,
                field_metadata_list=self._diff_field_metas,
                geometry_type="polyline",
                spatial_reference_item=self._dataset["init"]["spatial_reference"],
                log_level=None,
            )
        feature_count.update(
            features.insert_from_dicts(
                dataset_path,
                insert_features=self._displacement_links,
                field_names=self._keys["id"] + ["shape@"],
                log_level=None,
                **kwargs
            )
        )
        for key in ["deleted", "inserted"]:
            LOG.info("%s features %s.", feature_count[key], key)
        LOG.info("End: Load.")
        return feature_count
