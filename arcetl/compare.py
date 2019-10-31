"""Diff operations."""
from copy import copy
from functools import partial
import logging

from arcetl.arcobj import dataset_metadata, field_metadata
from arcetl import attributes
from arcetl import dataset
from arcetl.geometry import convex_hull, line_between_centroids
from arcetl.helpers import contain, same_feature, same_value


LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""

DATASET_TAGS = ["init", "new"]
"""list of str: Tags for dataset types."""
DIFF_TYPE_DESCRIPTION = {
    "added": "Feature added between init & new dataset.",
    "removed": "Feature removed between init & new dataset.",
    "geometry": "Feature geometry changed.",
    "attribute": "Value in `{field_name}` field changed.",
}
"""dict: Mapping of diff type to formatted description."""
DIFF_TYPES = sorted(DIFF_TYPE_DESCRIPTION.keys())
"""list of str: Tags for attibute diff types."""
OUTPUT_METADATA = {
    "differences": {
        "fields": [
            # Metadata for ID fields will be added in-function.
            {"name": "diff_type", "type": "text", "length": 9},
            {"name": "description", "type": "text", "length": 64},
            {"name": "init_repr", "type": "text", "length": 255},
            {"name": "new_repr", "type": "text", "length": 255},
        ],
        "geometry_type": "polygon",
    },
    "displacements": {
        "fields": [
            # Metadata for ID fields will be added in-function.
            {"name": "init_length", "type": "double"},
            {"name": "new_length", "type": "double"},
            {"name": "init_area", "type": "double"},
            {"name": "new_area", "type": "double"},
        ],
        "geometry_type": "polyline",
    },
}
"""dict: Mapping of output tag to metadata for output."""
OUTPUT_TAGS = sorted(OUTPUT_METADATA.keys())
"""list: Output type tags."""


def _difference(id_map, diff_type, init_value=None, new_value=None, **kwargs):
    """Return difference feature.

    Keyword arguments are generally related to customizing description values.

    Args:
        id_map (dict): Mapping of feature ID keys to values.
        diff_type (str): Type of diff to create row for (see DIFF_TYPES).
        init_value (object): Attribute value on init feature.
        new_value (object): Attribute value on new feature.
        represent_geometry (bool): Geometry should be represented by convex hull
            covering init & new geometry if True, no geometry representation if False.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        field_name (str): Name of attribute field.
        represent_geometry (bool): Geometry should be represented by convex hull
            covering init & new geometry if True, no geometry representation if False.
            Default is False.
        init_geometry (arcpy.Geometry): Geometry on init feature.
        new_geometry (arcpy.Geometry): Geometry on new feature.

    Returns:
        dict.
    """
    if diff_type not in DIFF_TYPES:
        raise AttributeError(
            "diff_type {} not valid. Valid types: {}".format(
                diff_type, ", ".join(DIFF_TYPES)
            )
        )
    diff = copy(id_map)
    diff["diff_type"] = diff_type
    diff["description"] = DIFF_TYPE_DESCRIPTION[diff_type].format(**kwargs)
    diff["init_repr"] = repr(init_value) if diff_type == "attribute" else None
    diff["new_repr"] = repr(new_value) if diff_type == "attribute" else None
    if kwargs.get("represent_geometry", False):
        diff["shape@"] = convex_hull(
            kwargs.get("init_geometry"), kwargs.get("new_geometry")
        )
    return diff


def _displacement(id_map, init_geometry, new_geometry):
    """Return features of displacement between given features.

    Args:
        id_map (dict): Mapping of feature ID keys to values.
        init_geometry (arcpy.Geometry): Geometry for init feature.
        new_geometry (arcpy.Geometry): Geometry for new feature.

    Returns:
        dict
    """
    diff = copy(id_map)
    for tag, geom in {"init": init_geometry, "new": new_geometry}.items():
        for attr in ["length", "area"]:
            diff[tag + "_" + attr] = getattr(geom, attr) if geom else None
    if init_geometry and new_geometry:
        diff["shape@"] = line_between_centroids(init_geometry, new_geometry)
    else:
        diff["shape@"] = None
    return diff


def differences(
    init_dataset_path, new_dataset_path, id_field_names, cmp_field_names=None, **kwargs
):
    """Generate tuples of differences between given dataset features.

    Args:
        init_dataset_path (str): Path of initial dataset.
        new_dataset_path (str): Path of new dataset.
        id_field_names (iter): Ordered collection of fields used to identify a feature.
        cmp_field_names (iter): Ordered collection of fields to compare attributes
            between datasets for differences.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        use_external_sort (bool): Use external sort. Helpful for memory management with
             large datasets. Default is False.
        init_dataset_where_sql (str): SQL where-clause for inital dataset
            subselection. Default is None.
        new_dataset_where_sql (str): SQL where-clause for new dataset subselection.
            Default is None.
        spatial_reference_item: Item from which the spatial reference of the output
            geometry will be derived (if needed). Default is same as the init dataset.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).
        log_evaluated_count (int): Divisor at which the function will log how many
            features it has evaluated. Default is 10,000.

    Yields:
        tuple
    """
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level,
        "Start: Evaluate differences from `%s` to `%s`.",
        init_dataset_path,
        new_dataset_path,
    )
    kwargs.setdefault("log_evaluated_count", 10000)
    keys = {
        "id": list(contain(id_field_names)),
        "cmp": list(contain(cmp_field_names)) if cmp_field_names else [],
    }
    meta = {
        "init": dataset_metadata(init_dataset_path),
        "new": dataset_metadata(new_dataset_path),
    }
    for tag in DATASET_TAGS:
        meta[tag]["where_sql"] = kwargs.get(tag + "_dataset_where_sql")
    # Need to add geometry to cmp-keys if extant in both.
    if all(meta[tag]["is_spatial"] for tag in DATASET_TAGS):
        keys["cmp"].append("shape@")
    id_vals = {
        tag: attributes.id_values(
            dataset_path=meta[tag]["path"],
            id_field_names=keys["id"],
            field_names=keys["cmp"],
            sort_by_id=True,
            use_external_sort=kwargs.get("use_external_sort", False),
            dataset_where_sql=meta[tag]["where_sql"],
            spatial_reference_item=kwargs.get(
                "spatial_reference_item", meta["init"]["spatial_reference"]
            ),
        )
        for tag in DATASET_TAGS
    }
    diff = partial(_difference, represent_geometry=("shape@" in keys["cmp"]))
    init = {}
    new = {}
    for i, (init["id"], init["vals"]) in enumerate(id_vals["init"], 1):
        for key in ["id", "vals"]:
            init[key] = tuple(contain(init[key]))
        init["geom"] = init["vals"][-1] if "shape@" in keys["cmp"] else None
        while not new or new["id"] < init["id"]:
            try:
                (new["id"], new["vals"]) = next(id_vals["new"])
            except StopIteration:
                # At end of feats["new"]: init feature has been removed.
                yield diff(
                    id_map=dict(zip(keys["id"], init["id"])),
                    diff_type="removed",
                    init_geometry=init["geom"],
                )
                continue

            for key in ["id", "vals"]:
                new[key] = tuple(contain(new[key]))
            new["geom"] = new["vals"][-1] if "shape@" in keys["cmp"] else None
            if same_feature(init["id"], new["id"]):
                # IDs match: check if attributes match.
                for key, init_val, new_val in zip(
                    keys["cmp"], init["vals"], new["vals"]
                ):
                    if not same_value(init_val, new_val):
                        # Features have different attribute value.
                        yield diff(
                            id_map=dict(zip(keys["id"], init["id"])),
                            diff_type=("geometry" if key == "shape@" else "attribute"),
                            init_value=init_val,
                            new_value=new_val,
                            field_name=key,
                            init_geometry=init["geom"],
                            new_geometry=new["geom"],
                        )

            elif init["id"] < new["id"]:
                # Init feature has been removed.
                yield diff(
                    id_map=dict(zip(keys["id"], init["id"])),
                    diff_type="removed",
                    init_geometry=init["geom"],
                )

            elif new["id"] < init["id"]:
                # New feature has been added.
                yield diff(
                    id_map=dict(zip(keys["id"], new["id"])),
                    diff_type="added",
                    new_geometry=new["geom"],
                )

        if i % kwargs["log_evaluated_count"] == 0:
            LOG.log(level, "%s features evaluated.", i)
    # No more init features: remaining new features have been added.
    for i, (new["id"], new["vals"]) in enumerate(id_vals["new"], i + 1):
        for key in ["id", "vals"]:
            new[key] = tuple(contain(new[key]))
        new["geom"] = new["vals"][-1] if "shape@" in keys["cmp"] else None
        yield diff(
            id_map=dict(zip(keys["id"], new["id"])),
            diff_type="added",
            new_geometry=new["geom"],
        )

        if i % kwargs["log_evaluated_count"] == 0:
            LOG.log(level, "%s features evaluated.", i)
    LOG.log(level, "%s features evaluated.", i)
    LOG.log(level, "End: Evaluate.")


def displacements(init_dataset_path, new_dataset_path, id_field_names, **kwargs):
    """Generate tuples of displacement line features between given dataset features.

    Args:
        init_dataset_path (str): Path of initial dataset.
        new_dataset_path (str): Path of new dataset.
        id_field_names (iter): Field names used to identify a feature.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        use_external_sort (bool): Use external sort. Helpful for memory management with
             large datasets. Default is False.
        init_dataset_where_sql (str): SQL where-clause for inital dataset
            subselection. Default is None.
        new_dataset_where_sql (str): SQL where-clause for new dataset subselection.
            Default is None.
        spatial_reference_item: Item from which the spatial reference of the output
            geometry will be derived (if needed). Default is same as the init dataset.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).
        log_evaluated_count (int): Divisor at which the function will log how many
            features it has evaluated. Default is 10,000.

    Yields:
        tuple
    """
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level,
        "Start: Evaluate displacements from `%s` to `%s`.",
        init_dataset_path,
        new_dataset_path,
    )
    kwargs.setdefault("log_evaluated_count", 10000)
    keys = {"id": list(contain(id_field_names)), "cmp": ["shape@"]}
    meta = {
        "init": dataset_metadata(init_dataset_path),
        "new": dataset_metadata(new_dataset_path),
    }
    for tag in DATASET_TAGS:
        meta[tag]["where_sql"] = kwargs.get(tag + "_dataset_where_sql")
    id_vals = {
        tag: attributes.id_values(
            dataset_path=meta[tag]["path"],
            id_field_names=keys["id"],
            field_names=keys["cmp"],
            sort_by_id=True,
            use_external_sort=kwargs.get("use_external_sort", False),
            dataset_where_sql=meta[tag]["where_sql"],
            spatial_reference_item=kwargs.get(
                "spatial_reference_item", meta["init"]["spatial_reference"]
            ),
        )
        for tag in DATASET_TAGS
    }
    init = {}
    new = {}
    for i, (init["id"], init["geom"]) in enumerate(id_vals["init"], 1):
        init["id"] = tuple(contain(init["id"]))
        while not new or new["id"] < init["id"]:
            try:
                (new["id"], new["geom"]) = next(id_vals["new"])
            except StopIteration:
                # At end of feats["new"]: init feature has been removed.
                continue

            new["id"] = tuple(contain(new["id"]))
            if same_feature(init["id"], new["id"]):
                # IDs match: check if attributes match.
                if not same_value(init["geom"], new["geom"]):
                    # Features have different attribute value.
                    yield _displacement(
                        id_map=dict(zip(keys["id"], init["id"])),
                        init_geometry=init["geom"],
                        new_geometry=new["geom"],
                    )

        if i % kwargs["log_evaluated_count"] == 0:
            LOG.log(level, "%s features evaluated.", i)
    LOG.log(
        level, "%s features evaluated.", i
    )  # pylint: disable=undefined-loop-variable
    LOG.log(level, "End: Evaluate.")


def create_output_dataset(
    dataset_path, output_type, cmp_dataset_path, id_field_names, **kwargs
):
    """Create output comparison dataset.

    Args:
        dataset_path (str): Path where to initialize dataset.
        output_type (str): Tag indicating type of output comparison dataset (see
            OUTPUT_TAGS).
        cmp_dataset_path (str): Path of a comprison dataset. Used for ID field metadata
            & spatial reference.
        id_field_names (iter): Ordered collection of fields used to identify a feature.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        spatial_reference_item: Item from which the spatial reference of the output
            geometry will be derived (if needed). Default is same as comparison dataset.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns
        str: Path of dataset.
    """
    kwargs.setdefault("spatial_reference_item", cmp_dataset_path)
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(level, "Start: Initialize comparison dataset at `%s`.", dataset_path)
    field_metadata_list = [
        {
            key: val
            for key, val in field_metadata(cmp_dataset_path, field_name).items()
            # Only transferring basic field properties to output.
            if key in ["name", "type", "length", "precision", "scale"]
        }
        for field_name in contain(id_field_names)
    ] + OUTPUT_METADATA[output_type]["fields"]
    dataset.create(
        dataset_path,
        field_metadata_list,
        geometry_type=(
            OUTPUT_METADATA[output_type]["geometry_type"]
            if dataset_metadata(cmp_dataset_path)["is_spatial"]
            else None
        ),
        spatial_reference_item=kwargs["spatial_reference_item"],
        log_level=logging.DEBUG,
    )
    LOG.log(level, "End: Initialize.")
    return dataset_path
