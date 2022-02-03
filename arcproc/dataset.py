"""Dataset operations."""
from collections import Counter
from functools import partial
import logging
from pathlib import Path

import arcpy

from arcproc.arcobj import (
    DatasetView,
    dataset_metadata,
)
from arcproc.helpers import contain
from arcproc.metadata import Field, SpatialReference


LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""

arcpy.SetLogHistory(False)


def add_field(dataset_path, name, **kwargs):
    """Add field to dataset.

    Args:
        dataset_path (pathlib.Path, str): Path of the dataset.
        name (str): Name of the field.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        type (str): Data type of the field. Default is "TEXT".
        precision (int): Precision of field. Only applies to float/double fields.
        scale (int): Scale of field. Only applies to float/double fields.
        length (int): Length of field. Only applies to text fields. Default is 64.
        alias (str): Alias to assign field.
        is_nullable (bool): Field can be nullable if True. Default is True.
        is_required (bool): Field value will be required for feature if True. Default is
            False.
        exist_ok (bool): If field already exists: will raise an error if False;
            will act as if field was added if True. Default is False.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        arcproc.metadata.Field: Field metadata instance.

    Raises:
        RuntimeError: If `exist_ok=False` and field already exists.
    """
    dataset_path = Path(dataset_path)
    field = {
        "name": name,
        "type": kwargs.get("type", "TEXT"),
        "precision": kwargs.get("precision", None),
        "scale": kwargs.get("scale", None),
        "length": kwargs.get("length", 64),
        "alias": kwargs.get("alias", None),
        "is_nullable": kwargs.get("is_nullable", True),
        "is_required": kwargs.get("is_required", False),
    }
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(level, "Start: Add field `%s` on `%s`.", name, dataset_path)
    if arcpy.ListFields(dataset_path, name):
        LOG.log(level, "Field already exists.")
        if not kwargs.get("exist_ok", False):
            raise RuntimeError("Cannot add existing field (exist_ok=False).")

    else:
        add_field_kwargs = {f"field_{key}": value for key, value in field.items()}
        # ArcPy2.8.0: Convert to str.
        arcpy.management.AddField(in_table=str(dataset_path), **add_field_kwargs)
    LOG.log(level, "End: Add.")
    return Field(dataset_path, name)


def add_index(dataset_path, field_names, **kwargs):
    """Add index to dataset fields.

    Note:
        Index names can only be applied to non-spatial indexes for geodatabase feature
        classes and tables.

        There is a limited length allowed for index names; longer names will be
        truncated without warning.

    Args:
        dataset_path (pathlib.Path, str): Path of the dataset.
        field_names (iter): Collection of participating field names.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        index_name (str): Name for index. Optional; see note.
        is_ascending (bool): Build index in ascending order if True. Default is False.
        is_unique (bool): Build index with unique constraint if True. Default is False.
        fail_on_lock_ok (bool): If True, indicate success even if dataset locks prevent
            adding index. Default is False.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        pathlib.Path: Path of the dataset receiving the index.

    Raises:
        RuntimeError: If more than one field and any are geometry-types.
        arcpy.ExecuteError: If dataset lock prevents adding index.
    """
    dataset_path = Path(dataset_path)
    field_names = list(contain(field_names))
    kwargs.setdefault("index_name", "ndx_" + "_".join(field_names))
    kwargs.setdefault("is_ascending", False)
    kwargs.setdefault("is_unique", False)
    kwargs.setdefault("fail_on_lock_ok", False)
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level, "Start: Add index to field(s) `%s` on `%s`.", field_names, dataset_path
    )
    field_types = {
        field["type"].upper()
        for field in dataset_metadata(dataset_path)["fields"]
        if field["name"].lower() in [name.lower() for name in field_names]
    }
    if "GEOMETRY" in field_types:
        if len(field_names) > 1:
            raise RuntimeError("Cannot create a composite spatial index.")

        # ArcPy2.8.0: Convert to str.
        func = partial(arcpy.management.AddSpatialIndex, in_features=str(dataset_path))
    else:
        func = partial(
            arcpy.management.AddIndex,
            # ArcPy2.8.0: Convert to str.
            in_table=str(dataset_path),
            fields=field_names,
            index_name=kwargs["index_name"],
            unique=kwargs["is_unique"],
            ascending=kwargs["is_ascending"],
        )
    try:
        func()
    except arcpy.ExecuteError as error:
        if error.message.startswith("ERROR 000464"):
            LOG.warning("Lock on `%s` prevents adding index.", dataset_path)
            if not kwargs["fail_on_lock_ok"]:
                raise

    LOG.log(level, "End: Add.")
    return dataset_path


def as_feature_set(dataset_path, field_names=None, **kwargs):
    """Return dataset as feature set.

    Args:
        dataset_path (pathlib.Path, str): Path of the dataset.
        field_names (iter): Collection of field names to include in output. If
            field_names not specified or None, all fields will be included.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        force_record_set (bool): Return record set if True, whichever type matches the
            input if False. Default is False.

    Returns:
        arcpy.FeatureSet
    """
    dataset_path = Path(dataset_path)
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("force_record_set", False)
    if field_names is not None:
        field_names = list(field_names)
    view = DatasetView(
        dataset_path,
        field_names=field_names,
        dataset_where_sql=kwargs["dataset_where_sql"],
    )
    with view:
        if kwargs["force_record_set"] or not view.is_spatial:
            return arcpy.RecordSet(table=view.name)

        return arcpy.FeatureSet(table=view.name)


def compress(dataset_path, **kwargs):
    """Compress dataset.

    Compression only applies to datasets in file geodatabases.

    Args:
        dataset_path (pathlib.Path, str): Path of the workspace.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        pathlib.Path: Path of the compressed dataset.
    """
    dataset_path = Path(dataset_path)
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(level, "Start: Compress dataset `%s`.", dataset_path)
    try:
        # ArcPy2.8.0: Convert to str.
        arcpy.management.CompressFileGeodatabaseData(in_data=str(dataset_path))
    except arcpy.ExecuteError as error:
        # Bad allocation error just means the dataset is too big to compress.
        if str(error) == (
            "bad allocation\nFailed to execute (CompressFileGeodatabaseData).\n"
        ):
            LOG.error("Compress error: bad allocation.")
        else:
            LOG.error("""str(error) = "%s\"""", error)
            LOG.error("""repr(error) = "%r\"""", error)
            raise

    LOG.log(level, "End: Compress.")
    return dataset_path


def copy(dataset_path, output_path, **kwargs):
    """Copy features into a new dataset.

    Args:
        dataset_path (pathlib.Path, str): Path of the dataset.
        output_path (pathlib.Path, str): Path of output dataset.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        field_names (iter): Collection of field names to include in output. If
            field_names not specified or None, all fields will be included.
        schema_only (bool): Copy only the schema--omitting data--if True. Default is
            False.
        overwrite (bool): Overwrite the output dataset if it exists, if True. Default is
            False.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        collections.Counter: Counts of features for each copy-state.

    Raises:
        ValueError: If dataset type not supported.
    """
    dataset_path = Path(dataset_path)
    output_path = Path(output_path)
    kwargs.setdefault("dataset_where_sql")
    if "field_names" not in kwargs or kwargs["field_names"] is None:
        kwargs["field_names"] = None
    else:
        kwargs["field_names"] = list(contain(kwargs["field_names"]))
    kwargs.setdefault("schema_only", False)
    kwargs.setdefault("overwrite", False)
    if kwargs["schema_only"]:
        kwargs["dataset_where_sql"] = "0=1"
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(level, "Start: Copy dataset `%s` to `%s`.", dataset_path, output_path)
    dataset_meta = dataset_metadata(dataset_path)
    view = DatasetView(
        dataset_path,
        field_names=kwargs["field_names"],
        dataset_where_sql=kwargs["dataset_where_sql"],
    )
    with view:
        if kwargs["overwrite"] and arcpy.Exists(output_path):
            delete(output_path, log_level=logging.DEBUG)
        if dataset_meta["is_spatial"]:
            arcpy.management.CopyFeatures(
                # ArcPy2.8.0: Convert to str.
                in_features=view.name,
                out_feature_class=str(output_path),
            )
        elif dataset_meta["is_table"]:
            # ArcPy2.8.0: Convert to str.
            arcpy.management.CopyRows(in_rows=view.name, out_table=str(output_path))
        else:
            raise ValueError(f"`{dataset_path}` unsupported dataset type.")

    LOG.log(level, "End: Copy.")
    states = Counter(copied=feature_count(output_path))
    return states


def create(dataset_path, field_metadata_list=None, geometry_type=None, **kwargs):
    """Create new dataset.

    Args:
        dataset_path (pathlib.Path, str): Path of the dataset .
        field_metadata_list (iter): Collection of field metadata mappings.
        geometry_type (str): Type of geometry, if a spatial dataset.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        spatial_reference_item: Item from which the spatial reference of the output
            geometry will be derived. Default is 4326 (EPSG code for unprojected WGS84).
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        pathlib.Path: Path of the dataset created.
    """
    dataset_path = Path(dataset_path)
    kwargs.setdefault("spatial_reference_item", 4326)
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(level, "Start: Create dataset `%s`.", dataset_path)
    if geometry_type:
        arcpy.management.CreateFeatureclass(
            # ArcPy2.8.0: Convert to str.
            out_path=str(dataset_path.parent),
            out_name=dataset_path.name,
            geometry_type=geometry_type,
            has_z=(
                "ENABLED"
                if isinstance(kwargs["spatial_reference_item"], (tuple, list))
                else "DISABLED"
            ),
            spatial_reference=SpatialReference(kwargs["spatial_reference_item"]).object,
        )
    else:
        arcpy.management.CreateTable(
            # ArcPy2.8.0: Convert to str.
            out_path=str(dataset_path.parent),
            out_name=dataset_path.name,
        )
    if field_metadata_list:
        for field_meta in field_metadata_list:
            add_field(dataset_path, log_level=logging.DEBUG, **field_meta)
    LOG.log(level, "End: Create.")
    return dataset_path


def delete(dataset_path, **kwargs):
    """Delete dataset.

    Args:
        dataset_path (pathlib.Path, str): Path of the dataset.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        pathlib.Path: Path of deleted dataset.
    """
    dataset_path = Path(dataset_path)
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(level, "Start: Delete dataset `%s`.", dataset_path)
    # ArcPy2.8.0: Convert to str.
    arcpy.management.Delete(in_data=str(dataset_path))
    LOG.log(level, "End: Delete.")
    return dataset_path


def delete_field(dataset_path, field_name, **kwargs):
    """Delete field from dataset.

    Args:
        dataset_path (pathlib.Path, str): Path of the dataset.
        field_name (str): Name of the field.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        str: Name of the field deleted.
    """
    dataset_path = Path(dataset_path)
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(level, "Start: Delete field `%s` on `%s`.", field_name, dataset_path)
    # ArcPy2.8.0: Convert to str.
    arcpy.management.DeleteField(in_table=str(dataset_path), drop_field=field_name)
    LOG.log(level, "End: Delete.")
    return field_name


def duplicate_field(dataset_path, field_name, new_field_name, **kwargs):
    """Create new field as a duplicate of another.

    Note: This does *not* duplicate the values of the original field; only the schema.

    Args:
        dataset_path (pathlib.Path, str): Path of the dataset.
        field_name (str): Name of the field.
        new_field_name (str): Name of the new field.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        str: Name of the field created.
    """
    dataset_path = Path(dataset_path)
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level,
        "Start: Duplicate field `%s on `%s` as `%s`.",
        field_name,
        dataset_path,
        new_field_name,
    )
    field = Field(dataset_path, field_name)
    field.name = new_field_name
    # Cannot add another OID-type field, so change to long.
    if field.type.upper() == "OID":
        field.type = "LONG"
    add_field(dataset_path, log_level=logging.DEBUG, **field.as_dict)
    LOG.log(level, "End: Duplicate.")
    return new_field_name


def feature_count(dataset_path, dataset_where_sql=None):
    """Return number of features in dataset.

    Args:
        dataset_path (pathlib.Path, str): Path of the dataset.
        dataset_where_sql (str): SQL where-clause for dataset subselection.

    Returns:
        int
    """
    dataset_path = Path(dataset_path)
    view = DatasetView(dataset_path, dataset_where_sql)
    with view:
        return view.count


def is_valid(dataset_path):
    """Check whether dataset is extant & valid.

    Args:
        dataset_path (pathlib.Path, str): Path of the dataset.

    Returns:
        bool
    """
    dataset_path = Path(dataset_path)
    exists = dataset_path and arcpy.Exists(dataset=dataset_path)
    if exists:
        try:
            valid = dataset_metadata(dataset_path)["is_table"]
        except IOError:
            valid = False
    else:
        valid = False
    return valid


def join_field(
    dataset_path,
    join_dataset_path,
    join_field_name,
    on_field_name,
    on_join_field_name,
    **kwargs,
):
    """Add field and its values from join-dataset.

    Args:
        dataset_path (pathlib.Path, str): Path of the dataset.
        join_dataset_path (pathlib.Path, str): Path of the dataset to join field from.
        join_field_name (str): Name of the field to join.
        on_field_name (str): Name of the field to join the dataset on.
        on_join_field_name (str): Name of the field to join the join-dataset on.
        **kwargs: Arbitrary keyword arguments. See below.

   Keyword Args:
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        str: Name of the joined field.
    """
    dataset_path = Path(dataset_path)
    join_dataset_path = Path(join_dataset_path)
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level,
        "Start: Join field `%s` onto `%s` from `%s`.",
        join_field_name,
        dataset_path,
        join_dataset_path,
    )
    arcpy.management.JoinField(
        # ArcPy2.8.0: Convert to str.
        in_data=str(dataset_path),
        in_field=on_field_name,
        # ArcPy2.8.0: Convert to str.
        join_table=str(join_dataset_path),
        join_field=on_join_field_name,
        fields=[join_field_name],
    )
    LOG.log(level, "End: Join.")
    return join_field_name


def remove_all_default_field_values(dataset_path, **kwargs):
    """Remove all default field values in dataset.

    Args:
        dataset_path (pathlib.Path, str): Path of the dataset.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        pathlib.Path: Path of dataset.
    """
    dataset_path = Path(dataset_path)
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(level, "Start: Remove all default field values for `%s`.", dataset_path)
    field_names = [
        field["name"]
        for field in dataset_metadata(dataset_path)["fields"]
        if field["default_value"] is not None
    ]
    subtype_codes = [
        code
        for code, meta in arcpy.da.ListSubtypes(dataset_path).items()
        if meta["SubtypeField"]
    ]
    for field_name in field_names:
        LOG.log(level, "Removing default value for `%s`.", field_name)
        set_default_field_value(
            dataset_path,
            field_name,
            value=None,
            subtype_codes=subtype_codes,
            log_level=logging.DEBUG,
        )
    LOG.log(level, "End: Remove.")
    return dataset_path


def set_default_field_value(dataset_path, field_name, value=None, **kwargs):
    """Set a default value for field.

    Args:
        dataset_path (pathlib.Path, str): Path of the dataset.
        field_name (str): Name of the field.
        value (object): Default value to assign.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        subtype_codes (int): Codes for subtypes.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        str: Name of the field.
    """
    dataset_path = Path(dataset_path)
    if "subtype_codes" in kwargs:
        kwargs["subtype_codes"] = list(contain(kwargs["subtype_codes"]))
    else:
        kwargs["subtype_codes"] = []
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level,
        "Start: Set default value for field `%s` on `%s` to `%s`.",
        field_name,
        dataset_path,
        value,
    )
    if dataset_path.parent == Path("in_memory"):
        raise OSError("Cannot change field default in `in_memory` workspace")

    arcpy.management.AssignDefaultToField(
        # ArcPy2.8.0: Convert to str.
        in_table=str(dataset_path),
        field_name=field_name,
        default_value=value if value is not None else "",
        subtype_code=kwargs["subtype_codes"],
        clear_value=value is None,
    )
    LOG.log(level, "End: Set.")
    return field_name


def rename_field(dataset_path, field_name, new_field_name, **kwargs):
    """Rename field.

    Args:
        dataset_path (pathlib.Path, str): Path of the dataset.
        field_name (str): Name of the field.
        new_field_name (str): New name for the field.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        str: New name of the field.
    """
    dataset_path = Path(dataset_path)
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level,
        "Start: Rename field `%s` on `%s` to `%s`.",
        field_name,
        dataset_path,
        new_field_name,
    )
    arcpy.management.AlterField(
        # ArcPy2.8.0: Convert to str.
        in_table=str(dataset_path),
        field=field_name,
        new_field_name=new_field_name,
    )
    LOG.log(level, "End: Rename.")
    return new_field_name


def set_privileges(dataset_path, user_name, allow_view=None, allow_edit=None, **kwargs):
    """Set privileges for dataset in enterprise geodatabase.

    For the allow-flags, True = grant; False = revoke; None = as is.

    Args:
        dataset_path (pathlib.Path, str): Path of the dataset.
        allow_view (bool): Set view privileges to "GRANT" if True, "REVOKE" if False.
            Any other value will keep as-is.
        allow_edit (bool): Set edit privileges to "GRANT" if True, "REVOKE" if False.
            Any other value will keep as-is.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        pathlib.Path: Path of the dataset with changed privileges.
    """
    dataset_path = Path(dataset_path)
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(
        level,
        """Start: Set privileges on `%s` for `%s` (view="%s", edit="%s").""",
        dataset_path,
        user_name,
        allow_view,
        allow_edit,
    )
    arcpy.management.ChangePrivileges(
        in_dataset=dataset_path,
        user=user_name,
        View={True: "GRANT", False: "REVOKE"}.get(allow_view, "AS_IS"),
        Edit={True: "GRANT", False: "REVOKE"}.get(allow_edit, "AS_IS"),
    )
    LOG.log(level, "End: Set.")
    return dataset_path
