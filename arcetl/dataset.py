"""Dataset operations."""
from collections import Counter
import logging
import os

import arcpy

from arcetl.arcobj import (
    DatasetView,
    dataset_metadata,
    field_metadata,
    spatial_reference,
)
from arcetl.helpers import contain, leveled_logger


LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""

arcpy.SetLogHistory(False)


def add_field(dataset_path, name, **kwargs):
    """Add field to dataset.

    Args:
        dataset_path (str): Path of the dataset.
        name (str): Name of the field.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        type (str): Data type of the field. Default is "text".
        length (int): Length of field. Only applies to text fields. Default is 64.
        precision (int): Precision of field. Only applies to float/double fields.
        scale (int): Scale of field. Only applies to float/double fields.
        is_nullable (bool): Field can be nullable if True. Default is True.
        is_required (bool): Field value will be required for feature if True.
            Default is False.
        exist_ok (bool): If field already exists: will raise an error if False;
            will act as if field was added if True. Default is False.
        log_level (str): Level to log the function at. Default is "info".

    Returns:
        str: Name of the field added.

    Raises:
        RuntimeError: If `exist_ok=False` and field already exists.
    """
    log = leveled_logger(LOG, kwargs.get("log_level", "info"))
    log("Start: Add field %s to %s.", name, dataset_path)
    if arcpy.ListFields(dataset_path, name):
        LOG.info("Field already exists.")
        if not kwargs.get("exist_ok", False):
            raise RuntimeError("Cannot add existing field (exist_ok=False).")

    else:
        default_add_kwargs = {
            "type": "text",
            "precision": None,
            "scale": None,
            "length": 64,
            "is_nullable": True,
            "is_required": False,
        }
        add_kwargs = {}
        for key, default in default_add_kwargs.items():
            add_kwargs["field_" + key] = kwargs[key] if key in kwargs else default
        arcpy.management.AddField(dataset_path, name, **add_kwargs)
    log("End: Add.")
    return name


def add_index(dataset_path, field_names, **kwargs):
    """Add index to dataset fields.

    Note:
        Index names can only be applied to non-spatial indexes for geodatabase feature
        classes and tables.

        There is a limited length allowed for index names; longer names will be
        truncated without warning.

    Args:
        dataset_path (str): Path of the dataset.
        field_names (iter): Collection of participating field names.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        index_name (str): Name for index. Optional; see note.
        is_ascending (bool): Build index in ascending order if True. Default is False.
        is_unique (bool): Build index with unique constraint if True. Default is False.
        fail_on_lock_ok (bool): If True, indicate success even if dataset locks prevent
            adding index. Default is False.
        log_level (str): Level to log the function at. Default is "info".

    Returns:
        str: Path of the dataset receiving the index.

    Raises:
        RuntimeError: If more than one field and any are geometry-types.
        arcpy.ExecuteError: If dataset lock prevents adding index.
    """
    field_names = [name.lower() for name in contain(field_names)]
    kwargs.setdefault("index_name", "ndx_" + "_".join(field_names))
    kwargs.setdefault("is_ascending", False)
    kwargs.setdefault("is_unique", False)
    kwargs.setdefault("fail_on_lock_ok", False)
    log = leveled_logger(LOG, kwargs.setdefault("log_level", "info"))
    log("Start: Add index to field(s) %s on %s.", field_names, dataset_path)
    meta = {"dataset": dataset_metadata(dataset_path)}
    meta["field_types"] = {
        field["type"].lower()
        for field in meta["dataset"]["fields"]
        if field["name"].lower() in field_names
    }
    if "geometry" in meta["field_types"]:
        if len(field_names) > 1:
            raise RuntimeError("Cannot create a composite spatial index.")

        exec_add = arcpy.management.AddSpatialIndex
        add_kwargs = {"in_features": dataset_path}
    else:
        exec_add = arcpy.management.AddIndex
        add_kwargs = {
            "in_table": dataset_path,
            "fields": field_names,
            "index_name": kwargs["index_name"],
            "unique": kwargs["is_unique"],
            "ascending": kwargs["is_ascending"],
        }
    try:
        exec_add(**add_kwargs)
    except arcpy.ExecuteError as error:
        if error.message.startswith("ERROR 000464"):
            LOG.warning("Lock on %s prevents adding index.", dataset_path)
            if not kwargs["fail_on_lock_ok"]:
                raise

    log("End: Add.")
    return dataset_path


def compress(dataset_path, **kwargs):
    """Compress dataset.

    Compression only applies to datasets in file geodatabases.

    Args:
        dataset_path (str): Path of the workspace.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (str): Level to log the function at. Default is "info".

    Returns:
        str: Path of the compressed dataset.
    """
    log = leveled_logger(LOG, kwargs.setdefault("log_level", "info"))
    log("Start: Compress dataset %s.", dataset_path)
    try:
        arcpy.management.CompressFileGeodatabaseData(dataset_path)
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

    log("End: Compress.")
    return dataset_path


def copy(dataset_path, output_path, **kwargs):
    """Copy features into a new dataset.

    Args:
        dataset_path (str): Path of the dataset.
        output_path (str): Path of output dataset.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        schema_only (bool): Copy only the schema--omitting data--if True. Default is
            False.
        overwrite (bool): Overwrite the output dataset if it exists, if True. Default is
            False.
        log_level (str): Level to log the function at. Default is "info".

    Returns:
        collections.Counter: Counts for each feature action.

    Raises:
        ValueError: If dataset type not supported.
    """
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("schema_only", False)
    kwargs.setdefault("overwrite", False)
    if kwargs["schema_only"]:
        kwargs["dataset_where_sql"] = "0=1"
    log = leveled_logger(LOG, kwargs.setdefault("log_level", "info"))
    log("Start: Copy dataset %s to %s.", dataset_path, output_path)
    meta = {"dataset": dataset_metadata(dataset_path)}
    view = DatasetView(dataset_path, kwargs["dataset_where_sql"])
    with view:
        if meta["dataset"]["is_spatial"]:
            exec_copy = arcpy.management.CopyFeatures
        elif meta["dataset"]["is_table"]:
            exec_copy = arcpy.management.CopyRows
        else:
            raise ValueError("{} unsupported dataset type.".format(dataset_path))

        if kwargs["overwrite"] and arcpy.Exists(output_path):
            delete(output_path, log_level=None)
        exec_copy(view.name, output_path)
    log("End: Copy.")
    return Counter(copied=feature_count(output_path))


def create(dataset_path, field_metadata_list=None, **kwargs):
    """Create new dataset.

    Args:
        dataset_path (str): Path of the dataset .
        field_metadata_list (iter): Collection of field metadata mappings.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        geometry_type (str): Type of geometry, if a spatial dataset.
        spatial_reference_item: Item from which the spatial reference of the output
            geometry will be derived. Default is 4326 (EPSG code for unprojected WGS84).
        log_level (str): Level to log the function at. Default is "info".

    Returns:
        str: Path of the dataset created.
    """
    kwargs.setdefault("geometry_type")
    kwargs.setdefault("spatial_reference_item", 4326)
    log = leveled_logger(LOG, kwargs.setdefault("log_level", "info"))
    log("Start: Create dataset %s.", dataset_path)
    create_kwargs = {
        "out_path": os.path.dirname(dataset_path),
        "out_name": os.path.basename(dataset_path),
    }
    if kwargs["geometry_type"]:
        exec_create = arcpy.management.CreateFeatureclass
        create_kwargs["geometry_type"] = kwargs["geometry_type"]
        create_kwargs["spatial_reference"] = spatial_reference(
            kwargs["spatial_reference_item"]
        )
    else:
        exec_create = arcpy.management.CreateTable
    exec_create(**create_kwargs)
    if field_metadata_list:
        for field_meta in field_metadata_list:
            add_field(dataset_path, log_level=None, **field_meta)
    log("End: Create.")
    return dataset_path


def delete(dataset_path, **kwargs):
    """Delete dataset.

    Args:
        dataset_path (str): Path of the dataset.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (str): Level to log the function at. Default is "info".

    Returns:
        str: Path of deleted dataset.
    """
    log = leveled_logger(LOG, kwargs.setdefault("log_level", "info"))
    log("Start: Delete dataset %s.", dataset_path)
    arcpy.management.Delete(in_data=dataset_path)
    log("End: Delete.")
    return dataset_path


def delete_field(dataset_path, field_name, **kwargs):
    """Delete field from dataset.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (str): Level to log the function at. Default is "info".

    Returns:
        str: Name of the field deleted.
    """
    log = leveled_logger(LOG, kwargs.setdefault("log_level", "info"))
    log("Start: Delete field %s on %s.", field_name, dataset_path)
    arcpy.management.DeleteField(in_table=dataset_path, drop_field=field_name)
    log("End: Delete.")
    return field_name


def duplicate_field(dataset_path, field_name, new_field_name, **kwargs):
    """Create new field as a duplicate of another.

    Note: This does *not* duplicate the values of the original field; only the schema.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        new_field_name (str): Name of the new field.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (str): Level to log the function at. Default is "info".

    Returns:
        str: Name of the field created.
    """
    log = leveled_logger(LOG, kwargs.setdefault("log_level", "info"))
    log(
        "Start: Duplicate field %s as %s on %s.",
        field_name,
        new_field_name,
        dataset_path,
    )
    meta = {"field": field_metadata(dataset_path, field_name)}
    meta["field"]["name"] = new_field_name
    # Cannot add OID-type field, so change to long.
    if meta["field"]["type"].lower() == "oid":
        meta["field"]["type"] = "long"
    add_field(dataset_path, log_level=None, **meta["field"])
    log("End: Duplicate.")
    return new_field_name


def feature_count(dataset_path, **kwargs):
    """Return number of features in dataset.

    Args:
        dataset_path (str): Path of the dataset.
        **kwargs: Arbitrary keyword arguments. See below.

   Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.

    Returns:
        int.
    """
    kwargs.setdefault("dataset_where_sql")
    view = DatasetView(dataset_path, **kwargs)
    with view:
        return view.count


def is_valid(dataset_path):
    """Check whether dataset exists/is valid.

    Args:
        dataset_path (str): Path of the dataset.

    Returns:
        bool: True if dataset is valid, False otherwise.
    """
    valid = (
        dataset_path
        and arcpy.Exists(dataset_path)
        and dataset_metadata(dataset_path)["is_table"]
    )
    return valid


def join_field(
    dataset_path,
    join_dataset_path,
    join_field_name,
    on_field_name,
    on_join_field_name,
    **kwargs
):
    """Add field and its values from join-dataset.

    Args:
        dataset_path (str): Path of the dataset.
        join_dataset_path (str): Path of the dataset to join field from.
        join_field_name (str): Name of the field to join.
        on_field_name (str): Name of the field to join the dataset on.
        on_join_field_name (str): Name of the field to join the join-dataset on.
        **kwargs: Arbitrary keyword arguments. See below.

   Keyword Args:
        log_level (str): Level to log the function at. Default is "info".

    Returns:
        str: Name of the joined field.
    """
    log = leveled_logger(LOG, kwargs.setdefault("log_level", "info"))
    log(
        "Start: Join field %s on %s from %s.",
        join_field_name,
        dataset_path,
        join_dataset_path,
    )
    arcpy.management.JoinField(
        in_data=dataset_path,
        in_field=on_field_name,
        join_table=join_dataset_path,
        join_field=on_join_field_name,
        fields=[join_field_name],
    )
    log("End: Join.")
    return join_field_name


def rename_field(dataset_path, field_name, new_field_name, **kwargs):
    """Rename field.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        new_field_name (str): New name for the field.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (str): Level to log the function at. Default is "info".

    Returns:
        str: New name of the field.
    """
    log = leveled_logger(LOG, kwargs.setdefault("log_level", "info"))
    log("Start: Rename field %s to %s on %s.", field_name, new_field_name, dataset_path)
    arcpy.management.AlterField(
        in_table=dataset_path, field=field_name, new_field_name=new_field_name
    )
    log("End: Rename.")
    return new_field_name


def set_privileges(dataset_path, user_name, allow_view=None, allow_edit=None, **kwargs):
    """Set privileges for dataset in enterprise geodatabase.

    For the allow-flags, True = grant; False = revoke; None = as is.

    Args:
        dataset_path (str): Path of the dataset.
        allow_view (bool): Set view privileges to "grant" if True, "revoke" if False,
            "as_is" with any other value.
        allow_edit (bool): Set edit privileges to "grant" if True, "revoke" if False,
            "as_is" with any other value.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (str): Level to log the function at. Default is "info".

    Returns:
        str: Path of the dataset with changed privileges.
    """
    log = leveled_logger(LOG, kwargs.setdefault("log_level", "info"))
    privilege_keyword = {True: "grant", False: "revoke", None: "as_is"}
    privilege = {
        "View": privilege_keyword.get(allow_view, "as_is"),
        "Edit": privilege_keyword.get(allow_edit, "as_is"),
    }
    log(
        "Start: Set privileges on dataset %s for %s to view=%s, edit=%s.",
        dataset_path,
        user_name,
        privilege["View"],
        privilege["Edit"],
    )
    arcpy.management.ChangePrivileges(
        in_dataset=dataset_path, user=user_name, **privilege
    )
    log("End: Set.")
    return dataset_path
