"""Dataset operations."""
import logging
import os

import arcpy

from arcetl import arcobj
from arcetl.helpers import contain, leveled_logger


LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""


def add_field(dataset_path, field_name, field_type, **kwargs):
    """Add field to dataset.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        field_type (str): Data type of the field.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        field_is_nullable (bool): Flag to indicate if field can be null. Default is
            True.
        field_is_required (bool): Flag to indicate if field is required. Default is
            False.
        field_length (int): Length of field. Only applies to text fields. Default is
            64.
        field_precision (int): Precision of the field. Only applies to float/double
            fields.
        field_scale (int): Scale of the field. Only applies to float/double fields.
        exist_ok (bool): Flag indicating whether field already existing treated same as
            field being added. Default is False.
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        str: Name of the field added.

    Raises:
        RuntimeError: If `exist_ok=False` and field already exists.

    """
    kwargs.setdefault('field_length', 64)
    kwargs.setdefault('field_precision')
    kwargs.setdefault('field_scale')
    kwargs.setdefault('field_is_nullable', True)
    kwargs.setdefault('field_is_required', False)
    kwargs.setdefault('exist_ok', False)
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    log("Start: Add field %s to %s.", field_name, dataset_path)
    if arcpy.ListFields(dataset_path, field_name):
        LOG.warning("Field already exists.")
        if not kwargs['exist_ok']:
            raise RuntimeError("Cannot add existing field (exist_ok=False).")

    else:
        add_kwargs = {key: kwargs[key] for key in kwargs if key.startswith('field_')}
        arcpy.management.AddField(dataset_path, field_name, field_type, **add_kwargs)
    log("End: Add.")
    return field_name


def add_field_from_metadata(dataset_path, add_metadata, **kwargs):
    """Add field to dataset from metadata dictionary.

    Args:
        dataset_path (str): Path of the dataset.
        add_metadata (dict): Metadata with field properties for adding.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        exist_ok (bool): Flag indicating whether field already existing is considered a
            successful 'add'. Default is False.
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        str: Name of the field added.

    """
    field_keywords = [
        'name', 'type', 'length', 'precision', 'scale', 'is_nullable', 'is_required'
    ]
    add_kwargs = {
        'field_' + keyword: add_metadata[keyword]
        for keyword in field_keywords
        if keyword in add_metadata
    }
    add_kwargs.update(kwargs)
    result = add_field(dataset_path, **add_kwargs)
    return result


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
        is_ascending (bool): Flag to indicate index to be built in ascending order.
            Default is False.
        is_unique (bool): Flag to indicate index to be built with unique constraint.
            Default is False.
        fail_on_lock_ok (bool): Flag to indicate success even if dataset locks prevent
            adding index. Default is False.
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        str: Path of the dataset receiving the index.

    Raises:
        RuntimeError: If more than one field and any are geometry-types.
        arcpy.ExecuteError: If dataset lock prevents adding index.

    """
    keys = {'field': list(contain(field_names))}
    kwargs.setdefault('index_name', 'ndx_' + '_'.join(field_names))
    kwargs.setdefault('is_ascending', False)
    kwargs.setdefault('is_unique', False)
    kwargs.setdefault('fail_on_lock_ok', False)
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    log("Start: Add index to field(s) %s on %s.", field_names, dataset_path)
    meta = {'dataset': arcobj.dataset_metadata(dataset_path)}
    keys['type'] = {
        field['type'].lower()
        for field in meta['dataset']['fields']
        if field['name'].lower() in (name.lower() for name in keys['field'])
    }
    if 'geometry' in keys['type']:
        if len(field_names) > 1:
            raise RuntimeError("Cannot create a composite spatial index.")

        _add_index = arcpy.management.AddSpatialIndex
        add_kwargs = {'in_features': dataset_path}
    else:
        _add_index = arcpy.management.AddIndex
        add_kwargs = {
            'in_table': dataset_path,
            'fields': keys['field'],
            'index_name': kwargs['index_name'],
            'unique': kwargs['is_unique'],
            'ascending': kwargs['is_ascending'],
        }
    try:
        _add_index(**add_kwargs)
    except arcpy.ExecuteError as error:
        if kwargs['fail_on_lock_ok'] and error.message.startswith('ERROR 000464'):
            LOG.warning("Lock on %s prevents adding index.", dataset_path)
        raise

    log("End: Add.")
    return dataset_path


def copy(dataset_path, output_path, **kwargs):
    """Copy features into a new dataset.

    Args:
        dataset_path (str): Path of the dataset.
        output_path (str): Path of output dataset.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        schema_only (bool): Flag to only copy the schema, omitting data. Default is
            False.
        overwrite (bool): Flag to overwrite the output, if it exists. Default is False.
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        str: Path of the output dataset.

    Raises:
        ValueError: If dataset type not supported.

    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('schema_only', False)
    kwargs.setdefault('overwrite', False)
    if kwargs['schema_only']:
        kwargs['dataset_where_sql'] = "0=1"
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    log("Start: Copy dataset %s to %s.", dataset_path, output_path)
    meta = {'dataset': arcobj.dataset_metadata(dataset_path)}
    view = arcobj.DatasetView(dataset_path, kwargs['dataset_where_sql'])
    with view:
        if meta['dataset']['is_spatial']:
            _copy = arcpy.management.CopyFeatures
        elif meta['dataset']['is_table']:
            _copy = arcpy.management.CopyRows
        else:
            raise ValueError("{} unsupported dataset type.".format(dataset_path))

        if kwargs['overwrite'] and arcpy.Exists(output_path):
            delete(output_path, log_level=None)
        _copy(view.name, output_path)
    log("End: Copy.")
    return output_path


def create(dataset_path, field_metadata_list=None, **kwargs):
    """Create new dataset.

    Args:
        dataset_path (str): Path of the dataset .
        field_metadata_list (iter): Collection of field metadata dictionaries.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        geometry_type (str): Type of geometry, if a spatial dataset.
        spatial_reference_item: Item from which the output geometry's spatial reference
            will be derived. Default is 4326 (EPSG code for unprojected WGS84).
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        str: Path of the dataset created.

    """
    kwargs.setdefault('geometry_type')
    kwargs.setdefault('spatial_reference_item', 4326)
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    log("Start: Create dataset %s.", dataset_path)
    meta = {'sref': arcobj.spatial_reference(kwargs['spatial_reference_item'])}
    create_kwargs = {
        'out_path': os.path.dirname(dataset_path),
        'out_name': os.path.basename(dataset_path),
    }
    if kwargs['geometry_type']:
        _create = arcpy.management.CreateFeatureclass
        create_kwargs['geometry_type'] = kwargs['geometry_type']
        create_kwargs['spatial_reference'] = meta['sref']
    else:
        _create = arcpy.management.CreateTable
    _create(**create_kwargs)
    if field_metadata_list:
        for field_meta in field_metadata_list:
            add_field_from_metadata(dataset_path, field_meta, log_level=None)
    log("End: Create.")
    return dataset_path


def delete(dataset_path, **kwargs):
    """Delete dataset.

    Args:
        dataset_path (str): Path of the dataset.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        str: Path of the dataset deleted.

    """
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
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
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        str: Name of the field deleted.

    """
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    log("Start: Delete field %s on %s.", field_name, dataset_path)
    arcpy.management.DeleteField(in_table=dataset_path, drop_field=field_name)
    log("End: Delete.")
    return field_name


def duplicate_field(dataset_path, field_name, new_field_name, **kwargs):
    """Create new field as a duplicate of another.

    Note: This does *not* duplicate the values in the original field.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        new_field_name (str): Name of the field to call duplicate.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        str: Name of the field created.

    """
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    log(
        "Start: Duplicate field %s as %s on %s.",
        field_name,
        new_field_name,
        dataset_path,
    )
    meta = {'field': arcobj.field_metadata(dataset_path, field_name)}
    meta['field']['name'] = new_field_name
    # Cannot add OID-type field, so change to long.
    if meta['field']['type'].lower() == 'oid':
        meta['field']['type'] = 'long'
    add_field_from_metadata(dataset_path, meta['field'], log_level=None)
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
        int: Number of features counted.

    """
    kwargs.setdefault('dataset_where_sql')
    view = arcobj.DatasetView(dataset_path, **kwargs)
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
        dataset_path is not None
        and arcpy.Exists(dataset_path)
        and arcobj.dataset_metadata(dataset_path)['is_table']
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
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        str: Name of the joined field.

    """
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
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
        fields=join_field_name,
    )
    log("End: Join.")
    return join_field_name


def rename_field(dataset_path, field_name, new_field_name, **kwargs):
    """Rename field.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        new_field_name (str): Name to change field to.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        str: New name of the field.

    """
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
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
        allow_view (bool): Flag to allow or revoke view privileges.
        allow_edit (bool): Flag to allow or revoke edit privileges.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        str: Path of the dataset with changed privileges.

    """
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    privilege_key = {True: 'grant', False: 'revoke', None: 'as_is'}
    log(
        "Start: Set privileges on dataset %s for %s to view=%s, edit=%s.",
        dataset_path,
        user_name,
        privilege_key[allow_view],
        privilege_key[allow_edit],
    )
    arcpy.management.ChangePrivileges(
        in_dataset=dataset_path,
        user=user_name,
        View=privilege_key[allow_view],
        Edit=privilege_key[allow_edit],
    )
    log("End: Set.")
    return dataset_path
