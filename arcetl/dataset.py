"""Dataset operations."""
import logging
import os

import arcpy

from arcetl import arcobj
from arcetl import helpers


LOG = logging.getLogger(__name__)


def add_field(dataset_path, field_name, field_type, **kwargs):
    """Add field to dataset.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        field_type (str): Data type of the field.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        exist_ok (bool): Flag indicating whether field already existing treated
            same as field being added. Defaults to False.
        field_is_nullable (bool): Flag to indicate if field can be null.
            Defaults to True.
        field_is_required (bool): Dlag to indicate if field is required.
            Defaults to False.
        field_length (int): Length of field. Only applies to text fields.
            Defaults to 64.
        field_precision (int): Precision of the field. Only applies to
            float/double fields.
        field_scale (int): Scale of the field. Only applies to
            float/double fields.
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        str: Name of the field added.

    Raises:
        RuntimeError: If `exist_ok=False` and field already exists.
    """
    log_level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(log_level, "Start: Add field %s to %s.", field_name, dataset_path)
    if arcpy.ListFields(dataset_path, field_name):
        LOG.warning("Field already exists.")
        if not kwargs.get('exist_ok', False):
            raise RuntimeError("Cannot add existing field (exist_ok=False).")
    else:
        arcpy.management.AddField(
            in_table=dataset_path, field_name=field_name, field_type=field_type,
            field_length=kwargs.get('field_length', 64),
            field_precision=kwargs.get('field_precision'),
            field_scale=kwargs.get('field_scale'),
            field_is_nullable=kwargs.get('field_is_nullable', True),
            field_is_required=kwargs.get('field_is_required', False),
            )
    LOG.log(log_level, "End: Add.")
    return field_name


def add_field_from_metadata(dataset_path, add_metadata, **kwargs):
    """Add field to dataset from metadata dictionary.

    Args:
        dataset_path (str): Path of the dataset.
        add_metadata (dict): Metadata with field properties for adding.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        exist_ok (bool): Flag indicating whether field already existing
            is considered a successful 'add'. Defaults to False.
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        str: Name of the field added.
    """
    log_level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(log_level, "Start: Add field %s to %s.",
            add_metadata['name'], dataset_path)
    field_keywords = ('name', 'type', 'length', 'precision', 'scale',
                      'is_nullable', 'is_required')
    add_kwargs = {'field_{}'.format(keyword): add_metadata[keyword]
                  for keyword in field_keywords if keyword in add_metadata}
    add_field(dataset_path, exist_ok=kwargs.get('exist_ok', False),
              log_level=None, **add_kwargs)
    LOG.log(log_level, "End: Add.")
    return add_metadata['name']


def add_index(dataset_path, field_names, **kwargs):
    """Add index to dataset fields.

    Note:
        Index names can only be applied to non-spatial indexes for
        geodatabase feature classes and tables.
        There is a limited length allowed for index names; longer names will
        be truncated without warning.

    Args:
        dataset_path (str): Path of the dataset.
        field_names (iter): Collections with names of participating
            fields.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        fail_on_lock_ok (bool): Flag to indicate success even if dataset
            locks prevent adding index. Defaults to False.
        index_name (str): Name for index. Optional; see note.
        is_ascending (bool): Flag to indicate index to be built in ascending
            order. Defaults to False.
        is_unique (bool): Flag to indicate index to be built with unique
            constraint. Defaults to False.
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        str: Path of the dataset receiving the index.

    Raises:
        RuntimeError: If more than one field and any are geometry-types.
        arcpy.ExecuteError: If dataset lock prevents adding index.
    """
    log_level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(log_level, "Start: Add index to field(s) %s on %s.",
            field_names, dataset_path)
    index_types = {field['type'].lower() for field
                   in arcobj.dataset_metadata(dataset_path)['fields']
                   if field['name'].lower() in (name.lower()
                                                for name in field_names)}
    if 'geometry' in index_types:
        if len(field_names) > 1:
            raise RuntimeError("Cannot create a composite spatial index.")
        add_function = arcpy.management.AddSpatialIndex
        add_kwargs = {'in_features': dataset_path}
    else:
        add_function = arcpy.management.AddIndex
        add_kwargs = {
            'in_table': dataset_path, 'fields': field_names,
            'index_name': kwargs.get('index_name',
                                     '_'.join(('ndx',) + tuple(field_names))),
            'unique': kwargs.get('is_unique', False),
            'ascending': kwargs.get('is_ascending', False),
            }
    try:
        add_function(**add_kwargs)
    except arcpy.ExecuteError as error:
        if all((kwargs.get('fail_on_lock_ok', False),
                error.message.startswith('ERROR 000464'))):
            LOG.warning("Lock on %s prevents adding index.", dataset_path)
        else:
            raise
    LOG.log(log_level, "End: Add.")
    return dataset_path


def copy(dataset_path, output_path, **kwargs):
    """Copy features into a new dataset.

    Args:
        dataset_path (str): Path of the dataset.
        output_path (str): Path of output dataset.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level to log the function at. Defaults to 'info'.
        overwrite (bool): Flag to overwrite the output, if it exists.
            Defaults to False.
        schema_only (bool): Flag to only copy the schema, omitting data.
            Defaults to False.

    Returns:
        str: Path of the output dataset.

    Raises:
        ValueError: If dataset type not supported.
    """
    log_level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(log_level, "Start: Copy dataset %s to %s.",
            dataset_path, output_path)
    _dataset = {
        'meta': arcobj.dataset_metadata(dataset_path),
        'view': arcobj.DatasetView(
            dataset_path, ("0=1" if kwargs.get('schema_only', False)
                           else kwargs.get('dataset_where_sql'))
            ),
        }
    with _dataset['view']:
        if _dataset['meta']['is_spatial']:
            function = arcpy.management.CopyFeatures
        elif _dataset['meta']['is_table']:
            function = arcpy.management.CopyRows
        else:
            raise ValueError(
                "{} unsupported dataset type.".format(dataset_path)
                )
        if kwargs.get('overwrite', False) and arcpy.Exists(output_path):
            delete(output_path, log_level=None)
        function(_dataset['view'].name, output_path)
    LOG.log(log_level, "End: Copy.")
    return output_path


def create(dataset_path, field_metadata_list=None, **kwargs):
    """Create new dataset.

    Args:
        dataset_path (str): Path of the dataset .
        field_metadata_list (iter): Collection of field metadata dictionaries.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        geometry_type (str): Type of geometry, if a spatial dataset.
        spatial_reference_id (int): EPSG code for spatial reference, if a
            spatial dataset. Defaults to 4326 (WGS 84).
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        str: Path of the dataset created.
    """
    log_level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(log_level, "Start: Create dataset %s.", dataset_path)
    create_kwargs = {'out_path': os.path.dirname(dataset_path),
                     'out_name': os.path.basename(dataset_path)}
    if kwargs.get('geometry_type'):
        create_function = arcpy.management.CreateFeatureclass
        create_kwargs['geometry_type'] = kwargs['geometry_type']
        # Default to EPSG 4326 (unprojected WGS 84).
        create_kwargs['spatial_reference'] = arcobj.spatial_reference(
            kwargs.get('spatial_reference_id', 4326)
            )
    else:
        create_function = arcpy.management.CreateTable
    create_function(**create_kwargs)
    if field_metadata_list:
        for field_meta in field_metadata_list:
            add_field_from_metadata(dataset_path, field_meta, log_level=None)
    LOG.log(log_level, "End: Create.")
    return dataset_path


def delete(dataset_path, **kwargs):
    """Delete dataset.

    Args:
        dataset_path (str): Path of the dataset.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        str: Path of the dataset deleted.
    """
    log_level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(log_level, "Start: Delete dataset %s.", dataset_path)
    arcpy.management.Delete(in_data=dataset_path)
    LOG.log(log_level, "End: Delete.")
    return dataset_path


def delete_field(dataset_path, field_name, **kwargs):
    """Delete field from dataset.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        str: Name of the field deleted.
    """
    log_level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(log_level, "Start: Delete field %s on %s.",
            field_name, dataset_path)
    arcpy.management.DeleteField(in_table=dataset_path, drop_field=field_name)
    LOG.log(log_level, "End: Delete.")
    return field_name


def duplicate_field(dataset_path, field_name, new_field_name, **kwargs):
    """Create new field as a duplicate of another.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        new_field_name (str): Name of the field to call duplicate.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        str: Name of the field created.
    """
    log_level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(log_level, "Start: Duplicate field %s as %s on %s.",
            field_name, new_field_name, dataset_path)
    field_meta = arcobj.field_metadata(dataset_path, field_name)
    field_meta['name'] = new_field_name
    # Cannot add OID-type field, so change to long.
    if field_meta['type'].lower() == 'oid':
        field_meta['type'] = 'long'
    add_field_from_metadata(dataset_path, field_meta, log_level=None)
    LOG.log(log_level, "End: Duplicate.")
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
    with arcpy.da.SearchCursor(
        in_table=dataset_path, field_names=('oid@',),
        where_clause=kwargs.get('dataset_where_sql')
        ) as cursor:
        count = len(tuple(None for _ in cursor))
    return count


field_metadata = arcobj.field_metadata  # pylint: disable=invalid-name


def is_valid(dataset_path):
    """Check whether dataset exists/is valid.

    Args:
        dataset_path (str): Path of the dataset.

    Returns:
        bool: Dataset is valid (True) or not (False).
    """
    return (dataset_path is not None and arcpy.Exists(dataset_path)
            and arcobj.dataset_metadata(dataset_path)['is_table'])


def join_field(dataset_path, join_dataset_path, join_field_name,
               on_field_name, on_join_field_name, **kwargs):
    """Add field and its values from join-dataset.

    Args:
        dataset_path (str): Path of the dataset.
        join_dataset_path (str): Path of the dataset to join field from.
        join_field_name (str): Name of the field to join.
        on_field_name (str): Name of the field to join the dataset on.
        on_join_field_name (str): Name of the field to join the join-dataset
            on.
        **kwargs: Arbitrary keyword arguments. See below.

   Keyword Args:
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        str: Name of the joined field.
    """
    log_level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(log_level, "Start: Join field %s on %s from %s.",
            join_field_name, dataset_path, join_dataset_path)
    arcpy.management.JoinField(
        in_data=dataset_path, in_field=on_field_name,
        join_table=join_dataset_path, join_field=on_join_field_name,
        fields=join_field_name
        )
    LOG.log(log_level, "End: Join.")
    return join_field_name


metadata = arcobj.dataset_metadata  # pylint: disable=invalid-name


def rename_field(dataset_path, field_name, new_field_name, **kwargs):
    """Rename field.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        new_field_name (str): Name to change field to.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        str: New name of the field.
    """
    log_level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(log_level, "Start: Rename field %s to %s on %s.",
            field_name, new_field_name, dataset_path)
    arcpy.management.AlterField(in_table=dataset_path, field=field_name,
                                new_field_name=new_field_name)
    LOG.log(log_level, "End: Rename.")
    return new_field_name


def set_privileges(dataset_path, user_name, allow_view=None, allow_edit=None,
                   **kwargs):
    """Set privileges for dataset in enterprise geodatabase.

    For the allow-flags, True = grant; False = revoke; None = as is.

    Args:
        dataset_path (str): Path of the dataset.
        allow_view (bool): Flag to allow or revoke view privileges.
        allow_edit (bool): Flag to allow or revoke edit privileges.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        str: Path of the dataset with changed privileges.
    """
    log_level = helpers.log_level(kwargs.get('log_level', 'info'))
    privilege_map = {True: 'grant', False: 'revoke', None: 'as_is'}
    view_arg, edit_arg = privilege_map[allow_view], privilege_map[allow_edit]
    LOG.log(log_level,
            "Start: Set privileges on dataset %s for %s to view=%s, edit=%s.",
            dataset_path, user_name, view_arg, edit_arg)
    arcpy.management.ChangePrivileges(in_dataset=dataset_path, user=user_name,
                                      View=view_arg, Edit=edit_arg)
    LOG.log(log_level, "End: Set.")
    return dataset_path
