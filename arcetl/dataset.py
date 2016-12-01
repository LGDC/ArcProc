"""Dataset operations."""

import logging
import os

import arcpy

from arcetl import arcobj
from arcetl.helpers import LOG_LEVEL_MAP, unique_name


LOG = logging.getLogger(__name__)


def add_field(dataset_path, field_name, field_type, **kwargs):
    """Add field to dataset.

    Args:
        dataset_path (str): Path of dataset.
        field_name (str): Name of field.
        field_type (str): Type of field.
    Kwargs:
        field_length (int): Length of field.
        field_precision (int): Precision of field.
        field_scale (int): Scale of field.
        field_is_nullable (bool): Flag indicating if field will be nullable.
        field_is_required (bool): Flag indicating if field will be required.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [
            ('field_is_nullable', True), ('field_is_required', False),
            ('field_length', 64), ('field_precision', None),
            ('field_scale', None), ('log_level', 'info')
        ]:
        kwargs.setdefault(*kwarg_default)
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Add field %s to %s.", field_name, dataset_path)
    arcpy.management.AddField(
        in_table=dataset_path, field_name=field_name,
        field_type=arcobj.FIELD_TYPE_AS_ARC.get(field_type.lower(), field_type),
        field_length=kwargs['field_length'],
        field_precision=kwargs['field_precision'],
        field_scale=kwargs['field_scale'],
        field_is_nullable=kwargs['field_is_nullable'],
        field_is_required=kwargs['field_is_required']
        )
    LOG.log(log_level, "End: Add.")
    return field_name


def add_field_from_metadata(dataset_path, metadata, **kwargs):
    """Add field to dataset from metadata dictionary.

    Args:
        dataset_path (str): Path of dataset.
        metadata (dict): Metadata of field properties.
    Kwargs:
        log_level (str): Level at which to log this function.
    Returns:
        list.
    """
    for kwarg_default in [('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Add field %s to %s.",
            metadata['name'], dataset_path)
    field_keywords = ['name', 'type', 'length', 'precision', 'scale',
                      'is_nullable', 'is_required']
    add_kwargs = {'field_{}'.format(kw): metadata[kw]
                  for kw in field_keywords if kw in metadata}
    add_field(dataset_path, log_level=None, **add_kwargs)
    LOG.log(log_level, "End: Add.")
    return metadata['name']


def add_index(dataset_path, field_names, **kwargs):
    """Add index to dataset fields.

    Index names can only be applied to non-spatial indexes for geodatabase
    feature classes and tables. There is a limited length allowed from index
    names, which will be truncated to without warning.

    Args:
        dataset_path (str): Path of dataset.
        field_names (iter): Iterable of field names.
    Kwargs:
        fail_on_lock_ok (bool): Flag indicating success even if dataset locks
            prevent adding index.
        index_name (str): Optional name for index.
        is_ascending (bool): Flag indicating index built in ascending order.
        is_unique (bool): Flag indicating index built with unique constraint.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [
            ('fail_on_lock_ok', False),
            ('index_name', '_'.join(['ndx'] + field_names)),
            ('is_ascending', False), ('is_unique', False),
            ('log_level', 'info')
        ]:
        kwargs.setdefault(*kwarg_default)
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Add index to field(s) %s on %s.",
            field_names, dataset_path)
    index_types = {
        field['type'].lower() for field in metadata(dataset_path)['fields']
        if field['name'].lower() in (name.lower() for name in field_names)
        }
    if 'geometry' in index_types:
        if len(field_names) > 1:
            raise RuntimeError("Cannot create a composite spatial index.")
        add_function = arcpy.management.AddSpatialIndex
        add_kwargs = {'in_features': dataset_path}
    else:
        add_function = arcpy.management.AddIndex
        add_kwargs = {'in_table': dataset_path, 'fields': field_names,
                      'index_name': kwargs['index_name'],
                      'unique': kwargs['is_unique'],
                      'ascending': kwargs['is_ascending']}
    try:
        add_function(**add_kwargs)
    except arcpy.ExecuteError as error:
        if all([kwargs['fail_on_lock_ok'],
                error.message.startswith('ERROR 000464')]):
            LOG.warning("Lock on %s prevents adding index.", dataset_path)
        else:
            raise
    LOG.log(log_level, "End: Add.")
    return dataset_path


def copy(dataset_path, output_path, **kwargs):
    """Copy features into a new dataset.

    Args:
        dataset_path (str): Path of dataset.
        output_path (str): Path of output dataset.
    Kwargs:
        schema_only (bool): Flag to copy only the schema, omitting the data.
        overwrite (bool): Flag to overwrite an existing dataset at the path.
        sort_field_names (iter): Iterable of field names to sort on, in order.
        sort_reversed_field_names (iter): Iterable of field names (present in
            sort_field_names) to sort values in reverse-order.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [
            ('dataset_where_sql', None), ('log_level', 'info'),
            ('overwrite', False), ('schema_only', False),
            ('sort_field_names', []), ('sort_reversed_field_names', [])
        ]:
        kwargs.setdefault(*kwarg_default)
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Copy dataset %s to %s.",
            dataset_path, output_path)
    dataset_meta = metadata(dataset_path)
    dataset_view_name = create_view(
        unique_name('view'), dataset_path,
        dataset_where_sql=("0=1" if kwargs['schema_only']
                           else kwargs['dataset_where_sql']),
        log_level=None
        )
    if kwargs['sort_field_names']:
        copy_function = arcpy.management.Sort
        copy_kwargs = {
            'in_dataset': dataset_view_name,
            'out_dataset': output_path,
            'sort_field': [
                (name, 'descending')
                if name in kwargs['sort_reversed_field_names']
                else (name, 'ascending') for name in kwargs['sort_field_names']
                ],
            'spatial_sort_method': 'UR'
            }
    elif dataset_meta['is_spatial']:
        copy_function = arcpy.management.CopyFeatures
        copy_kwargs = {'in_features': dataset_view_name,
                       'out_feature_class': output_path}
    elif dataset_meta['is_table']:
        copy_function = arcpy.management.CopyRows
        copy_kwargs = {'in_rows': dataset_view_name, 'out_table': output_path}
    else:
        raise ValueError("{} unsupported dataset type.".format(dataset_path))
    if kwargs['overwrite'] and arcpy.Exists(output_path):
        delete(output_path, log_level=None)
    copy_function(**copy_kwargs)
    delete(dataset_view_name, log_level=None)
    LOG.log(log_level, "End: Copy.")
    return output_path


def create(dataset_path, field_metadata_list=None, **kwargs):
    """Create new dataset.

    Args:
        dataset_path (str): Path of dataset to create.
        field_metadata_list (iter): Iterable of field metadata dicts.
    Kwargs:
        geometry_type (str): Type of geometry, if a spatial dataset.
        spatial_reference_id (int): EPSG code for spatial reference, if a
            spatial dataset.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('geometry_type', None), ('log_level', 'info'),
                          ('spatial_reference_id', 4326)]:
        kwargs.setdefault(*kwarg_default)
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Create dataset %s.", dataset_path)
    create_kwargs = {'out_path': os.path.dirname(dataset_path),
                     'out_name': os.path.basename(dataset_path)}
    if kwargs['geometry_type']:
        create_function = arcpy.management.CreateFeatureclass
        create_kwargs['geometry_type'] = kwargs['geometry_type']
        # Default to EPSG 4326 (unprojected WGS 84).
        create_kwargs['spatial_reference'] = arcobj.spatial_reference_as_arc(
            kwargs['spatial_reference_id']
            )
    else:
        create_function = arcpy.management.CreateTable
    create_function(**create_kwargs)
    if field_metadata_list:
        for field_meta in field_metadata_list:
            add_field_from_metadata(dataset_path, field_meta, log_level=None)
    LOG.log(log_level, "End: Create.")
    return dataset_path


def create_view(view_name, dataset_path, **kwargs):
    """Create new view of dataset.

    Args:
        view_name (str): Name of view to create.
        dataset_path (str): Path of dataset.
    Kwargs:
        force_nonspatial (bool): Flag ensure view is nonspatial.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('dataset_where_sql', None),
                          ('force_nonspatial', False), ('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Create view %s of dataset %s.",
            view_name, dataset_path)
    dataset_meta = metadata(dataset_path)
    create_kwargs = {'where_clause': kwargs['dataset_where_sql'],
                     'workspace':  dataset_meta['workspace_path']}
    if dataset_meta['is_spatial'] and not kwargs['force_nonspatial']:
        create_function = arcpy.management.MakeFeatureLayer
        create_kwargs['in_features'] = dataset_path
        create_kwargs['out_layer'] = view_name
    elif dataset_meta['is_table']:
        create_function = arcpy.management.MakeTableView
        create_kwargs['in_table'] = dataset_path
        create_kwargs['out_view'] = view_name
    else:
        raise ValueError("{} unsupported dataset type.".format(dataset_path))
    create_function(**create_kwargs)
    LOG.log(log_level, "End: Create.")
    return view_name


def delete(dataset_path, **kwargs):
    """Delete dataset.

    Args:
        dataset_path (str): Path of dataset.
    Kwargs:
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Delete dataset %s.", dataset_path)
    arcpy.management.Delete(in_data=dataset_path)
    LOG.log(log_level, "End: Delete.")
    return dataset_path


def delete_field(dataset_path, field_name, **kwargs):
    """Delete field from dataset.

    Args:
        dataset_path (str): Path of dataset.
        field_name (str): Name of field.
    Kwargs:
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Delete field %s on %s.",
            field_name, dataset_path)
    arcpy.management.DeleteField(in_table=dataset_path, drop_field=field_name)
    LOG.log(log_level, "End: Delete.")
    return field_name


def duplicate_field(dataset_path, field_name, new_field_name, **kwargs):
    """Create new field as a duplicate of another.

    Args:
        dataset_path (str): Path of dataset.
        field_name (str): Name of field.
        new_field_name (str): Field name to call duplicate.
    Kwargs:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('dataset_where_sql', None), ('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Duplicate field %s as %s on %s.",
            field_name, new_field_name, dataset_path)
    field_meta = field_metadata(dataset_path, field_name)
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
        dataset_path (str): Path of dataset.
    Kwargs:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
    Returns:
        int.
    """
    kwargs.setdefault('dataset_where_sql', None)
    with arcpy.da.SearchCursor(
        in_table=dataset_path, field_names=['oid@'],
        where_clause=kwargs['dataset_where_sql']
        ) as cursor:
        count = len([None for _ in cursor])
    return count


def field_metadata(dataset_path, field_name):
    """Return dictionary of field metadata.

    Field name is case-insensitive.

    Args:
        dataset_path (str): Path of dataset.
        field_name (str): Name of field.
    Returns:
        dict.
    """
    try:
        meta = arcobj.field_as_metadata(
            arcpy.ListFields(dataset=dataset_path, wild_card=field_name)[0]
            )
    except IndexError:
        raise AttributeError(
            "Field {} not present on {}".format(field_name, dataset_path)
            )
    return meta


def is_valid(dataset_path):
    """Check whether dataset exists/is valid.

    Args:
        dataset_path (str): Path of dataset.
    Returns:
        bool.
    """
    return (dataset_path is not None and arcpy.Exists(dataset_path)
            and metadata(dataset_path)['is_table'])


def join_field(dataset_path, join_dataset_path, join_field_name,
               on_field_name, on_join_field_name, **kwargs):
    """Add field and its values from join-dataset.

    Args:
        dataset_path (str): Path of dataset.
        join_dataset_path (str): Path of dataset to join field from.
        join_field_name (str): Name of field to join.
        on_field_name (str): Name of field to join the dataset on.
        on_join_field_name (str): Name of field to join the join-dataset on.
    Kwargs:
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Join field %s on %s from %s.",
            join_field_name, dataset_path, join_dataset_path)
    arcpy.management.JoinField(
        in_data=dataset_path, in_field=on_field_name,
        join_table=join_dataset_path, join_field=on_join_field_name,
        fields=join_field_name
        )
    LOG.log(log_level, "End: Join.")
    return join_field_name


def metadata(dataset_path):
    """Return dictionary of dataset metadata.

    Args:
        dataset_path (str): Path of dataset.
    Returns:
        dict.
    """
    return arcobj.dataset_as_metadata(arcpy.Describe(dataset_path))


def rename_field(dataset_path, field_name, new_field_name, **kwargs):
    """Rename field.

    Args:
        dataset_path (str): Path of dataset.
        field_name (str): Name of field.
        new_field_name (str): Field name to change to.
    Kwargs:
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
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
        dataset_path (str): Path of dataset.
        allow_view (bool): Flag to allow or revoke view privileges.
        allow_edit (bool): Flag to allow or revoke edit privileges.
    Kwargs:
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
    privilege_map = {True: 'grant', False: 'revoke', None: 'as_is'}
    view_arg, edit_arg = privilege_map[allow_view], privilege_map[allow_edit]
    LOG.log(log_level,
            "Start: Set privileges on dataset %s for %s to view=%s, edit=%s.",
            dataset_path, user_name, view_arg, edit_arg)
    arcpy.management.ChangePrivileges(in_dataset=dataset_path, user=user_name,
                                      View=view_arg, Edit=edit_arg)
    LOG.log(log_level, "End: Set.")
    return dataset_path
