# -*- coding=utf-8 -*-
"""Processing operation objects."""
import collections
import csv
import logging
import os
import tempfile

import arcpy

from . import arcwrap, helpers


LOG = logging.getLogger(__name__)


# Products.

@helpers.log_function
def sort_features(dataset_path, output_path, sort_field_names, **kwargs):
    """Sort features into a new dataset.

    reversed_field_names are fields in sort_field_names that should have
    their sort order reversed.

    Args:
        dataset_path (str): Path of dataset.
        output_path (str): Path of output dataset.
        sort_field_names (iter): Iterable of field names to sort on, in order.
    Kwargs:
        reversed_field_names (iter): Iterable of field names (present in
            sort_field_names) to sort values in reverse-order.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('dataset_where_sql', None), ('log_level', 'info'),
                          ('reversed_field_names', [])]:
        kwargs.setdefault(*kwarg_default)
    _description = "Sort features in {} to {}.".format(
        dataset_path, output_path)
    helpers.log_line('start', _description, kwargs['log_level'])
    dataset_view_name = arcwrap.create_dataset_view(
        helpers.unique_name('dataset_view'), dataset_path,
        dataset_where_sql=kwargs['dataset_where_sql'])
    try:
        arcpy.management.Sort(
            in_dataset=dataset_view_name, out_dataset=output_path,
            sort_field=[
                (name, 'descending') if name in  kwargs['reversed_field_names']
                else (name, 'ascending') for name in sort_field_names],
            spatial_sort_method='UR')
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    delete_dataset(dataset_view_name, log_level=None)
    helpers.log_line('end', _description, kwargs['log_level'])
    return output_path


@helpers.log_function
def write_rows_to_csvfile(rows, output_path, field_names, **kwargs):
    """Write collected of rows to a CSV-file.

    The rows can be represented by either a dictionary or iterable.
    Args:
        rows (iter): Iterable of obejcts representing rows (iterables or
            dictionaries).
        output_path (str): Path of output dataset.
        field_names (iter): Iterable of field names, in the desired order.
    Kwargs:
        header (bool): Flag indicating whether to write a header to the output.
        file_mode (str): Code indicating the file mode for writing.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('file_mode', 'wb'), ('header', False),
                          ('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    _description = "Write rows iterable to CSV-file {}".format(output_path)
    helpers.log_line('start', _description, kwargs['log_level'])
    with open(output_path, kwargs['file_mode']) as csvfile:
        for index, row in enumerate(rows):
            if index == 0:
                if isinstance(row, dict):
                    writer = csv.DictWriter(csvfile, field_names)
                    if kwargs['header']:
                        writer.writeheader()
                elif isinstance(row, collections.Sequence):
                    writer = csv.writer(csvfile)
                    if kwargs['header']:
                        writer.writerow(field_names)
                else:
                    raise TypeError(
                        "Row objects must be dictionaries or sequences.")
            writer.writerow(row)
    helpers.log_line('end', _description, kwargs['log_level'])
    return output_path


@helpers.log_function
def xref_near_features(dataset_path, dataset_id_field_name,
                       xref_path, xref_id_field_name, **kwargs):
    """Generator for cross-referenced near feature-pairs.

    Yielded objects will include at least the dataset ID & XRef ID.
    Setting max_near_distance to NoneType will generate every possible
    feature cross-reference.
    Setting only_closest to True will generate a cross reference only with
    the closest feature.
    Setting any include_* to True will include that value in the generated
    tuple, in argument order.
    Distance values will match the linear unit of the main dataset.
    Angle values are in decimal degrees.

    Args:
        dataset_path (str): Path of dataset.
        dataset_id_field_name (str): Name of ID field.
        xref_path (str): Path of xref-dataset.
        xref_id_field_name (str): Name of xref ID field.
    Kwargs:
        max_near_distance (float): Maximum distance to search for near-
            features, in units of the dataset's spatial reference.
        only_closest (bool): Flag indicating only closest feature  will be
            cross-referenced.
        include_distance (bool): Flag to include distance in output.
        include_rank (bool): Flag to include rank in output.
        include_angle (bool): Flag to include angle in output.
        include_coordinates (bool): Flag to include coordinates in output.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
    Yields:
        tuple.
    """
    for kwarg_default in [
            ('dataset_where_sql', None), ('include_angle', False),
            ('include_coordinates', False), ('include_distance', False),
            ('include_rank', False), ('max_near_distance', None),
            ('only_closest', False)]:
        kwargs.setdefault(*kwarg_default)
    dataset_view_name = arcwrap.create_dataset_view(
        helpers.unique_name('dataset_view'), dataset_path,
        dataset_where_sql=kwargs['dataset_where_sql'], log_level=None)
    temp_near_path = helpers.unique_temp_dataset_path('temp_near')
    try:
        arcpy.analysis.GenerateNearTable(
            in_features=dataset_view_name, near_features=xref_path,
            out_table=temp_near_path,
            search_radius=kwargs['max_near_distance'],
            location=kwargs['include_coordinates'],
            angle=kwargs['include_angle'], closest=kwargs['only_closest'],
            method='geodesic')
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    near_field_names = ['in_fid', 'near_fid']
    for flag, field_name in [
            (kwargs['include_distance'], 'near_dist'),
            (kwargs['include_rank'], 'near_rank'),
            (kwargs['include_angle'], 'near_angle'),
            (kwargs['include_coordinates'], 'near_x'),
            (kwargs['include_coordinates'], 'near_y')]:
        if flag:
            near_field_names.append(field_name)
    dataset_oid_id_map = properties.oid_field_value_map(
        dataset_view_name, dataset_id_field_name)
    xref_oid_id_map = properties.oid_field_value_map(
        xref_path, xref_id_field_name)
    #pylint: disable=no-member
    with arcpy.da.SearchCursor(
        in_table=temp_near_path, field_names=near_field_names) as cursor:
    #pylint: enable=no-member
        for row in cursor:
            row_info = dict(zip(cursor.fields, row))
            result = [dataset_oid_id_map[row_info['in_fid']],
                      xref_oid_id_map[row_info['near_fid']]]
            if kwargs['include_distance']:
                result.append(row_info['near_dist'])
            if kwargs['include_rank']:
                result.append(row_info['near_rank'])
            if kwargs['include_angle']:
                result.append(row_info['near_angle'])
            if kwargs['include_coordinates']:
                result.append(row_info['near_x'])
                result.append(row_info['near_y'])
            yield tuple(result)
    delete_dataset(dataset_view_name)
    delete_dataset(temp_near_path)


# Workspace.

@helpers.log_function
def build_network(network_path, **kwargs):
    """Build network (dataset or geometric).

    Args:
        network_path (str): Path of network.
    Kwargs:
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    kwargs.setdefault('log_level', 'info')
    _description = "Build network {}.".format(network_path)
    helpers.log_line('start', _description, kwargs['log_level'])
    #pylint: disable=no-member
    network_type = arcpy.Describe(network_path).dataType
    #pylint: enable=no-member
    if network_type == 'GeometricNetwork':
        #pylint: disable=no-member
        _build = arcpy.management.RebuildGeometricNetwork
        #pylint: enable=no-member
        _build_kwargs = {'geometric_network': network_path,
                         # Geometric network build requires logfile output.
                         'out_log': os.path.join(tempfile.gettempdir(), 'log')}
    elif network_type == 'NetworkDataset':
        helpers.toggle_arc_extension('Network', toggle_on=True)
        _build = arcpy.na.BuildNetwork
        _build_kwargs = {'in_network_dataset': network_path}
    else:
        raise ValueError("{} not a valid network type.".format(network_path))
    try:
        _build(**_build_kwargs)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    helpers.toggle_arc_extension('Network', toggle_off=True)
    if 'out_log' in _build_kwargs:
        os.remove(_build_kwargs['out_log'])
    helpers.log_line('end', _description, kwargs['log_level'])
    return network_path


@helpers.log_function
def compress_geodatabase(geodatabase_path, **kwargs):
    """Compress geodatabase.

    Args:
        geodatabase_path (str): Path of geodatabase.
    Kwargs:
        disconnect_users (bool): Flag to disconnect users before compressing.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    kwargs.setdefault('disconnect_users', False)
    kwargs.setdefault('log_level', 'info')
    _description = "Compress {}.".format(geodatabase_path)
    helpers.log_line('start', _description, kwargs['log_level'])
    try:
        workspace_type = arcpy.Describe(geodatabase_path).workspaceType
    except AttributeError:
        raise ValueError(
            "{} not a valid geodatabase.".format(geodatabase_path))
    if workspace_type == 'LocalDatabase':
        _compress = arcpy.management.CompressFileGeodatabaseData
        _compress_kwargs = {'in_data': geodatabase_path}
        # Local databases cannot disconnect users (connections managed
        # by file access).
        kwargs['disconnect_users'] = False
    elif workspace_type == 'RemoteDatabase':
        _compress = arcpy.management.Compress
        _compress_kwargs = {'in_workspace': geodatabase_path}
    else:
        raise ValueError(
            "{} not a valid geodatabase type.".format(workspace_type))
    if kwargs['disconnect_users']:
        arcpy.AcceptConnections(sde_workspace=geodatabase_path,
                                accept_connections=False)
        arcpy.DisconnectUser(sde_workspace=geodatabase_path, users='all')
    try:
        _compress(**_compress_kwargs)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    if kwargs['disconnect_users']:
        arcpy.AcceptConnections(sde_workspace=geodatabase_path,
                                accept_connections=True)
    helpers.log_line('end', _description, kwargs['log_level'])
    return geodatabase_path


@helpers.log_function
def copy_dataset(dataset_path, output_path, **kwargs):
    """Copy features into a new dataset.

    Wraps arcwrap.copy_dataset.

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
            ('sort_field_names', []), ('sort_reversed_field_names', [])]:
        kwargs.setdefault(*kwarg_default)
    meta = {'description': "Copy {} to {}.".format(dataset_path, output_path)}
    helpers.log_line('start', meta['description'], kwargs['log_level'])
    result = arcwrap.copy_dataset(dataset_path, output_path, **kwargs)
    helpers.log_line('end', meta['description'], kwargs['log_level'])
    return result


@helpers.log_function
def create_dataset(dataset_path, field_metadata_list=None, **kwargs):
    """Create new dataset.

    Wraps arcwrap.create_dataset.

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
    _description = "Create dataset {}.".format(dataset_path)
    helpers.log_line('start', _description, kwargs['log_level'])
    result = arcwrap.create_dataset(dataset_path, field_metadata_list, **kwargs)
    helpers.log_line('end', _description, kwargs['log_level'])
    return result


@helpers.log_function
def create_file_geodatabase(geodatabase_path, **kwargs):
    """Create new file geodatabase.

    Args:
        geodatabase_path (str): Path of geodatabase to create.
    Kwargs:
        xml_workspace_path (str): Path of XML workspace document to define
            geodatabase with.
        include_xml_data (bool): Flag to include data from a provided XML
            workspace document, if it has any.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('include_xml_data', False), ('log_level', 'info'),
                          ('xml_workspace_path', None)]:
        kwargs.setdefault(*kwarg_default)
    _description = "Create file geodatabase at {}.".format(geodatabase_path)
    helpers.log_line('start', _description, kwargs['log_level'])
    if os.path.exists(geodatabase_path):
        LOG.warning("Geodatabase already exists.")
        return geodatabase_path
    try:
        arcpy.management.CreateFileGDB(
            out_folder_path=os.path.dirname(geodatabase_path),
            out_name=os.path.basename(geodatabase_path),
            out_version='current')
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    if kwargs['xml_workspace_path']:
        try:
            arcpy.management.ImportXMLWorkspaceDocument(
                target_geodatabase=geodatabase_path,
                in_file=kwargs['xml_workspace_path'],
                import_type=(
                    'data' if kwargs['include_xml_data'] else 'schema_only'),
                config_keyword='defaults')
        except arcpy.ExecuteError:
            LOG.exception("ArcPy execution.")
            raise
    helpers.log_line('end', _description, kwargs['log_level'])
    return geodatabase_path


@helpers.log_function
def create_geodatabase_xml_backup(geodatabase_path, output_path, **kwargs):
    """Create backup of geodatabase as XML workspace document.

    Args:
        geodatabase_path (str): Path of geodatabase.
        output_path (str): Path of output XML workspace document.
    Kwargs:
        include_data (bool): Flag to include data in backup.
        include_metadata (bool): Flag to include metadata in backup.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('include_data', False), ('include_metadata', True),
                          ('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    _description = "Create backup {} for {}.".format(
        geodatabase_path, output_path)
    helpers.log_line('start', _description, kwargs['log_level'])
    try:
        arcpy.management.ExportXMLWorkspaceDocument(
            in_data=geodatabase_path, out_file=output_path,
            export_type='data' if kwargs['include_data'] else 'schema_only',
            storage_type='binary', export_metadata=kwargs['include_metadata'])
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    helpers.log_line('end', _description, kwargs['log_level'])
    return output_path


@helpers.log_function
def delete_dataset(dataset_path, **kwargs):
    """Delete dataset.

    Wraps arcwrap.delete_dataset.

    Args:
        dataset_path (str): Path of dataset.
    Kwargs:
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    kwargs.setdefault('log_level', 'info')
    meta = {'description': "Delete {}.".format(dataset_path)}
    helpers.log_line('start', meta['description'], kwargs['log_level'])
    result = arcwrap.delete_dataset(dataset_path)
    helpers.log_line('end', meta['description'], kwargs['log_level'])
    return result


@helpers.log_function
def execute_sql_statement(statement, database_path, **kwargs):
    """Runs a SQL statement via SDE's SQL execution interface.

    Only works if path resolves to an actual SQL database.

    Args:
        dataset_path (str): Path of dataset.
    Kwargs:
        log_level (str): Level at which to log this function.
    Returns:
        str, tuple.
    """
    kwargs.setdefault('log_level', 'info')
    _description = "Execute SQL statement."
    helpers.log_line('start', _description, kwargs['log_level'])
    conn = arcpy.ArcSDESQLExecute(server=database_path)
    try:
        result = conn.execute(statement)
    except AttributeError:
        LOG.exception("Incorrect SQL syntax.")
        raise
    finally:
        del conn  # Yeah, what can you do?
    helpers.log_line('end', _description, kwargs['log_level'])
    return result


@helpers.log_function
def set_dataset_privileges(dataset_path, user_name, allow_view=None,
                           allow_edit=None, **kwargs):
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
    kwargs.setdefault('log_level', 'info')
    _description = "Set privileges for {} on {}.".format(
        user_name, dataset_path)
    helpers.log_line('start', _description, kwargs['log_level'])
    boolean_privilege_map = {True: 'grant', False: 'revoke', None: 'as_is'}
    try:
        arcpy.management.ChangePrivileges(
            in_dataset=dataset_path, user=user_name,
            View=boolean_privilege_map[allow_view],
            Edit=boolean_privilege_map[allow_edit])
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    helpers.log_line('end', _description, kwargs['log_level'])
    return dataset_path
