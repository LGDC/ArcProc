"""Feature operations."""
import inspect
import logging

import arcpy

from arcetl import arcobj
from arcetl import dataset
from arcetl import helpers


LOG = logging.getLogger(__name__)


def clip(dataset_path, clip_dataset_path, **kwargs):
    """Clip feature geometry where it overlaps clip-dataset geometry.

    Args:
        dataset_path (str): Path of the dataset.
        clip_dataset_path (str): Path of dataset whose features define the
            clip area.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        clip_where_sql (str): SQL where-clause for clip dataset subselection.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        tolerance (float): Tolerance for coincidence, in dataset's units.
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        str: Path of the dataset updated.
    """
    log_level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(log_level, "Start: Clip features in %s where overlapping %s.",
            dataset_path, clip_dataset_path)
    dataset_view = arcobj.DatasetView(dataset_path,
                                      kwargs.get('dataset_where_sql'))
    clip_view = arcobj.DatasetView(clip_dataset_path,
                                   kwargs.get('clip_where_sql'))
    with dataset_view, clip_view:
        temp_output_path = helpers.unique_dataset_path('output')
        arcpy.analysis.Clip(in_features=dataset_view.name,
                            clip_features=clip_view.name,
                            out_feature_class=temp_output_path,
                            cluster_tolerance=kwargs.get('tolerance'))
        delete(dataset_view.name, log_level=None)
    insert_from_path(dataset_path, temp_output_path, log_level=None)
    dataset.delete(temp_output_path, log_level=None)
    LOG.log(log_level, "End: Clip.")
    return dataset_path


count = dataset.feature_count  # pylint: disable=invalid-name


def delete(dataset_path, **kwargs):
    """Delete features in the dataset.

    Args:
        dataset_path (str): Path of the dataset.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        use_edit_session (bool): Flag to perform updates in an edit session.
            Default is False.
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        str: Path of the dataset updated.
    """
    log_level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(log_level, "Start: Delete features from %s.", dataset_path)
    truncate_error_codes = (
        # "Only supports Geodatabase tables and feature classes."
        'ERROR 000187',
        # "Operation not supported on a versioned table."
        'ERROR 001259',
        # "Operation not supported on table {table name}."
        'ERROR 001260',
        # Operation not supported on a feature class in a controller dataset.
        'ERROR 001395',
        )
    # Can use (faster) truncate when no sub-selection or edit session.
    run_truncate = (kwargs.get('dataset_where_sql') is None
                    and kwargs.get('use_edit_session', False) is False)
    if run_truncate:
        try:
            arcpy.management.TruncateTable(in_table=dataset_path)
        except arcpy.ExecuteError:
            # Avoid arcpy.GetReturnCode(); error code position inconsistent.
            # Search messages for 'ERROR ######' instead.
            if any(code in arcpy.GetMessages()
                   for code in truncate_error_codes):
                LOG.debug("Truncate unsupported; will try deleting rows.")
                run_truncate = False
            else:
                raise
    if not run_truncate:
        session = arcobj.Editor(
            arcobj.dataset_metadata(dataset_path)['workspace_path'],
            kwargs.get('use_edit_session', False),
            )
        dataset_view = arcobj.DatasetView(dataset_path,
                                          kwargs.get('dataset_where_sql'))
        with session, dataset_view:
            arcpy.management.DeleteRows(in_rows=dataset_view.name)
    LOG.log(log_level, "End: Delete.")
    return dataset_path


def dissolve(dataset_path, dissolve_field_names=None, multipart=True,
             unsplit_lines=False, **kwargs):
    """Dissolve geometry of features that share values in given fields.

    Args:
        dataset_path (str): Path of the dataset.
        dissolve_field_names (iter): Iterable of field names to dissolve on.
        multipart (bool): Flag to allow multipart features in output.
        unsplit_lines (bool): Flag to merge line features when endpoints meet
            without crossing features.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        tolerance (float): Tolerance for coincidence, in dataset's units.
        use_edit_session (bool): Flag to perform updates in an edit session.
            Default is False.
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        str: Path of the dataset updated.
    """
    dissolve_field_names = (tuple(dissolve_field_names)
                            if dissolve_field_names else None)
    log_level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(log_level, "Start: Dissolve features in %s on fields: %s.",
            dataset_path, dissolve_field_names)
    if kwargs.get('tolerance') is not None:
        old_tolerance = arcpy.env.XYTolerance
        arcpy.env.XYTolerance = kwargs['tolerance']
    dataset_view = arcobj.DatasetView(dataset_path,
                                      kwargs.get('dataset_where_sql'))
    temp_output_path = helpers.unique_dataset_path('output')
    with dataset_view:
        arcpy.management.Dissolve(
            in_features=dataset_view.name, out_feature_class=temp_output_path,
            dissolve_field=dissolve_field_names, multi_part=multipart,
            unsplit_lines=unsplit_lines
            )
    if kwargs.get('tolerance') is not None:
        arcpy.env.XYTolerance = old_tolerance
    session = arcobj.Editor(
        arcobj.dataset_metadata(dataset_path)['workspace_path'],
        kwargs.get('use_edit_session', False),
        )
    with session:
        delete(dataset_path,
               dataset_where_sql=kwargs.get('dataset_where_sql'),
               log_level=None)
        insert_from_path(dataset_path, insert_dataset_path=temp_output_path,
                         log_level=None)
    dataset.delete(temp_output_path, log_level=None)
    LOG.log(log_level, "End: Dissolve.")
    return dataset_path


def eliminate_interior_rings(dataset_path, max_area=None,
                             max_percent_total_area=None, **kwargs):
    """Eliminate interior rings of polygon features.

    Note:
        If no value if provided for either max_area or max_percent_total_area,
        (nearly) all interior rings will be removed. Technically,
        max_percent_total_area will be set to 99.9999.

    Args:
        dataset_path (str): Path of the dataset.
        max_area (float, str): Maximum area which parts smaller than are
            eliminated. Numeric area will be in dataset's units. String area
            will be formatted as '{number} {unit}'.
        max_percent_total_area (float): Maximum percent of total area which
            parts smaller than are eliminated.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        use_edit_session (bool): Flag to perform updates in an edit session.
            Default is False.
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        str: Path of the dataset updated.
    """
    log_level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(log_level, "Start: Eliminate interior rings in %s.", dataset_path)
    # Only set max_percent_total_area default if neither it or area defined.
    if all((max_area is None, max_percent_total_area is None)):
        max_percent_total_area = 99.9999
    if all((max_area is not None, max_percent_total_area is not None)):
        condition = 'area_or_percent'
    elif max_area is not None:
        condition = 'area'
    else:
        condition = 'percent'
    dataset_view = arcobj.DatasetView(dataset_path,
                                      kwargs.get('dataset_where_sql'))
    temp_output_path = helpers.unique_dataset_path('output')
    with dataset_view:
        arcpy.management.EliminatePolygonPart(
            in_features=dataset_view.name, out_feature_class=temp_output_path,
            condition=condition, part_area=max_area,
            part_area_percent=max_percent_total_area,
            part_option='contained_only'
            )
    session = arcobj.Editor(
        arcobj.dataset_metadata(dataset_path)['workspace_path'],
        kwargs.get('use_edit_session', False),
        )
    with session:
        delete(dataset_path,
               dataset_where_sql=kwargs.get('dataset_where_sql'),
               log_level=None)
        insert_from_path(dataset_path, insert_dataset_path=temp_output_path,
                         log_level=None)
    dataset.delete(temp_output_path, log_level=None)
    LOG.log(log_level, "End: Eliminate.")
    return dataset_path


def erase(dataset_path, erase_dataset_path, **kwargs):
    """Erase feature geometry where it overlaps erase-dataset geometry.

    Args:
        dataset_path (str): Path of the dataset.
        erase_dataset_path (str): Path of the dataset defining the erase-
            area.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        erase_where_sql (str): SQL where-clause for erase-dataset
            subselection.
        tolerance (float): Tolerance for coincidence, in dataset's units.
        use_edit_session (bool): Flag to perform updates in an edit session.
            Default is False.
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        str: Path of the dataset updated.
    """
    log_level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(log_level, "Start: Erase features in %s where overlapping %s.",
            dataset_path, erase_dataset_path)
    dataset_view = arcobj.DatasetView(dataset_path,
                                      kwargs.get('dataset_where_sql'))
    erase_view = arcobj.DatasetView(erase_dataset_path,
                                    kwargs.get('erase_where_sql'))
    temp_output_path = helpers.unique_dataset_path('output')
    with dataset_view, erase_view:
        arcpy.analysis.Erase(in_features=dataset_view.name,
                             erase_features=erase_view.name,
                             out_feature_class=temp_output_path,
                             cluster_tolerance=kwargs.get('tolerance'))
    session = arcobj.Editor(
        arcobj.dataset_metadata(dataset_path)['workspace_path'],
        kwargs.get('use_edit_session', False),
        )
    with session:
        delete(dataset_path,
               dataset_where_sql=kwargs.get('dataset_where_sql'),
               log_level=None)
        insert_from_path(dataset_path, insert_dataset_path=temp_output_path,
                         log_level=None)
    dataset.delete(temp_output_path, log_level=None)
    LOG.log(log_level, "End: Erase.")
    return dataset_path


def insert_from_dicts(dataset_path, insert_features, field_names, **kwargs):
    """Insert features into dataset from dictionaries.

    Args:
        dataset_path (str): Path of the dataset.
        insert_features (iter of dict): Collection of dictionaries
            representing features.
        field_names (iter): Collection of field names/keys to insert.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        use_edit_session (bool): Flag to perform updates in an edit session.
            Default is False.
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        str: Path of the dataset updated.
    """
    log_level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(log_level, "Start: Insert features into %s from dictionaries.",
            dataset_path)
    field_names = tuple(field_names)  # Ensures not generator (names used 2x).
    if inspect.isgeneratorfunction(insert_features):
        insert_features = insert_features()
    iters = (
        (feature[name] if name in feature else None for name in field_names)
        for feature in insert_features
        )
    result = insert_from_iters(
        dataset_path, iters, field_names,
        use_edit_session=kwargs.get('use_edit_session', False),
        log_level=None,
        )
    LOG.log(log_level, "End: Insert.")
    return result


def insert_from_iters(dataset_path, insert_features, field_names, **kwargs):
    """Insert features into dataset from iterables.

    Args:
        dataset_path (str): Path of the dataset.
        insert_features (iter of iter): Collection of iterables representing
            features.
        field_names (iter): Collection of field names to insert. These must
            match the order of their attributes in the insert_features items.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        use_edit_session (bool): Flag to perform updates in an edit session.
            Default is False.
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        str: Path of the dataset updated.
    """
    log_level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(log_level, "Start: Insert features into %s from iterables.",
            dataset_path)
    if inspect.isgeneratorfunction(insert_features):
        insert_features = insert_features()
    session = arcobj.Editor(
        arcobj.dataset_metadata(dataset_path)['workspace_path'],
        kwargs.get('use_edit_session', False),
        )
    cursor = arcpy.da.InsertCursor(dataset_path, field_names)
    with session, cursor:
        for row in insert_features:
            cursor.insertRow(tuple(row))
    LOG.log(log_level, "End: Insert.")
    return dataset_path


def insert_from_path(dataset_path, insert_dataset_path, field_names=None,
                     **kwargs):
    """Insert features into dataset from another dataset.

    Args:
        dataset_path (str): Path of the dataset.
        insert_dataset_path (str): Path of dataset to insert features from.
        field_names (iter): Collection of field names to insert. Listed field
            must be present in both datasets. If field_names is None, all
            fields will be inserted.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        insert_where_sql (str): SQL where-clause for insert-dataset
            subselection.
        use_edit_session (bool): Flag to perform updates in an edit session.
            Default is False.
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        str: Path of the dataset updated.
    """
    log_level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(log_level, "Start: Insert features into %s from %s.",
            dataset_path, insert_dataset_path)
    meta = {'dataset': arcobj.dataset_metadata(dataset_path),
            'insert': arcobj.dataset_metadata(insert_dataset_path)}
    # Create field maps.
    # Added because ArcGIS Pro's no-test append is case-sensitive (verified
    # 1.0-1.1.1). BUG-000090970 - ArcGIS Pro 'No test' field mapping in
    # Append tool does not auto-map to the same field name if naming
    # convention differs.
    _field_names = {
        'insert': {name.lower() for name in meta['insert']['field_names']}
        }
    if field_names is None:
        _field_names['dataset'] = [name.lower() for name
                                   in meta['dataset']['field_names']]
    else:
        _field_names['dataset'] = [name.lower() for name in field_names]
    # Append takes care of geometry & OIDs independent of the field maps.
    for field_name_type in ('geometry_field_name', 'oid_field_name'):
        for _set in ('dataset', 'insert'):
            if meta[_set].get(field_name_type) is None:
                continue
            field_name = meta[_set][field_name_type].lower()
            if field_name and field_name in _field_names[_set]:
                _field_names[_set].remove(field_name)
    field_maps = arcpy.FieldMappings()
    for field_name in _field_names['dataset']:
        if field_name in _field_names['insert']:
            field_map = arcpy.FieldMap()
            field_map.addInputField(insert_dataset_path, field_name)
            field_maps.addFieldMap(field_map)
    session = arcobj.Editor(meta['dataset']['workspace_path'],
                            kwargs.get('use_edit_session', False))
    insert_view = arcobj.DatasetView(
        insert_dataset_path, kwargs.get('insert_where_sql'),
        # Insert view must be nonspatial to append to nonspatial table.
        force_nonspatial=(not meta['dataset']['is_spatial'])
        )
    with session, insert_view:
        arcpy.management.Append(inputs=insert_view.name,
                                target=dataset_path, schema_type='no_test',
                                field_mapping=field_maps)
    LOG.log(log_level, "End: Insert.")
    return dataset_path


def keep_by_location(dataset_path, location_dataset_path, **kwargs):
    """Keep features where geometry overlaps location-dataset geometry.

    Args:
        dataset_path (str): Path of the dataset.
        location_dataset_path (str): Path of location-dataset.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        location_where_sql (str): SQL where-clause for location-dataset
            subselection.
        use_edit_session (bool): Flag to perform updates in an edit session.
            Default is False.
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        str: Path of the dataset updated.
    """
    log_level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(log_level, "Start: Keep features in %s where overlapping %s.",
            dataset_path, location_dataset_path)
    session = arcobj.Editor(
        arcobj.dataset_metadata(dataset_path)['workspace_path'],
        kwargs.get('use_edit_session', False),
        )
    dataset_view = arcobj.DatasetView(dataset_path,
                                      kwargs.get('dataset_where_sql'))
    location_view = arcobj.DatasetView(location_dataset_path,
                                       kwargs.get('location_where_sql'))
    with session, dataset_view, location_view:
        arcpy.management.SelectLayerByLocation(
            in_layer=dataset_view.name, overlap_type='intersect',
            select_features=location_view.name, selection_type='new_selection'
            )
        arcpy.management.SelectLayerByLocation(
            in_layer=dataset_view.name, selection_type='switch_selection'
            )
        delete(dataset_view.name, log_level=None)
    LOG.log(log_level, "End: Keep.")
    return dataset_path
