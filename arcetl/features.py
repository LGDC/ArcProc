"""Feature operations."""
from collections import Counter
import datetime
import inspect
import logging

import arcpy

from arcetl import arcobj
from arcetl import attributes
from arcetl import dataset
from arcetl import helpers


LOG = logging.getLogger(__name__)


def _is_same(*rows):
    """Determine whether feature representations are the same.

    Args:
        *rows (iter of iter): Collection of features to compare.

    Returns:
        bool: True if rows are the same, False otherwise.

    """
    for i, row in enumerate(rows):
        if i == 0:
            cmp_bools = [True for val in row]
            continue
        cmp_row = rows[i-1]
        for num, (val, cmp_val) in enumerate(zip(row, cmp_row)):
            if all(isinstance(v, datetime.datetime) for v in (val, cmp_val)):
                _bool = (val - cmp_val).total_seconds() < 1
            elif all(isinstance(v, arcpy.Geometry) for v in (val, cmp_val)):
                _bool = val.equals(cmp_val)
            else:
                _bool = val == cmp_val
            cmp_bools[num] = cmp_bools[num] and _bool
    return all(cmp_bools)


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


def delete_by_id(dataset_path, delete_ids, id_field_names, **kwargs):
    """Delete features in dataset with given IDs.

    Note:
        There is no guarantee that the ID field(s) are unique.
        Use ArcPy cursor token names for object IDs and geometry objects/
        properties.

    Args:
        dataset_path (str): Path of the dataset.
        delete_ids (iter): Collection of feature IDs.
        id_field_names (iter, str): Name(s) of the ID field/key(s).
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        use_edit_session (bool): Flag to perform updates in an edit session.
            Default is True.
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        str: Path of the dataset updated.

    """
    _level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(_level, "Start: Delete features in %s with given IDs.",
            dataset_path)
    keys = {'id': tuple(helpers.contain(id_field_names))}
    if inspect.isgeneratorfunction(delete_ids):
        delete_ids = delete_ids()
    ids = {'delete': {tuple(helpers.contain(_id)) for _id in delete_ids}}
    feature_count = Counter()
    session = arcobj.Editor(
        arcobj.dataset_metadata(dataset_path)['workspace_path'],
        kwargs.get('use_edit_session', True),
        )
    if ids['delete']:
        cursor = arcpy.da.UpdateCursor(dataset_path, field_names=keys['id'])
        with session, cursor:
            for row in cursor:
                _id = tuple(row)
                if _id in ids['delete']:
                    cursor.deleteRow()
                    feature_count['deleted'] += 1
                else:
                    feature_count['unchanged'] += 1
    for key in ('deleted', 'unchanged'):
        LOG.info("%s features %s.", feature_count[key], key)
    LOG.log(_level, "End: Delete.")
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
    _level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(_level, "Start: Insert features into %s from dictionaries.",
            dataset_path)
    keys = {'row': tuple(helpers.contain(field_names))}
    if inspect.isgeneratorfunction(insert_features):
        insert_features = insert_features()
    iters = ((feature[key] for key in keys['row'])
             for feature in insert_features)
    result = insert_from_iters(
        dataset_path, iters, field_names,
        use_edit_session=kwargs.get('use_edit_session', False),
        log_level=None,
        )
    LOG.log(_level, "End: Insert.")
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
    _level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(_level, "Start: Insert features into %s from iterables.",
            dataset_path)
    keys = {'row': tuple(helpers.contain(field_names))}
    if inspect.isgeneratorfunction(insert_features):
        insert_features = insert_features()
    session = arcobj.Editor(
        arcobj.dataset_metadata(dataset_path)['workspace_path'],
        kwargs.get('use_edit_session', False),
        )
    cursor = arcpy.da.InsertCursor(dataset_path, field_names=keys['row'])
    with session, cursor:
        for row in insert_features:
            cursor.insertRow(tuple(row))
    LOG.log(_level, "End: Insert.")
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
    _level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(_level, "Start: Insert features into %s from %s.",
            dataset_path, insert_dataset_path)
    if field_names is None:
        meta = {'dataset': arcobj.dataset_metadata(dataset_path),
                'insert': arcobj.dataset_metadata(insert_dataset_path)}
        field_names = (
            set(n.lower() for n in meta['dataset']['field_names'])
            & set(n.lower() for n in meta['insert']['field_names'])
            )
        for _meta in meta.values():
            for name, token in (
                    (_meta['oid_field_name'].lower(), 'oid@'),
                    (_meta['geom_field_name'].lower(), 'shape@'),
                ):
                if name in field_names:
                    field_names.remove(name)
                    field_names.add(token)
    keys = {'row': tuple(helpers.contain(field_names))}
    iters = attributes.as_iters(
        insert_dataset_path, keys['row'],
        dataset_where_sql=kwargs.get('insert_where_sql'),
        )
    result = insert_from_iters(
        dataset_path, insert_features=iters, field_names=keys['row'],
        use_edit_session=kwargs.get('use_edit_session', True),
        log_level=None,
        )
    LOG.log(_level, "End: Insert.")
    return result


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


def update_from_dicts(dataset_path, update_features, id_field_names,
                      field_names, **kwargs):
    """Update features in dataset from dictionaries.

    Note:
        There is no guarantee that the ID field(s) are unique.
        Use ArcPy cursor token names for object IDs and geometry objects/
        properties.

    Args:
        dataset_path (str): Path of the dataset.
        update_features (iter of dict): Collection of dictionaries
            representing features.
        id_field_names (iter, str): Name(s) of the ID field/key(s).
        field_names (iter): Collection of field names/keys to check & update.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        delete_missing_features (bool): True if update should delete features
            missing from update_features, False otherwise. Default is False.
        use_edit_session (bool): Flag to perform updates in an edit session.
            Default is True.
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        str: Path of the dataset updated.

    """
    _level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(_level, "Start: Update features in %s from dictionaries.",
            dataset_path)
    keys = {'id': tuple(helpers.contain(id_field_names)),
            'attr': tuple(helpers.contain(field_names))}
    keys['row'] = keys['id'] + keys['attr']
    if inspect.isgeneratorfunction(update_features):
        update_features = update_features()
    iters = ((feature[key] for key in keys['row'])
             for feature in update_features)
    result = update_from_iters(
        dataset_path, update_features=iters,
        id_field_names=keys['id'], field_names=keys['row'],
        delete_missing_features=kwargs.get('delete_missing_features', False),
        use_edit_session=kwargs.get('use_edit_session', True),
        log_level=None,
        )
    LOG.log(_level, "End: Update.")
    return result


def update_from_iters(dataset_path, update_features, id_field_names,
                      field_names, **kwargs):
    """Update features in dataset from iterables.

    Note:
        There is no guarantee that the ID field(s) are unique.
        Use ArcPy cursor token names for object IDs and geometry objects/
        properties.

    Args:
        dataset_path (str): Path of the dataset.
        update_features (iter of dict): Collection of iterables representing
            features.
        id_field_names (iter, str): Name(s) of the ID field/key(s). All ID
            fields must also be in field_names.
        field_names (iter): Collection of field names/keys to check & update.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        delete_missing_features (bool): True if update should delete features
            missing from update_features, False otherwise. Default is False.
        use_edit_session (bool): Flag to perform updates in an edit session.
            Default is True.
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        str: Path of the dataset updated.

    """
    _level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(_level, "Start: Update features in %s from iterables.",
            dataset_path)
    keys = {'id': tuple(helpers.contain(id_field_names)),
            'row': tuple(helpers.contain(field_names))}
    if not set(keys['id']).issubset(keys['row']):
        raise ValueError("id_field_names must be a subset of field_names.")
    ids = {'dataset': set(attributes.as_iters(dataset_path, keys['id'])),
           'delete': set()}
    if inspect.isgeneratorfunction(update_features):
        update_features = update_features()
    insert_rows = set()
    id_update_row = {}
    for row in update_features:
        row = tuple(row)
        _id = tuple(row[keys['row'].index(key)] for key in keys['id'])
        if _id not in ids['dataset']:
            insert_rows.add(row)
        else:
            id_update_row[_id] = row
    if kwargs.get('delete_missing_features', False):
        ids['delete'] = {_id for _id in ids['dataset']
                         if _id not in id_update_row}
    session = arcobj.Editor(
        arcobj.dataset_metadata(dataset_path)['workspace_path'],
        kwargs.get('use_edit_session', True),
        )
    cursor = arcpy.da.UpdateCursor(dataset_path, field_names=keys['row'])
    with session, cursor:
        for row in cursor:
            _id = tuple(row[keys['row'].index(key)] for key in keys['id'])
            if _id in ids['delete']:
                cursor.deleteRow()
            # Don't pop this. Otherwise if ID isn't unique, will get only
            # one row with ID updated.
            update_row = id_update_row.get(_id, row)
            if row != update_row:
                cursor.updateRow(tuple(update_row))
    cursor = arcpy.da.InsertCursor(dataset_path, field_names=keys['row'])
    with session, cursor:
        for row in insert_rows:
            cursor.insertRow(row)
    LOG.log(_level, "End: Update.")
    return dataset_path


def update_from_path(dataset_path, update_dataset_path, id_field_names,
                     field_names=None, **kwargs):
    """Update features in dataset from another dataset.

    Args:
        dataset_path (str): Path of the dataset.
        update_dataset_path (str): Path of dataset to update features from.
        id_field_names (iter, str): Name(s) of the ID field/key(s).
        field_names (iter): Collection of field names/keys to check & update.
            Listed field must be present in both datasets. If field_names is
            None, all fields will be inserted.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        update_where_sql (str): SQL where-clause for update-dataset
            subselection.
        delete_missing_features (bool): True if update should delete features
            missing from update_features, False otherwise. Default is False.
        use_edit_session (bool): Flag to perform updates in an edit session.
            Default is True.
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        str: Path of the dataset updated.

    """
    _level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(_level, "Start: Update features in %s from %s.",
            dataset_path, update_dataset_path)
    if field_names is None:
        meta = {'dataset': arcobj.dataset_metadata(dataset_path),
                'update': arcobj.dataset_metadata(update_dataset_path)}
        field_names = (
            set(n.lower() for n in meta['dataset']['field_names'])
            & set(n.lower() for n in meta['update']['field_names'])
            )
        for _meta in meta.values():
            for name, token in (
                    (_meta['oid_field_name'].lower(), 'oid@'),
                    (_meta['geom_field_name'].lower(), 'shape@'),
                ):
                if name in field_names:
                    field_names.remove(name)
                    field_names.add(token)
    keys = {'id': tuple(helpers.contain(id_field_names)),
            'attr': tuple(helpers.contain(field_names))}
    keys['row'] = keys['id'] + keys['attr']
    iters = attributes.as_iters(
        update_dataset_path, keys['row'],
        dataset_where_sql=kwargs.get('update_where_sql'),
        )
    result = update_from_iters(
        dataset_path, update_features=iters,
        id_field_names=keys['id'], field_names=keys['row'],
        delete_missing_features=kwargs.get('delete_missing_features', False),
        use_edit_session=kwargs.get('use_edit_session', True),
        log_level=None,
        )
    LOG.log(_level, "End: Update.")
    return result
