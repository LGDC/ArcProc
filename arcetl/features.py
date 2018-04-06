"""Feature operations."""
from collections import Counter
import datetime
import inspect
import logging

from more_itertools import pairwise

import arcpy

from arcetl import arcobj
from arcetl import attributes
from arcetl import dataset
from arcetl.helpers import (
    contain, freeze_values, leveled_logger, unique_name, unique_path,
)


LOG = logging.getLogger(__name__)

UPDATE_TYPES = ('deleted', 'inserted', 'altered', 'unchanged')


def _insert_from_path_with_append(dataset_path, insert_dataset_path,
                                  field_names, **kwargs):
    """Insert features into dataset from another dataset with append tool.

    Refer to insert_from_path for arguments.

    Returns:
        str: Path of the dataset updated.

    """
    meta = {'dataset': arcobj.dataset_metadata(dataset_path)}
    # Create field maps.
    append_kwargs = {'inputs': unique_name('view'),
                     'target': dataset_path,
                     'schema_type': 'no_test',
                     'field_mapping': arcpy.FieldMappings()}
    # ArcGIS Pro's no-test append is case-sensitive (verified 1.0-1.1.1).
    # Avoid this problem by using field mapping.
    # BUG-000090970 - ArcGIS Pro 'No test' field mapping in Append tool does
    # not auto-map to the same field name if naming convention differs.
    for name in field_names:
        # Append takes care of geometry independent of field maps.
        if name == 'shape@':
            continue
        field_map = arcpy.FieldMap()
        field_map.addInputField(insert_dataset_path, name)
        append_kwargs['field_mapping'].addFieldMap(field_map)
    view = {
        'insert': arcobj.DatasetView(
            insert_dataset_path, kwargs['insert_where_sql'],
            view_name=append_kwargs['inputs'],
            # Insert view must be nonspatial to append to nonspatial table.
            force_nonspatial=(not meta['dataset']['is_spatial'])
            ),
        }
    session = arcobj.Editor(meta['dataset']['workspace_path'],
                            kwargs['use_edit_session'])
    with view['insert'], session:
        arcpy.management.Append(**append_kwargs)
        feature_count = Counter({'inserted': view['insert'].count})
    return feature_count


def _insert_from_path_with_cursor(dataset_path, insert_dataset_path,
                                  field_names, **kwargs):
    """Insert features into dataset from another dataset with cursor.

    Refer to insert_from_path for arguments.

    Returns:
        str: Path of the dataset updated.

    """
    insert_features = attributes.as_iters(
        insert_dataset_path, field_names,
        dataset_where_sql=kwargs['insert_where_sql'],
        )
    feature_count = insert_from_iters(
        dataset_path, insert_features, field_names,
        use_edit_session=kwargs['use_edit_session'],
        log_level=None,
        )
    return feature_count


def _same_feature(*features):
    """Determine whether feature representations are the same.

    Args:
        *features (iter of iter): Collection of features to compare.

    Returns:
        bool: True if features are the same, False otherwise.

    """
    same = all(_same_value(*vals) for pair in pairwise(features) for vals in zip(*pair))
    return same


def _same_value(*values):
    """Determine whether values are the same.

    Args:
        *values (iter of iter): Collection of values to compare.

    Returns:
        bool: True if values are the same, False otherwise.

    """
    same = all(val1 == val2 for val1, val2 in pairwise(values))
    # Some types are quite as simple.
    if all(isinstance(val, datetime.datetime) for val in values):
        same = all((val1 - val2).total_seconds() < 1 for val1, val2 in pairwise(values))
    elif all(isinstance(val, arcpy.Geometry) for val in values):
        ##TODO: Test polyline.
        ##TODO: Test multipoint, multipatch, dimension, annotation.
        same = all(val1.equals(val2) for val1, val2 in pairwise(values))
    return same


def clip(dataset_path, clip_dataset_path, **kwargs):
    """Clip feature geometry where it overlaps clip-dataset geometry.

    Args:
        dataset_path (str): Path of the dataset.
        clip_dataset_path (str): Path of dataset whose features define the
            clip area.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        clip_where_sql (str): SQL where-clause for clip dataset subselection.
        tolerance (float): Tolerance for coincidence, in dataset's units.
        use_edit_session (bool): Flag to perform updates in an edit session.
            Default is False.
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        str: Path of the dataset updated.

    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('clip_where_sql')
    kwargs.setdefault('use_edit_session', False)
    kwargs.setdefault('tolerance')
    log = leveled_logger(LOG, kwargs.get('log_level', 'info'))
    log("Start: Clip features in %s where overlapping %s.",
        dataset_path, clip_dataset_path)
    meta = {'dataset': arcobj.dataset_metadata(dataset_path)}
    view = {'dataset': arcobj.DatasetView(dataset_path,
                                          kwargs['dataset_where_sql']),
            'clip': arcobj.DatasetView(clip_dataset_path,
                                       kwargs['clip_where_sql'])}
    temp_output_path = unique_path('output')
    session = arcobj.Editor(meta['dataset']['workspace_path'],
                            kwargs['use_edit_session'])
    with view['dataset'], view['clip'], session:
        arcpy.analysis.Clip(in_features=view['dataset'].name,
                            clip_features=view['clip'].name,
                            out_feature_class=temp_output_path,
                            cluster_tolerance=kwargs['tolerance'])
        delete(view['dataset'].name, log_level=None)
        insert_from_path(dataset_path, temp_output_path, log_level=None)
    dataset.delete(temp_output_path, log_level=None)
    log("End: Clip.")
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
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('use_edit_session', False)
    log = leveled_logger(LOG, kwargs.get('log_level', 'info'))
    if kwargs['dataset_where_sql']:
        log("Start: Delete features from %s where `%s`.",
            dataset_path, kwargs['dataset_where_sql'])
    else:
        log("Start: Delete features from %s.", dataset_path)
    meta = {'dataset': arcobj.dataset_metadata(dataset_path)}
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
    run_truncate = (kwargs['dataset_where_sql'] is None
                    and kwargs['use_edit_session'] is False)
    feature_count = Counter()
    if run_truncate:
        feature_count['deleted'] = dataset.feature_count(dataset_path)
        feature_count['unchanged'] = 0
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
        view = {'dataset': arcobj.DatasetView(dataset_path,
                                              kwargs['dataset_where_sql'])}
        session = arcobj.Editor(meta['dataset']['workspace_path'],
                                kwargs['use_edit_session'])
        with view['dataset'], session:
            feature_count['deleted'] = view['dataset'].count
            arcpy.management.DeleteRows(in_rows=view['dataset'].name)
        feature_count['unchanged'] = dataset.feature_count(dataset_path)
    for key in ('deleted', 'unchanged'):
        log("%s features %s.", feature_count[key], key)
    log("End: Delete.")
    return feature_count


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
            Default is False.
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        str: Path of the dataset updated.

    """
    kwargs.setdefault('use_edit_session', False)
    log = leveled_logger(LOG, kwargs.get('log_level', 'info'))
    log("Start: Delete features in %s with given IDs.", dataset_path)
    meta = {'dataset': arcobj.dataset_metadata(dataset_path)}
    keys = {'id': tuple(contain(id_field_names))}
    if inspect.isgeneratorfunction(delete_ids):
        delete_ids = delete_ids()
    ids = {'delete': {tuple(contain(_id)) for _id in delete_ids}}
    feature_count = Counter()
    session = arcobj.Editor(meta['dataset']['workspace_path'],
                            kwargs['use_edit_session'])
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
        log("%s features %s.", feature_count[key], key)
    log("End: Delete.")
    return feature_count


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
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('tolerance')
    kwargs.setdefault('use_edit_session', False)
    log = leveled_logger(LOG, kwargs.get('log_level', 'info'))
    log("Start: Dissolve features in %s on fields: %s.",
        dataset_path, dissolve_field_names)
    meta = {'dataset': arcobj.dataset_metadata(dataset_path)}
    keys = {'dissolve': tuple(contain(dissolve_field_names))}
    view = {'dataset': arcobj.DatasetView(dataset_path,
                                          kwargs['dataset_where_sql'])}
    if kwargs['tolerance'] is not None:
        old_tolerance = arcpy.env.XYTolerance
        arcpy.env.XYTolerance = kwargs['tolerance']
    temp_output_path = unique_path('output')
    with view['dataset']:
        arcpy.management.Dissolve(in_features=view['dataset'].name,
                                  out_feature_class=temp_output_path,
                                  dissolve_field=keys['dissolve'],
                                  multi_part=multipart,
                                  unsplit_lines=unsplit_lines)
    if kwargs['tolerance'] is not None:
        arcpy.env.XYTolerance = old_tolerance
    session = arcobj.Editor(meta['dataset']['workspace_path'],
                            kwargs['use_edit_session'])
    with session:
        delete(dataset_path, dataset_where_sql=kwargs['dataset_where_sql'],
               log_level=None)
        insert_from_path(dataset_path, insert_dataset_path=temp_output_path,
                         log_level=None)
    dataset.delete(temp_output_path, log_level=None)
    log("End: Dissolve.")
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
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('use_edit_session', False)
    log = leveled_logger(LOG, kwargs.get('log_level', 'info'))
    log("Start: Eliminate interior rings in %s.", dataset_path)
    # Only set max_percent_total_area default if neither it or area defined.
    if all((max_area is None, max_percent_total_area is None)):
        max_percent_total_area = 99.9999
    if all((max_area is not None, max_percent_total_area is not None)):
        condition = 'area_or_percent'
    elif max_area is not None:
        condition = 'area'
    else:
        condition = 'percent'
    meta = {'dataset': arcobj.dataset_metadata(dataset_path)}
    view = {'dataset': arcobj.DatasetView(dataset_path,
                                          kwargs['dataset_where_sql'])}
    temp_output_path = unique_path('output')
    with view['dataset']:
        arcpy.management.EliminatePolygonPart(
            in_features=view['dataset'].name,
            out_feature_class=temp_output_path,
            condition=condition, part_area=max_area,
            part_area_percent=max_percent_total_area,
            part_option='contained_only',
            )
    session = arcobj.Editor(meta['dataset']['workspace_path'],
                            kwargs['use_edit_session'])
    with session:
        delete(dataset_path, dataset_where_sql=kwargs['dataset_where_sql'],
               log_level=None)
        insert_from_path(dataset_path, insert_dataset_path=temp_output_path,
                         log_level=None)
    dataset.delete(temp_output_path, log_level=None)
    log("End: Eliminate.")
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
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('erase_where_sql')
    kwargs.setdefault('tolerance')
    kwargs.setdefault('use_edit_session', False)
    log = leveled_logger(LOG, kwargs.get('log_level', 'info'))
    log("Start: Erase features in %s where overlapping %s.",
        dataset_path, erase_dataset_path)
    meta = {'dataset': arcobj.dataset_metadata(dataset_path)}
    view = {'dataset': arcobj.DatasetView(dataset_path,
                                          kwargs['dataset_where_sql']),
            'erase': arcobj.DatasetView(erase_dataset_path,
                                        kwargs['erase_where_sql'])}
    temp_output_path = unique_path('output')
    with view['dataset'], view['erase']:
        arcpy.analysis.Erase(in_features=view['dataset'].name,
                             erase_features=view['erase'].name,
                             out_feature_class=temp_output_path,
                             cluster_tolerance=kwargs['tolerance'])
    session = arcobj.Editor(meta['dataset']['workspace_path'],
                            kwargs['use_edit_session'])
    with session:
        delete(dataset_path, dataset_where_sql=kwargs['dataset_where_sql'],
               log_level=None)
        insert_from_path(dataset_path, insert_dataset_path=temp_output_path,
                         log_level=None)
    dataset.delete(temp_output_path, log_level=None)
    log("End: Erase.")
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
    kwargs.setdefault('use_edit_session', False)
    log = leveled_logger(LOG, kwargs.get('log_level', 'info'))
    log("Start: Insert features into %s from dictionaries.", dataset_path)
    keys = {'row': tuple(contain(field_names))}
    if inspect.isgeneratorfunction(insert_features):
        insert_features = insert_features()
    iters = ((feature[key] for key in keys['row'])
             for feature in insert_features)
    feature_count = insert_from_iters(
        dataset_path, iters, field_names,
        use_edit_session=kwargs['use_edit_session'], log_level=None,
        )
    log("%s features inserted.", feature_count['inserted'])
    log("End: Insert.")
    return feature_count


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
    kwargs.setdefault('use_edit_session', False)
    log = leveled_logger(LOG, kwargs.get('log_level', 'info'))
    log("Start: Insert features into %s from iterables.", dataset_path)
    meta = {'dataset': arcobj.dataset_metadata(dataset_path)}
    keys = {'row': tuple(contain(field_names))}
    if inspect.isgeneratorfunction(insert_features):
        insert_features = insert_features()
    session = arcobj.Editor(meta['dataset']['workspace_path'],
                            kwargs['use_edit_session'])
    cursor = arcpy.da.InsertCursor(dataset_path, field_names=keys['row'])
    feature_count = Counter()
    with session, cursor:
        for row in insert_features:
            cursor.insertRow(tuple(row))
            feature_count['inserted'] += 1
    log("%s features inserted.", feature_count['inserted'])
    log("End: Insert.")
    return feature_count


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
        insert_with_cursor (bool): Flag to insert features using a cursor,
            instead of the Append tool. Default is False: the Append tool
            does a better job handling unusual geometries.
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        str: Path of the dataset updated.

    """
    kwargs.setdefault('insert_where_sql')
    kwargs.setdefault('use_edit_session', False)
    kwargs.setdefault('insert_with_cursor', False)
    log = leveled_logger(LOG, kwargs.get('log_level', 'info'))
    log("Start: Insert features into %s from %s.",
        dataset_path, insert_dataset_path)
    metas = (arcobj.dataset_metadata(dataset_path),
             arcobj.dataset_metadata(insert_dataset_path))
    keys = {}
    if field_names is None:
        keys['insert'] = set.intersection(
            *(set(name.lower() for name in meta['field_names'])
              for meta in metas)
            )
    else:
        keys['insert'] = set(name.lower() for name in contain(field_names))
    for meta in metas:
        # OIDs have no business being part of an insert.
        for key in (meta['oid_field_name'], 'oid@'):
            keys['insert'].discard(key)
        # If field names include geometry, use token.
        if meta['geom_field_name'] and (meta['geom_field_name'].lower()
                                        in keys['insert']):
            keys['insert'].remove(meta['geom_field_name'].lower())
            keys['insert'].add('shape@')
    if kwargs['insert_with_cursor']:
        log("Features will be inserted with cursor.")
        feature_count = _insert_from_path_with_cursor(
            dataset_path, insert_dataset_path, keys['insert'], **kwargs
            )
    else:
        feature_count = _insert_from_path_with_append(
            dataset_path, insert_dataset_path, keys['insert'], **kwargs
            )
    log("%s features inserted.", feature_count['inserted'])
    log("End: Insert.")
    return feature_count


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
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('location_where_sql')
    kwargs.setdefault('use_edit_session', False)
    log = leveled_logger(LOG, kwargs.get('log_level', 'info'))
    log("Start: Keep features in %s where overlapping %s.",
        dataset_path, location_dataset_path)
    meta = {'dataset': arcobj.dataset_metadata(dataset_path)}
    session = arcobj.Editor(meta['dataset']['workspace_path'],
                            kwargs['use_edit_session'])
    view = {'dataset': arcobj.DatasetView(dataset_path,
                                          kwargs['dataset_where_sql']),
            'location': arcobj.DatasetView(location_dataset_path,
                                           kwargs['location_where_sql'])}
    with session, view['dataset'], view['location']:
        arcpy.management.SelectLayerByLocation(
            in_layer=view['dataset'].name, overlap_type='intersect',
            select_features=view['location'].name,
            selection_type='new_selection',
            )
        arcpy.management.SelectLayerByLocation(
            in_layer=view['dataset'].name, selection_type='switch_selection',
            )
        feature_count = delete(view['dataset'].name, log_level=None)
    for key in ('deleted', 'unchanged'):
        log("%s features %s.", feature_count[key], key)
    log("End: Keep.")
    return feature_count


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
            missing from update_features, False otherwise. Default is True.
        use_edit_session (bool): Flag to perform updates in an edit session.
            Default is True.
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        str: Path of the dataset updated.

    """
    kwargs.setdefault('delete_missing_features', True)
    kwargs.setdefault('use_edit_session', True)
    log = leveled_logger(LOG, kwargs.get('log_level', 'info'))
    log("Start: Update features in %s from dictionaries.", dataset_path)
    keys = {'id': tuple(contain(id_field_names)),
            'attr': tuple(contain(field_names))}
    keys['row'] = keys['id'] + keys['attr']
    if inspect.isgeneratorfunction(update_features):
        update_features = update_features()
    iters = ((feature[key] for key in keys['row'])
             for feature in update_features)
    feature_count = update_from_iters(
        dataset_path, update_features=iters,
        id_field_names=keys['id'], field_names=keys['row'],
        delete_missing_features=kwargs['delete_missing_features'],
        use_edit_session=kwargs['use_edit_session'],
        log_level=None,
        )
    for key in UPDATE_TYPES:
        log("%s features %s.", feature_count[key], key)
    log("End: Update.")
    return feature_count


def update_from_iters(dataset_path, update_features, id_field_names, field_names,
                      **kwargs):
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
            missing from update_features, False otherwise. Default is True.
        use_edit_session (bool): Flag to perform updates in an edit session.
            Default is True.
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        collections.Counter: Counts for each update type.

    """
    kwargs.setdefault('delete_missing_features', True)
    kwargs.setdefault('use_edit_session', True)
    log = leveled_logger(LOG, kwargs.get('log_level', 'info'))
    log("Start: Update features in %s from iterables.", dataset_path)
    meta = {'dataset': arcobj.dataset_metadata(dataset_path)}
    keys = {'id': tuple(contain(id_field_names)),
            'feat': tuple(contain(field_names))}
    if not set(keys['id']).issubset(keys['feat']):
        raise ValueError("id_field_names must be a subset of field_names.")
    ids = {'dataset': {tuple(freeze_values(*_id)) for _id
                       in attributes.as_iters(dataset_path, keys['id'])}}
    feats = {'update': (update_features()
                        if inspect.isgeneratorfunction(update_features)
                        else update_features),
             'insert': set(),
             'id_update': dict()}
    for feat in feats['update']:
        feat = tuple(freeze_values(*feat))
        _id = tuple(feat[keys['feat'].index(key)] for key in keys['id'])
        if _id not in ids['dataset']:
            feats['insert'].add(feat)
        else:
            feats['id_update'][_id] = feat
    if kwargs['delete_missing_features']:
        ids['delete'] = {_id for _id in ids['dataset']
                         if _id not in feats['id_update']}
    feature_count = Counter()
    session = arcobj.Editor(meta['dataset']['workspace_path'],
                            kwargs['use_edit_session'])
    if ids['delete'] or feats['id_update']:
        cursor = arcpy.da.UpdateCursor(dataset_path, field_names=keys['feat'])
        with session, cursor:
            for feat in cursor:
                _id = tuple(freeze_values(*(feat[keys['feat'].index(key)]
                                            for key in keys['id'])))
                if _id in ids['delete']:
                    cursor.deleteRow()
                    feature_count['deleted'] += 1
                    continue
                elif (_id in feats['id_update']
                      and not _same_feature(feat, feats['id_update'][_id])):
                    cursor.updateRow(feats['id_update'][_id])
                    feature_count['altered'] += 1
                else:
                    feature_count['unchanged'] += 1
    if feats['insert']:
        cursor = arcpy.da.InsertCursor(dataset_path, field_names=keys['feat'])
        with session, cursor:
            for feat in feats['insert']:
                try:
                    cursor.insertRow(feat)
                except RuntimeError:
                    LOG.error("Feature failed to write to cursor."
                              " Offending row:")
                    for key, val in zip(keys['feat'], feat):
                        LOG.error("%s: %s", key, val)
                    raise
                feature_count['inserted'] += 1
    for key in UPDATE_TYPES:
        log("%s features %s.", feature_count[key], key)
    log("End: Update.")
    return feature_count


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
            missing from update_features, False otherwise. Default is True.
        use_edit_session (bool): Flag to perform updates in an edit session.
            Default is True.
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        collections.Counter: Counts for each update type.

    """
    kwargs.setdefault('update_where_sql')
    kwargs.setdefault('delete_missing_features', True)
    kwargs.setdefault('use_edit_session', True)
    log = leveled_logger(LOG, kwargs.get('log_level', 'info'))
    log("Start: Update features in %s from %s.",
        dataset_path, update_dataset_path)
    meta = {'dataset': arcobj.dataset_metadata(dataset_path),
            'update': arcobj.dataset_metadata(update_dataset_path)}
    if field_names is None:
        field_names = (
            set(n.lower() for n in meta['dataset']['field_names_tokenized'])
            & set(n.lower() for n in meta['update']['field_names_tokenized'])
            )
    else:
        field_names = set(n.lower() for n in field_names)
    # But OIDs have no business being part of an update.
    field_names.discard('oid@')
    keys = {'id': tuple(contain(id_field_names)),
            'attr': tuple(contain(field_names))}
    keys['row'] = keys['id'] + keys['attr']
    iters = attributes.as_iters(
        update_dataset_path, keys['row'],
        dataset_where_sql=kwargs['update_where_sql'],
        )
    feature_count = update_from_iters(
        dataset_path, update_features=iters,
        id_field_names=keys['id'], field_names=keys['row'],
        delete_missing_features=kwargs['delete_missing_features'],
        use_edit_session=kwargs['use_edit_session'],
        log_level=None,
        )
    for key in UPDATE_TYPES:
        log("%s features %s.", feature_count[key], key)
    log("End: Update.")
    return feature_count
