"""Feature operations."""
from collections import Counter
import inspect
from itertools import chain
import logging

import arcpy

from arcetl import arcobj
from arcetl import attributes
from arcetl import dataset
from arcetl.helpers import (
    contain, freeze_values, leveled_logger, same_feature, unique_name, unique_path
)


LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""

UPDATE_TYPES = ['deleted', 'inserted', 'altered', 'unchanged']
"""list of str: Types of feature updates commonly associated wtth update counters."""

arcpy.SetLogHistory(False)


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
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        str: Path of the dataset updated.

    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('clip_where_sql')
    kwargs.setdefault('use_edit_session', False)
    kwargs.setdefault('tolerance')
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    log(
        "Start: Clip features in %s where overlapping %s.",
        dataset_path,
        clip_dataset_path,
    )
    meta = {'dataset': arcobj.dataset_metadata(dataset_path)}
    view = {
        'dataset': arcobj.DatasetView(dataset_path, kwargs['dataset_where_sql']),
        'clip': arcobj.DatasetView(clip_dataset_path, kwargs['clip_where_sql']),
    }
    temp_output_path = unique_path('output')
    session = arcobj.Editor(
        meta['dataset']['workspace_path'], kwargs['use_edit_session']
    )
    with view['dataset'], view['clip'], session:
        arcpy.analysis.Clip(
            in_features=view['dataset'].name,
            clip_features=view['clip'].name,
            out_feature_class=temp_output_path,
            cluster_tolerance=kwargs['tolerance'],
        )
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
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        collections.Counter: Counts for each feature action.

    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('use_edit_session', False)
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    if kwargs['dataset_where_sql']:
        log(
            "Start: Delete features from %s where `%s`.",
            dataset_path,
            kwargs['dataset_where_sql'],
        )
    else:
        log("Start: Delete features from %s.", dataset_path)
    meta = {'dataset': arcobj.dataset_metadata(dataset_path)}
    truncate_error_codes = [
        # "Only supports Geodatabase tables and feature classes."
        'ERROR 000187',
        # "Operation not supported on a versioned table."
        'ERROR 001259',
        # "Operation not supported on table {table name}."
        'ERROR 001260',
        # Operation not supported on a feature class in a controller dataset.
        'ERROR 001395',
        # Only the data owner may execute truncate.
        "ERROR 001400",
    ]
    feature_count = Counter()
    # Can use (faster) truncate when no sub-selection or edit session.
    run_truncate = (
        kwargs['dataset_where_sql'] is None and kwargs['use_edit_session'] is False
    )
    if run_truncate:
        feature_count['deleted'] = dataset.feature_count(dataset_path)
        feature_count['unchanged'] = 0
        try:
            arcpy.management.TruncateTable(in_table=dataset_path)
        except arcpy.ExecuteError:
            # Avoid arcpy.GetReturnCode(); error code position inconsistent.
            # Search messages for 'ERROR ######' instead.
            if any(code in arcpy.GetMessages() for code in truncate_error_codes):
                LOG.debug("Truncate unsupported; will try deleting rows.")
                run_truncate = False
            else:
                raise

    if not run_truncate:
        view = {
            'dataset': arcobj.DatasetView(dataset_path, kwargs['dataset_where_sql'])
        }
        session = arcobj.Editor(
            meta['dataset']['workspace_path'], kwargs['use_edit_session']
        )
        with view['dataset'], session:
            feature_count['deleted'] = view['dataset'].count
            arcpy.management.DeleteRows(in_rows=view['dataset'].name)
        feature_count['unchanged'] = dataset.feature_count(dataset_path)
    for key in ['deleted', 'unchanged']:
        log("%s features %s.", feature_count[key], key)
    log("End: Delete.")
    return feature_count


def delete_by_id(dataset_path, delete_ids, id_field_names, **kwargs):
    """Delete features in dataset with given IDs.

    Note:
        There is no guarantee that the ID field(s) are unique.
        Use ArcPy cursor token names for object IDs and geometry objects/properties.

    Args:
        dataset_path (str): Path of the dataset.
        delete_ids (iter): Collection of feature IDs.
        id_field_names (iter, str): Name(s) of the ID field/key(s).
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        collections.Counter: Counts for each feature action.

    """
    kwargs.setdefault('use_edit_session', False)
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    log("Start: Delete features in %s with given IDs.", dataset_path)
    meta = {'dataset': arcobj.dataset_metadata(dataset_path)}
    keys = {'id': list(contain(id_field_names))}
    if inspect.isgeneratorfunction(delete_ids):
        delete_ids = delete_ids()
    ids = {'delete': {tuple(contain(_id)) for _id in delete_ids}}
    feature_count = Counter()
    session = arcobj.Editor(
        meta['dataset']['workspace_path'], kwargs['use_edit_session']
    )
    if ids['delete']:
        cursor = arcpy.da.UpdateCursor(dataset_path, field_names=keys['id'])
        with session, cursor:
            for row in cursor:
                _id = tuple(row)
                if _id in ids['delete']:
                    cursor.deleteRow()
                    feature_count['deleted'] += 1
    feature_count['unchanged'] = dataset.feature_count(dataset_path)
    for key in ['deleted', 'unchanged']:
        log("%s features %s.", feature_count[key], key)
    log("End: Delete.")
    return feature_count


def dissolve(dataset_path, dissolve_field_names=None, multipart=True, **kwargs):
    """Dissolve geometry of features that share values in given fields.

    Args:
        dataset_path (str): Path of the dataset.
        dissolve_field_names (iter): Iterable of field names to dissolve on.
        multipart (bool): Flag to allow multipart features in output.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        unsplit_lines (bool): Flag to merge line features when endpoints meet without
            crossing features. Default is False.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        tolerance (float): Tolerance for coincidence, in dataset's units.
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        str: Path of the dataset updated.

    """
    kwargs.setdefault('unsplit_lines', False)
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('use_edit_session', False)
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    log(
        "Start: Dissolve features in %s on fields: %s.",
        dataset_path,
        dissolve_field_names,
    )
    meta = {
        'dataset': arcobj.dataset_metadata(dataset_path),
        'orig_tolerance': arcpy.env.XYTolerance,
    }
    keys = {'dissolve': tuple(contain(dissolve_field_names))}
    view = {'dataset': arcobj.DatasetView(dataset_path, kwargs['dataset_where_sql'])}
    temp_output_path = unique_path('output')
    with view['dataset']:
        if 'tolerance' in kwargs:
            arcpy.env.XYTolerance = kwargs['tolerance']
        arcpy.management.Dissolve(
            in_features=view['dataset'].name,
            out_feature_class=temp_output_path,
            dissolve_field=keys['dissolve'],
            multi_part=multipart,
            unsplit_lines=kwargs['unsplit_lines'],
        )
        if 'tolerance' in kwargs:
            arcpy.env.XYTolerance = meta['orig_tolerance']
    session = arcobj.Editor(
        meta['dataset']['workspace_path'], kwargs['use_edit_session']
    )
    with session:
        delete(
            dataset_path, dataset_where_sql=kwargs['dataset_where_sql'], log_level=None
        )
        insert_from_path(
            dataset_path, insert_dataset_path=temp_output_path, log_level=None
        )
    dataset.delete(temp_output_path, log_level=None)
    log("End: Dissolve.")
    return dataset_path


def eliminate_interior_rings(
    dataset_path, max_area=None, max_percent_total_area=None, **kwargs
):
    """Eliminate interior rings of polygon features.

    Note:
        If no value if provided for either max_area or max_percent_total_area, (nearly)
        all interior rings will be removed. Technically, max_percent_total_area will be
        set to 99.9999.

    Args:
        dataset_path (str): Path of the dataset.
        max_area (float, str): Maximum area which parts smaller than are eliminated.
            Numeric area will be in dataset's units. String area will be formatted as
            '{number} {unit}'.
        max_percent_total_area (float): Maximum percent of total area which parts
            smaller than are eliminated.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        str: Path of the dataset updated.

    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('use_edit_session', False)
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    log("Start: Eliminate interior rings in %s.", dataset_path)
    # Only set max_percent_total_area default if neither it or area defined.
    if all([max_area is None, max_percent_total_area is None]):
        max_percent_total_area = 99.9999
    if all([max_area is not None, max_percent_total_area is not None]):
        condition = 'area_or_percent'
    elif max_area is not None:
        condition = 'area'
    else:
        condition = 'percent'
    meta = {'dataset': arcobj.dataset_metadata(dataset_path)}
    view = {'dataset': arcobj.DatasetView(dataset_path, kwargs['dataset_where_sql'])}
    temp_output_path = unique_path('output')
    with view['dataset']:
        arcpy.management.EliminatePolygonPart(
            in_features=view['dataset'].name,
            out_feature_class=temp_output_path,
            condition=condition,
            part_area=max_area,
            part_area_percent=max_percent_total_area,
            part_option='contained_only',
        )
    session = arcobj.Editor(
        meta['dataset']['workspace_path'], kwargs['use_edit_session']
    )
    with session:
        delete(
            dataset_path, dataset_where_sql=kwargs['dataset_where_sql'], log_level=None
        )
        insert_from_path(
            dataset_path, insert_dataset_path=temp_output_path, log_level=None
        )
    dataset.delete(temp_output_path, log_level=None)
    log("End: Eliminate.")
    return dataset_path


def erase(dataset_path, erase_dataset_path, **kwargs):
    """Erase feature geometry where it overlaps erase-dataset geometry.

    Args:
        dataset_path (str): Path of the dataset.
        erase_dataset_path (str): Path of the dataset defining the erase-area.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        erase_where_sql (str): SQL where-clause for erase-dataset subselection.
        tolerance (float): Tolerance for coincidence, in dataset's units.
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        str: Path of the dataset updated.

    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('erase_where_sql')
    kwargs.setdefault('tolerance')
    kwargs.setdefault('use_edit_session', False)
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    log(
        "Start: Erase features in %s where overlapping %s.",
        dataset_path,
        erase_dataset_path,
    )
    meta = {'dataset': arcobj.dataset_metadata(dataset_path)}
    view = {
        'dataset': arcobj.DatasetView(dataset_path, kwargs['dataset_where_sql']),
        'erase': arcobj.DatasetView(erase_dataset_path, kwargs['erase_where_sql']),
    }
    temp_output_path = unique_path('output')
    with view['dataset'], view['erase']:
        arcpy.analysis.Erase(
            in_features=view['dataset'].name,
            erase_features=view['erase'].name,
            out_feature_class=temp_output_path,
            cluster_tolerance=kwargs['tolerance'],
        )
    session = arcobj.Editor(
        meta['dataset']['workspace_path'], kwargs['use_edit_session']
    )
    with session:
        delete(
            dataset_path, dataset_where_sql=kwargs['dataset_where_sql'], log_level=None
        )
        insert_from_path(
            dataset_path, insert_dataset_path=temp_output_path, log_level=None
        )
    dataset.delete(temp_output_path, log_level=None)
    log("End: Erase.")
    return dataset_path


def insert_from_dicts(dataset_path, insert_features, field_names, **kwargs):
    """Insert features into dataset from dictionaries.

    Args:
        dataset_path (str): Path of the dataset.
        insert_features (iter of dict): Collection of dictionaries representing
            features.
        field_names (iter): Collection of field names/keys to insert.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        collections.Counter: Counts for each feature action.

    """
    kwargs.setdefault('use_edit_session', False)
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    log("Start: Insert features into %s from dictionaries.", dataset_path)
    keys = {'row': list(contain(field_names))}
    if inspect.isgeneratorfunction(insert_features):
        insert_features = insert_features()
    iters = ((feature[key] for key in keys['row']) for feature in insert_features)
    feature_count = insert_from_iters(
        dataset_path,
        iters,
        field_names,
        use_edit_session=kwargs['use_edit_session'],
        log_level=None,
    )
    log("%s features inserted.", feature_count['inserted'])
    log("End: Insert.")
    return feature_count


def insert_from_iters(dataset_path, insert_features, field_names, **kwargs):
    """Insert features into dataset from iterables.

    Args:
        dataset_path (str): Path of the dataset.
        insert_features (iter of iter): Collection of iterables representing features.
        field_names (iter): Collection of field names to insert. These must match the
            order of their attributes in the insert_features items.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        collections.Counter: Counts for each feature action.

    """
    kwargs.setdefault('use_edit_session', False)
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    log("Start: Insert features into %s from iterables.", dataset_path)
    meta = {'dataset': arcobj.dataset_metadata(dataset_path)}
    keys = {'row': list(contain(field_names))}
    if inspect.isgeneratorfunction(insert_features):
        insert_features = insert_features()
    session = arcobj.Editor(
        meta['dataset']['workspace_path'], kwargs['use_edit_session']
    )
    cursor = arcpy.da.InsertCursor(dataset_path, field_names=keys['row'])
    feature_count = Counter()
    with session, cursor:
        for row in insert_features:
            cursor.insertRow(tuple(row))
            feature_count['inserted'] += 1
    log("%s features inserted.", feature_count['inserted'])
    log("End: Insert.")
    return feature_count


def insert_from_path(dataset_path, insert_dataset_path, field_names=None, **kwargs):
    """Insert features into dataset from another dataset.

    Args:
        dataset_path (str): Path of the dataset.
        insert_dataset_path (str): Path of dataset to insert features from.
        field_names (iter): Collection of field names to insert. Listed field must be
            present in both datasets. If field_names is None, all fields will be
            inserted.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        insert_where_sql (str): SQL where-clause for insert-dataset subselection.
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        collections.Counter: Counts for each feature action.

    """
    kwargs.setdefault('insert_where_sql')
    kwargs.setdefault('use_edit_session', False)
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    log("Start: Insert features into %s from %s.", dataset_path, insert_dataset_path)
    meta = {
        'dataset': arcobj.dataset_metadata(dataset_path),
        'insert': arcobj.dataset_metadata(insert_dataset_path),
    }
    if field_names is None:
        keys = set.intersection(
            *(
                set(name.lower() for name in _meta['field_names_tokenized'])
                for _meta in meta.values()
            )
        )
    else:
        keys = set(name.lower() for name in contain(field_names))
    # OIDs & area/length "fields" have no business being part of an insert.
    # Geometry itself is handled separately in append function.
    for _meta in meta.values():
        for key in chain(*_meta['field_token'].items()):
            keys.discard(key)
    append_kwargs = {
        'inputs': unique_name('view'),
        'target': dataset_path,
        'schema_type': 'no_test',
        'field_mapping': arcpy.FieldMappings(),
    }
    # Create field maps.
    # ArcGIS Pro's no-test append is case-sensitive (verified 1.0-1.1.1).
    # Avoid this problem by using field mapping.
    # BUG-000090970 - ArcGIS Pro 'No test' field mapping in Append tool does not auto-
    # map to the same field name if naming convention differs.
    for key in keys:
        field_map = arcpy.FieldMap()
        field_map.addInputField(insert_dataset_path, key)
        append_kwargs['field_mapping'].addFieldMap(field_map)
    view = arcobj.DatasetView(
        insert_dataset_path,
        kwargs['insert_where_sql'],
        view_name=append_kwargs['inputs'],
        # Must be nonspatial to append to nonspatial table.
        force_nonspatial=(not meta['dataset']['is_spatial']),
    )
    session = arcobj.Editor(
        meta['dataset']['workspace_path'], kwargs['use_edit_session']
    )
    with view, session:
        arcpy.management.Append(**append_kwargs)
        feature_count = Counter({'inserted': view.count})
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
        location_where_sql (str): SQL where-clause for location-dataset subselection.
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        collections.Counter: Counts for each feature action.

    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('location_where_sql')
    kwargs.setdefault('use_edit_session', False)
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    log(
        "Start: Keep features in %s where overlapping %s.",
        dataset_path,
        location_dataset_path,
    )
    meta = {'dataset': arcobj.dataset_metadata(dataset_path)}
    session = arcobj.Editor(
        meta['dataset']['workspace_path'], kwargs['use_edit_session']
    )
    view = {
        'dataset': arcobj.DatasetView(dataset_path, kwargs['dataset_where_sql']),
        'location': arcobj.DatasetView(
            location_dataset_path, kwargs['location_where_sql']
        ),
    }
    with session, view['dataset'], view['location']:
        arcpy.management.SelectLayerByLocation(
            in_layer=view['dataset'].name,
            overlap_type='intersect',
            select_features=view['location'].name,
            selection_type='new_selection',
        )
        arcpy.management.SelectLayerByLocation(
            in_layer=view['dataset'].name, selection_type='switch_selection'
        )
        feature_count = delete(view['dataset'].name, log_level=None)
    for key in ['deleted', 'unchanged']:
        log("%s features %s.", feature_count[key], key)
    log("End: Keep.")
    return feature_count


def update_from_dicts(
    dataset_path, update_features, id_field_names, field_names, **kwargs
):
    """Update features in dataset from dictionaries.

    Note:
        There is no guarantee that the ID field(s) are unique.
        Use ArcPy cursor token names for object IDs and geometry objects/properties.

    Args:
        dataset_path (str): Path of the dataset.
        update_features (iter of dict): Collection of dictionaries representing
            features.
        id_field_names (iter, str): Name(s) of the ID field/key(s).
        field_names (iter): Collection of field names/keys to check & update.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        delete_missing_features (bool): True if update should delete features missing
            from update_features, False otherwise. Default is True.
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            True.
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        collections.Counter: Counts for each feature action.

    """
    kwargs.setdefault('delete_missing_features', True)
    kwargs.setdefault('use_edit_session', True)
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    log("Start: Update features in %s from dictionaries.", dataset_path)
    keys = {'id': list(contain(id_field_names)), 'attr': list(contain(field_names))}
    keys['row'] = keys['id'] + keys['attr']
    if inspect.isgeneratorfunction(update_features):
        update_features = update_features()
    iters = ((feature[key] for key in keys['row']) for feature in update_features)
    feature_count = update_from_iters(
        dataset_path,
        update_features=iters,
        id_field_names=keys['id'],
        field_names=keys['row'],
        delete_missing_features=kwargs['delete_missing_features'],
        use_edit_session=kwargs['use_edit_session'],
        log_level=None,
    )
    for key in UPDATE_TYPES:
        log("%s features %s.", feature_count[key], key)
    log("End: Update.")
    return feature_count


def update_from_iters(
    dataset_path, update_features, id_field_names, field_names, **kwargs
):
    """Update features in dataset from iterables.

    Note:
        There is no guarantee that the ID field(s) are unique.
        Use ArcPy cursor token names for object IDs and geometry objects/properties.

    Args:
        dataset_path (str): Path of the dataset.
        update_features (iter of dict): Collection of iterables representing features.
        id_field_names (iter, str): Name(s) of the ID field/key(s). *All* ID fields
            must also be in field_names.
        field_names (iter): Collection of field names/keys to check & update.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        delete_missing_features (bool): True if update should delete features missing
            from update_features, False otherwise. Default is True.
        use_edit_session (bool): Flag to perform updates in an edit session. Default is True.
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        collections.Counter: Counts for each feature action.

    """
    kwargs.setdefault('delete_missing_features', True)
    kwargs.setdefault('use_edit_session', True)
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    log("Start: Update features in %s from iterables.", dataset_path)
    meta = {'dataset': arcobj.dataset_metadata(dataset_path)}
    keys = {'id': list(contain(id_field_names)), 'feat': list(contain(field_names))}
    if not set(keys['id']).issubset(keys['feat']):
        raise ValueError("id_field_names must be a subset of field_names.")

    ids = {
        'dataset': {
            tuple(freeze_values(*_id))
            for _id in attributes.as_iters(dataset_path, keys['id'])
        }
    }
    if inspect.isgeneratorfunction(update_features):
        update_features = update_features()
    feats = {'insert': set(), 'id_update': dict()}
    for feat in update_features:
        feat = tuple(freeze_values(*feat))
        _id = tuple(feat[keys['feat'].index(key)] for key in keys['id'])
        if _id not in ids['dataset']:
            feats['insert'].add(feat)
        else:
            feats['id_update'][_id] = feat
    if kwargs['delete_missing_features']:
        ids['delete'] = {_id for _id in ids['dataset'] if _id not in feats['id_update']}
    else:
        ids['delete'] = set()
    feature_count = Counter()
    session = arcobj.Editor(
        meta['dataset']['workspace_path'], kwargs['use_edit_session']
    )
    if ids['delete'] or feats['id_update']:
        cursor = arcpy.da.UpdateCursor(dataset_path, field_names=keys['feat'])
        with session, cursor:
            for feat in cursor:
                _id = tuple(
                    freeze_values(
                        *(feat[keys['feat'].index(key)] for key in keys['id'])
                    )
                )
                if _id in ids['delete']:
                    cursor.deleteRow()
                    feature_count['deleted'] += 1
                    continue

                elif (
                    _id in feats['id_update']
                    and not same_feature(feat, feats['id_update'][_id])
                ):
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
                    LOG.error("Feature failed to write to cursor. Offending row:")
                    for key, val in zip(keys['feat'], feat):
                        LOG.error("%s: %s", key, val)
                    raise

                feature_count['inserted'] += 1
    for key in UPDATE_TYPES:
        log("%s features %s.", feature_count[key], key)
    log("End: Update.")
    return feature_count


def update_from_path(
    dataset_path, update_dataset_path, id_field_names, field_names=None, **kwargs
):
    """Update features in dataset from another dataset.

    Args:
        dataset_path (str): Path of the dataset.
        update_dataset_path (str): Path of dataset to update features from.
        id_field_names (iter, str): Name(s) of the ID field/key(s).
        field_names (iter): Collection of field names/keys to check & update. Listed
            field must be present in both datasets. If field_names is None, all fields
            will be inserted.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection. WARNING:
            defining this has major effects: filtered features will not be considered
            for updating or deletion, and duplicates in the update features will be
            inserted as if novel.
        update_where_sql (str): SQL where-clause for update-dataset subselection.
        chunk_where_sqls (iter): Collection of SQL where-clauses for updating between
            the datasets in chunks.
        delete_missing_features (bool): True if update should delete features missing
            from update_features, False otherwise. Default is True.
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            True.
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        collections.Counter: Counts for each feature action.

    """
    for key in ['dataset_where_sql', 'update_where_sql']:
        kwargs.setdefault(key)
        if not kwargs[key]:
            kwargs[key] = "1=1"
    kwargs.setdefault('subset_where_sqls', ["1=1"])
    kwargs.setdefault('delete_missing_features', True)
    kwargs.setdefault('use_edit_session', True)
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    log("Start: Update features in %s from %s.", dataset_path, update_dataset_path)
    meta = {
        'dataset': arcobj.dataset_metadata(dataset_path),
        'update': arcobj.dataset_metadata(update_dataset_path),
    }
    if field_names is None:
        field_names = (
            set(name.lower() for name in meta['dataset']['field_names_tokenized']) &
            set(name.lower() for name in meta['update']['field_names_tokenized'])
        )
    else:
        field_names = set(name.lower() for name in field_names)
    # OIDs & area/length "fields" have no business being part of an update.
    for key in ['oid@', 'shape@area', 'shape@length']:
        field_names.discard(key)
    keys = {'id': list(contain(id_field_names)), 'attr': list(contain(field_names))}
    keys['row'] = keys['id'] + keys['attr']
    feature_count = Counter()
    for kwargs['subset_where_sql'] in contain(kwargs['subset_where_sqls']):
        if not kwargs['subset_where_sql'] == "1=1":
            log("Subset: `%s`", kwargs['subset_where_sql'])
        iters = attributes.as_iters(
            update_dataset_path,
            keys['row'],
            dataset_where_sql=(
                "({update_where_sql}) and ({subset_where_sql})".format(**kwargs)
            )
        )
        view = arcobj.DatasetView(
            dataset_path,
            dataset_where_sql=(
                "({dataset_where_sql}) and ({subset_where_sql})".format(**kwargs)
            ),
            field_names=keys['row']
        )
        with view:
            feature_count.update(
                update_from_iters(
                    dataset_path=view.name,
                    update_features=iters,
                    id_field_names=keys['id'],
                    field_names=keys['row'],
                    delete_missing_features=kwargs['delete_missing_features'],
                    use_edit_session=kwargs['use_edit_session'],
                    log_level=None,
                )
            )
    for key in UPDATE_TYPES:
        log("%s features %s.", feature_count[key], key)
    log("End: Update.")
    return feature_count
