# -*- coding=utf-8 -*-
"""Processing operation objects."""
import collections
import csv
import logging

import arcpy

from . import arcwrap, helpers


LOG = logging.getLogger(__name__)


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
    arcwrap.delete_dataset(dataset_view_name)
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

