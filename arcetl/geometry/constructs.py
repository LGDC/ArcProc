# -*- coding=utf-8 -*-
"""Geometric construct operations."""
import logging

import arcpy

from .. import arcwrap, fields, helpers, metadata


LOG = logging.getLogger(__name__)
TYPE_ID_FUNCTION_MAP = {
    'short': (lambda x: int(x.split(' : ')[0]) if x else None),
    'long': (lambda x: int(x.split(' : ')[0]) if x else None),
    'double': (lambda x: float(x.split(' : ')[0]) if x else None),
    'single': (lambda x: float(x.split(' : ')[0]) if x else None),
    'string': (lambda x: x.split(' : ')[0] if x else None)}


def generate_service_areas(dataset_path, output_path, network_path,
                           cost_attribute, max_distance, **kwargs):
    """Create network service area features.

    Args:
        dataset_path (str): Path of dataset.
        output_path (str): Path of output dataset.
        network_path (str): Path of network dataset.
        cost_attribute (str): Name of network cost attribute to use in
            analysis.
        max_distance (float): Distance in travel from the facility the outer
            ring will extend to, in the dataset's units.
    Kwargs:
        restriction_attributes (iter): Iterable of network restriction
            attribute names to use in analysis.
        travel_from_facility (bool): Flag indicating generating rings while
            travelling 'from' the facility. False indicate travelling 'to'.
        detailed_features (bool): Flag indicating features should be generated
            with high-detail.
        overlap_facilities (bool): Flag indicating whether different
            facilitys' service areas can overlap.
        trim_value (float): Distance from network features to trim areas to.
        id_field_name (str): Name of facility ID field.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [
            ('dataset_where_sql', None), ('detailed_features', False),
            ('id_field_name', None), ('log_level', 'info'),
            ('overlap_facilities', True), ('restriction_attributes', []),
            ('travel_from_facility', False), ('trim_value', None)]:
        kwargs.setdefault(*kwarg_default)
    log_level = helpers.LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Generate service areas for %s.", dataset_path)
    # trim_value assumes meters if not input as linear_unit string.
    if kwargs['trim_value']:
        kwargs['trim_value'] = metadata.linear_unit_as_string(
            kwargs['trim_value'], dataset_path)
    dataset_view_name = arcwrap.create_dataset_view(
        helpers.unique_name('view'), dataset_path,
        dataset_where_sql=kwargs['dataset_where_sql'])
    helpers.toggle_arc_extension('Network', toggle_on=True)
    arcpy.na.MakeServiceAreaLayer(
        in_network_dataset=network_path,
        out_network_analysis_layer='service_area',
        impedance_attribute=cost_attribute,
        travel_from_to=('travel_from' if kwargs['travel_from_facility']
                        else 'travel_to'),
        default_break_values='{}'.format(max_distance),
        polygon_type=('detailed_polys' if kwargs['detailed_features']
                      else 'simple_polys'),
        merge='no_merge' if kwargs['overlap_facilities'] else 'no_overlap',
        nesting_type='disks',
        UTurn_policy='allow_dead_ends_and_intersections_only',
        restriction_attribute_name=kwargs['restriction_attributes'],
        polygon_trim=True if kwargs['trim_value'] else False,
        poly_trim_value=kwargs['trim_value'],
        hierarchy='no_hierarchy')
    arcpy.na.AddLocations(
        in_network_analysis_layer="service_area", sub_layer="Facilities",
        in_table=dataset_view_name,
        field_mappings='Name {} #'.format(kwargs['id_field_name']),
        search_tolerance=max_distance, match_type='match_to_closest',
        append='clear', snap_to_position_along_network='no_snap',
        exclude_restricted_elements=True)
    arcwrap.delete_dataset(dataset_view_name)
    arcpy.na.Solve(in_network_analysis_layer="service_area",
                   ignore_invalids=True, terminate_on_solve_error=True)
    helpers.toggle_arc_extension('Network', toggle_off=True)
    arcwrap.copy_dataset('service_area/Polygons', output_path)
    arcwrap.delete_dataset('service_area')
    if kwargs['id_field_name']:
        id_field_metadata = metadata.field_metadata(
            dataset_path, kwargs['id_field_name'])
        fields.add_fields_from_metadata_list(
            output_path, [id_field_metadata], log_level=None)
        fields.update_field_by_function(
            output_path, kwargs['id_field_name'],
            function=TYPE_ID_FUNCTION_MAP[id_field_metadata['type']],
            field_as_first_arg=False, arg_field_names=['Name'], log_level=None)
    LOG.log(log_level, "End: Generate.")
    return output_path


def generate_service_rings(dataset_path, output_path, network_path,
                           cost_attribute, ring_width, max_distance, **kwargs):
    """Create facility service ring features using a network dataset.

    Args:
        dataset_path (str): Path of dataset.
        output_path (str): Path of output dataset.
        network_path (str): Path of network dataset.
        cost_attribute (str): Name of network cost attribute to use in
            analysis.
        ring_width (float): Distance a service ring represents in travel, in
            the dataset's units.
        max_distance (float): Distance in travel from the facility the outer
            ring will extend to, in the dataset's units.
    Kwargs:
        restriction_attributes (iter): Iterable of network restriction
            attribute names to use in analysis.
        travel_from_facility (bool): Flag indicating generating rings while
            travelling 'from' the facility. False indicate travelling 'to'.
        detailed_rings (bool): Flag indicating rings should be generated with
            high-detail.
        overlap_facilities (bool): Flag indicating whether different facility's
            rings can overlap.
        trim_value (float): Distance from network features to trim areas to.
        id_field_name (str): Name of facility ID field.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    if kwargs.get('trim_value'):
        raise NotImplementedError(
            "Polygon trim in ArcPy not working currently.")
    for kwarg_default in [
            ('dataset_where_sql', None), ('detailed_rings', False),
            ('id_field_name', None), ('log_level', 'info'),
            ('overlap_facilities', True), ('restriction_attributes', []),
            ('travel_from_facility', False), ('trim_value', None)]:
        kwargs.setdefault(*kwarg_default)
    log_level = helpers.LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Generate service rings for %s.", dataset_path)
    # trim_value assumes meters if not input as linear_unit string.
    if kwargs['trim_value']:
        kwargs['trim_value'] = metadata.linear_unit_as_string(
            kwargs['trim_value'], dataset_path)
    dataset_view_name = arcwrap.create_dataset_view(
        helpers.unique_name('view'), dataset_path,
        dataset_where_sql=kwargs['dataset_where_sql'])
    helpers.toggle_arc_extension('Network', toggle_on=True)
    arcpy.na.MakeServiceAreaLayer(
        in_network_dataset=network_path,
        out_network_analysis_layer='service_area',
        impedance_attribute=cost_attribute,
        travel_from_to=('travel_from' if kwargs['travel_from_facility']
                        else 'travel_to'),
        default_break_values=' '.join(
            str(x) for x
            in range(ring_width, max_distance + 1, ring_width)),
        polygon_type=('detailed_polys' if kwargs['detailed_rings']
                      else 'simple_polys'),
        merge='no_merge' if kwargs['overlap_facilities'] else 'no_overlap',
        nesting_type='rings',
        UTurn_policy='allow_dead_ends_and_intersections_only',
        restriction_attribute_name=kwargs['restriction_attributes'],
        polygon_trim=True if kwargs['trim_value'] else False,
        poly_trim_value=kwargs['trim_value'],
        hierarchy='no_hierarchy')
    arcpy.na.AddLocations(
        in_network_analysis_layer="service_area", sub_layer="Facilities",
        in_table=dataset_view_name,
        field_mappings='Name {} #'.format(kwargs['id_field_name']),
        search_tolerance=max_distance, match_type='match_to_closest',
        append='clear', snap_to_position_along_network='no_snap',
        exclude_restricted_elements=True)
    arcwrap.delete_dataset(dataset_view_name)
    arcpy.na.Solve(in_network_analysis_layer="service_area",
                   ignore_invalids=True, terminate_on_solve_error=True)
    helpers.toggle_arc_extension('Network', toggle_off=True)
    arcwrap.copy_dataset('service_area/Polygons', output_path)
    arcwrap.delete_dataset('service_area')
    if kwargs['id_field_name']:
        id_field_metadata = metadata.field_metadata(
            dataset_path, kwargs['id_field_name'])
        fields.add_fields_from_metadata_list(
            output_path, [id_field_metadata], log_level=None)
        fields.update_field_by_function(
            output_path, kwargs['id_field_name'],
            function=TYPE_ID_FUNCTION_MAP[id_field_metadata['type']],
            field_as_first_arg=False, arg_field_names=['Name'], log_level=None)
    LOG.log(log_level, "End: Generate.")
    return output_path
