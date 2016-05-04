# -*- coding=utf-8 -*-
"""Geometric construct operations."""
import logging

import arcpy

from . import arcwrap, fields, helpers, properties


LOG = logging.getLogger(__name__)


@helpers.log_function
#pylint: disable=too-many-arguments
def generate_facility_service_rings(dataset_path, output_path, network_path,
                                    cost_attribute, ring_width, max_distance,
                                    **kwargs):
    #pylint: enable=too-many-arguments
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
        id_field_name (str): Name of facility ID field.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [
            ('dataset_where_sql', None), ('detailed_rings', False),
            ('id_field_name', None), ('log_level', 'info'),
            ('overlap_facilities', True), ('restriction_attributes', []),
            ('travel_from_facility', False)]:
        kwargs.setdefault(*kwarg_default)
    meta = {
        'description': "Generate service rings for {} facilities.".format(
            dataset_path),
        'dataset_view_name': arcwrap.create_dataset_view(
            helpers.unique_name('view'), dataset_path,
            dataset_where_sql=kwargs['dataset_where_sql']),
        'type_id_function_map': {
            'short': (lambda x: int(x.split(' : ')[0]) if x else None),
            'long': (lambda x: int(x.split(' : ')[0]) if x else None),
            'double': (lambda x: float(x.split(' : ')[0]) if x else None),
            'single': (lambda x: float(x.split(' : ')[0]) if x else None),
            'string': (lambda x: x.split(' : ')[0] if x else None)}}
    helpers.log_line('start', meta['description'], kwargs['log_level'])
    helpers.toggle_arc_extension('Network', toggle_on=True)
    try:
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
            # The trim seems to override the non-overlapping part in
            # larger analyses.
            #polygon_trim=True, poly_trim_value=ring_width,
            hierarchy='no_hierarchy')
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    try:
        arcpy.na.AddLocations(
            in_network_analysis_layer="service_area", sub_layer="Facilities",
            in_table=meta['dataset_view_name'],
            field_mappings='Name {} #'.format(kwargs['id_field_name']),
            search_tolerance=max_distance, match_type='match_to_closest',
            append='clear', snap_to_position_along_network='no_snap',
            exclude_restricted_elements=True)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    arcwrap.delete_dataset(meta['dataset_view_name'])
    try:
        arcpy.na.Solve(in_network_analysis_layer="service_area",
                       ignore_invalids=True, terminate_on_solve_error=True)
    except arcpy.ExecuteError:
        LOG.exception("ArcPy execution.")
        raise
    helpers.toggle_arc_extension('Network', toggle_off=True)
    arcwrap.copy_dataset('service_area/Polygons', output_path)
    arcwrap.delete_dataset('service_area')
    if kwargs['id_field_name']:
        id_field_metadata = properties.field_metadata(
            dataset_path, kwargs['id_field_name'])
        fields.add_fields_from_metadata_list(
            output_path, [id_field_metadata], log_level=None)
        fields.update_field_by_function(
            output_path, kwargs['id_field_name'],
            function=meta['type_id_function_map'][id_field_metadata['type']],
            field_as_first_arg=False, arg_field_names=['Name'], log_level=None)
    helpers.log_line('end', meta['description'], kwargs['log_level'])
    return output_path
