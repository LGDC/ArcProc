# -*- coding=utf-8 -*-
"""Network analysis operations."""
import logging

import arcpy

from arcetl import attributes, dataset, metadata, values, workspace
from arcetl.helpers import LOG_LEVEL_MAP, toggle_arc_extension, unique_name


LOG = logging.getLogger(__name__)

TYPE_ID_FUNCTION_MAP = {
    'short': (lambda x: int(x.split(' : ')[0]) if x else None),
    'long': (lambda x: int(x.split(' : ')[0]) if x else None),
    'double': (lambda x: float(x.split(' : ')[0]) if x else None),
    'single': (lambda x: float(x.split(' : ')[0]) if x else None),
    'string': (lambda x: x.split(' : ')[0] if x else None)
    }


def build(network_path, **kwargs):
    """Build network dataset.

    Wraps workspace.build_network.

    Args:
        network_path (str): Path of network.
    Kwargs:
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    return workspace.build_network(network_path, **kwargs)


def closest_facility_route(dataset_path, id_field_name, facility_path,
                           facility_id_field_name, network_path, cost_attribute,
                           **kwargs):
    """Generator of route info dictionaries to location's closest facility.

    Args:
        dataset_path (str): Path of locations dataset.
        id_field_name (str): Name of dataset ID field.
        facility_path (str): Path of facilities dataset.
        facility_id_field_name (str): Name of facility ID field.
        network_path (str): Path of network dataset.
        cost_attribute (str): Name of network cost attribute to use in
            analysis.
    Kwargs:
        max_cost (float): Maximum travel cost the search will attempt, in the
            cost attribute's units.
        restriction_attributes (iter): Iterable of network restriction
            attribute names to use in analysis.
        travel_from_facility (bool): Flag indicating generating rings while
            travelling 'from' the facility. False indicate travelling 'to'.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        facility_where_sql (str): SQL where-clause for facility subselection.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [
            ('dataset_where_sql', None), ('facility_where_sql', None),
            ('log_level', 'info'), ('max_cost', None),
            ('restriction_attributes', []), ('travel_from_facility', False)
        ]:
        kwargs.setdefault(*kwarg_default)
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level,
            "Start: Generate closest facility in %s to locations in %s.",
            facility_path, dataset_path)
    toggle_arc_extension('Network', toggle_on=True)
    arcpy.na.MakeClosestFacilityLayer(
        in_network_dataset=network_path, out_network_analysis_layer='closest',
        impedance_attribute=cost_attribute,
        travel_from_to=('travel_from' if kwargs['travel_from_facility']
                        else 'travel_to'),
        default_cutoff=kwargs['max_cost'],
        UTurn_policy='allow_dead_ends_and_intersections_only',
        restriction_attribute_name=kwargs['restriction_attributes'],
        hierarchy='no_hierarchy', output_path_shape='true_lines_with_measures'
        )
    # Load facilities.
    facility = {
        'view_name': dataset.create_view(
            unique_name('facility_view'), facility_path,
            dataset_where_sql=kwargs['facility_where_sql'], log_level=None
            ),
        'id_field': dataset.field_metadata(facility_path,
                                           facility_id_field_name),
        }
    arcpy.na.AddFieldToAnalysisLayer(
        in_network_analysis_layer='closest', sub_layer='Facilities',
        field_name='facility_id', field_type=facility['id_field']['type'],
        field_precision=facility['id_field']['precision'],
        field_scale=facility['id_field']['scale'],
        field_length=facility['id_field']['length'],
        field_is_nullable=True
        )
    arcpy.na.AddLocations(
        in_network_analysis_layer='closest', sub_layer='Facilities',
        in_table=facility['view_name'],
        field_mappings='facility_id {} #'.format(facility_id_field_name),
        append=False, exclude_restricted_elements=True
        )
    dataset.delete(facility['view_name'], log_level=None)
    facility['oid_id_map'] = values.oid_field_value_map('closest/Facilities',
                                                        'facility_id')
    # Load dataset locations.
    dataset_info = {
        'view_name': dataset.create_view(
            unique_name('dataset_view'), dataset_path,
            dataset_where_sql=kwargs['dataset_where_sql'], log_level=None
            ),
        'id_field': dataset.field_metadata(dataset_path, id_field_name),
        }
    arcpy.na.AddFieldToAnalysisLayer(
        in_network_analysis_layer='closest', sub_layer='Incidents',
        field_name='dataset_id', field_type=dataset_info['id_field']['type'],
        field_precision=dataset_info['id_field']['precision'],
        field_scale=dataset_info['id_field']['scale'],
        field_length=dataset_info['id_field']['length'],
        field_is_nullable=True
        )
    arcpy.na.AddLocations(
        in_network_analysis_layer='closest', sub_layer='Incidents',
        in_table=dataset_info['view_name'],
        field_mappings='dataset_id {} #'.format(id_field_name),
        append=False, snap_to_position_along_network=False,
        exclude_restricted_elements=True
        )
    dataset.delete(dataset_info['view_name'], log_level=None)
    dataset_info['oid_id_map'] = values.oid_field_value_map(
        'closest/Incidents', 'dataset_id'
        )
    arcpy.na.Solve(in_network_analysis_layer='closest',
                   ignore_invalids=True, terminate_on_solve_error=True)
    toggle_arc_extension('Network', toggle_off=True)
    cursor_field_names = ['FacilityID', 'IncidentID',
                          'total_{}'.format(cost_attribute), 'shape@']
    with arcpy.da.SearchCursor('closest/Routes', cursor_field_names) as cursor:
        for facility_oid, incident_oid, cost, geometry in cursor:
            yield {'dataset_id': dataset_info['oid_id_map'][incident_oid],
                   'facility_id': facility['oid_id_map'][facility_oid],
                   'cost': cost, 'geometry': geometry}
    dataset.delete('closest', log_level=None)
    LOG.log(log_level, "End: Generate.")


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
            ('travel_from_facility', False), ('trim_value', None)
        ]:
        kwargs.setdefault(*kwarg_default)
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Generate service areas for %s.", dataset_path)
    # trim_value assumes meters if not input as linear_unit string.
    if kwargs['trim_value']:
        kwargs['trim_value'] = metadata.linear_unit_as_string(
            kwargs['trim_value'], dataset_path
            )
    dataset_view_name = dataset.create_view(
        unique_name('view'), dataset_path,
        dataset_where_sql=kwargs['dataset_where_sql'], log_level=None
        )
    toggle_arc_extension('Network', toggle_on=True)
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
        hierarchy='no_hierarchy'
        )
    arcpy.na.AddLocations(
        in_network_analysis_layer="service_area", sub_layer="Facilities",
        in_table=dataset_view_name,
        field_mappings='Name {} #'.format(kwargs['id_field_name']),
        search_tolerance=max_distance, match_type='match_to_closest',
        append='clear', snap_to_position_along_network='no_snap',
        exclude_restricted_elements=True
        )
    dataset.delete(dataset_view_name, log_level=None)
    arcpy.na.Solve(in_network_analysis_layer="service_area",
                   ignore_invalids=True, terminate_on_solve_error=True)
    toggle_arc_extension('Network', toggle_off=True)
    dataset.copy('service_area/Polygons', output_path, log_level=None)
    dataset.delete('service_area', log_level=None)
    if kwargs['id_field_name']:
        id_field_meta = dataset.field_metadata(dataset_path,
                                               kwargs['id_field_name'])
        dataset.add_field_from_metadata(output_path, id_field_meta,
                                        log_level=None)
        attributes.update_by_function(
            output_path, kwargs['id_field_name'],
            function=TYPE_ID_FUNCTION_MAP[id_field_meta['type']],
            field_as_first_arg=False, arg_field_names=['Name'], log_level=None
            )
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
    for kwarg_default in [
            ('dataset_where_sql', None), ('detailed_rings', False),
            ('id_field_name', None), ('log_level', 'info'),
            ('overlap_facilities', True), ('restriction_attributes', []),
            ('travel_from_facility', False), ('trim_value', None)
        ]:
        kwargs.setdefault(*kwarg_default)
    log_level = LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Generate service rings for %s.", dataset_path)
    # trim_value assumes meters if not input as linear_unit string.
    if kwargs['trim_value']:
        kwargs['trim_value'] = metadata.linear_unit_as_string(
            kwargs['trim_value'], dataset_path
            )
    dataset_view_name = dataset.create_view(
        unique_name('view'), dataset_path,
        dataset_where_sql=kwargs['dataset_where_sql'], log_level=None
        )
    toggle_arc_extension('Network', toggle_on=True)
    arcpy.na.MakeServiceAreaLayer(
        in_network_dataset=network_path,
        out_network_analysis_layer='service_area',
        impedance_attribute=cost_attribute,
        travel_from_to=('travel_from' if kwargs['travel_from_facility']
                        else 'travel_to'),
        default_break_values=(
            ' '.join(str(x) for x in range(ring_width,
                                           max_distance + 1, ring_width))
            ),
        polygon_type=('detailed_polys' if kwargs['detailed_rings']
                      else 'simple_polys'),
        merge='no_merge' if kwargs['overlap_facilities'] else 'no_overlap',
        nesting_type='rings',
        UTurn_policy='allow_dead_ends_and_intersections_only',
        restriction_attribute_name=kwargs['restriction_attributes'],
        polygon_trim=True if kwargs['trim_value'] else False,
        poly_trim_value=kwargs['trim_value'],
        hierarchy='no_hierarchy'
        )
    arcpy.na.AddLocations(
        in_network_analysis_layer="service_area", sub_layer="Facilities",
        in_table=dataset_view_name,
        field_mappings='Name {} #'.format(kwargs['id_field_name']),
        search_tolerance=max_distance, match_type='match_to_closest',
        append='clear', snap_to_position_along_network='no_snap',
        exclude_restricted_elements=True
        )
    dataset.delete(dataset_view_name, log_level=None)
    arcpy.na.Solve(in_network_analysis_layer="service_area",
                   ignore_invalids=True, terminate_on_solve_error=True)
    toggle_arc_extension('Network', toggle_off=True)
    dataset.copy('service_area/Polygons', output_path, log_level=None)
    dataset.delete('service_area', log_level=None)
    if kwargs['id_field_name']:
        id_field_meta = dataset.field_metadata(dataset_path,
                                               kwargs['id_field_name'])
        dataset.add_field_from_metadata(output_path, id_field_meta,
                                        log_level=None)
        attributes.update_by_function(
            output_path, kwargs['id_field_name'],
            function=TYPE_ID_FUNCTION_MAP[id_field_meta['type']],
            field_as_first_arg=False, arg_field_names=['Name'], log_level=None
            )
    LOG.log(log_level, "End: Generate.")
    return output_path
