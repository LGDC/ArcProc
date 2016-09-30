# -*- coding=utf-8 -*-
"""Network analysis operations."""
import logging

import arcpy

from . import helpers, metadata, values
from arcetl import dataset


LOG = logging.getLogger(__name__)


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
            ('restriction_attributes', []), ('travel_from_facility', False)]:
        kwargs.setdefault(*kwarg_default)
    log_level = helpers.LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(
        log_level, "Start: Generate closest facility in %s to locations in %s.",
        facility_path, dataset_path)
    helpers.toggle_arc_extension('Network', toggle_on=True)
    # pylint: disable=no-member
    arcpy.na.MakeClosestFacilityLayer(
        # pylint: enable=no-member
        in_network_dataset=network_path, out_network_analysis_layer='closest',
        impedance_attribute=cost_attribute,
        travel_from_to=('travel_from' if kwargs['travel_from_facility']
                        else 'travel_to'),
        default_cutoff=kwargs['max_cost'],
        UTurn_policy='allow_dead_ends_and_intersections_only',
        restriction_attribute_name=kwargs['restriction_attributes'],
        hierarchy='no_hierarchy', output_path_shape='true_lines_with_measures')
    # Load facilities.
    facility = {
        'view_name': dataset.create_view(
            helpers.unique_name('facility_view'), facility_path,
            dataset_where_sql=kwargs['facility_where_sql'], log_level=None),
        'id_field': metadata.field_metadata(
            facility_path, facility_id_field_name)}
    arcpy.na.AddFieldToAnalysisLayer(
        in_network_analysis_layer='closest', sub_layer='Facilities',
        field_name='facility_id', field_type=facility['id_field']['type'],
        field_precision=facility['id_field']['precision'],
        field_scale=facility['id_field']['scale'],
        field_length=facility['id_field']['length'],
        field_is_nullable=True)
    arcpy.na.AddLocations(
        in_network_analysis_layer='closest', sub_layer='Facilities',
        in_table=facility['view_name'],
        field_mappings='facility_id {} #'.format(facility_id_field_name),
        append=False, exclude_restricted_elements=True)
    dataset.delete(facility['view_name'], log_level=None)
    facility['oid_id_map'] = values.oid_field_value_map(
        'closest/Facilities', 'facility_id')
    # Load dataset locations.
    dataset_info = {
        'view_name': dataset.create_view(
            helpers.unique_name('dataset_view'), dataset_path,
            dataset_where_sql=kwargs['dataset_where_sql'], log_level=None),
        'id_field': metadata.field_metadata(dataset_path, id_field_name)
        }
    arcpy.na.AddFieldToAnalysisLayer(
        in_network_analysis_layer='closest', sub_layer='Incidents',
        field_name='dataset_id', field_type=dataset_info['id_field']['type'],
        field_precision=dataset_info['id_field']['precision'],
        field_scale=dataset_info['id_field']['scale'],
        field_length=dataset_info['id_field']['length'],
        field_is_nullable=True)
    arcpy.na.AddLocations(
        in_network_analysis_layer='closest', sub_layer='Incidents',
        in_table=dataset_info['view_name'],
        field_mappings='dataset_id {} #'.format(id_field_name),
        append=False, snap_to_position_along_network=False,
        exclude_restricted_elements=True)
    dataset.delete(dataset_info['view_name'], log_level=None)
    dataset_info['oid_id_map'] = values.oid_field_value_map(
        'closest/Incidents', 'dataset_id')
    arcpy.na.Solve(in_network_analysis_layer='closest',
                   ignore_invalids=True, terminate_on_solve_error=True)
    helpers.toggle_arc_extension('Network', toggle_off=True)
    cursor_field_names = ['FacilityID', 'IncidentID',
                          'total_{}'.format(cost_attribute), 'shape@']
    with arcpy.da.SearchCursor('closest/Routes', cursor_field_names) as cursor:
        for facility_oid, incident_oid, cost, geometry in cursor:
            yield {
                'dataset_id': dataset_info['oid_id_map'][incident_oid],
                'facility_id': facility['oid_id_map'][facility_oid],
                'cost': cost, 'geometry': geometry}
    dataset.delete('closest', log_level=None)
    LOG.log(log_level, "End: Generate.")
