"""Network analysis operations.

Attributes:
    TYPE_ID_FUNCTION_MAP (dict): Mapping of ArcGIS field type name to
        a function that will extract an ID value from the label of a network
        analysis layer.

"""
import logging

import arcpy

from arcetl import arcobj
from arcetl import attributes
from arcetl import dataset
from arcetl import helpers
from arcetl import workspace


LOG = logging.getLogger(__name__)

TYPE_ID_FUNCTION_MAP = {
    'short': (lambda x: int(x.split(' : ')[0]) if x else None),
    'long': (lambda x: int(x.split(' : ')[0]) if x else None),
    'double': (lambda x: float(x.split(' : ')[0]) if x else None),
    'single': (lambda x: float(x.split(' : ')[0]) if x else None),
    'string': (lambda x: x.split(' : ')[0] if x else None)
    }


build = workspace.build_network  # pylint: disable=invalid-name


# pylint: disable=too-many-arguments
def closest_facility_route(dataset_path, id_field_name, facility_path,
                           facility_id_field_name, network_path, cost_attribute,
                           **kwargs):
    """Generate route info dictionaries for dataset features's closest facility.

    Args:
        dataset_path (str): The path of the dataset.
        id_field_name (str): The name of the dataset ID field.
        facility_path (str): The path of the facilities dataset.
        facility_id_field_name (str): The name of the facility ID field.
        network_path (str): The path of the network dataset.
        cost_attribute (str): The name of the network cost attribute to use.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): The SQL where-clause for dataset subselection.
        facility_where_sql (str): The SQL where-clause for facility
            subselection.
        log_level (str): The level to log the function at.
        max_cost (float): The maximum travel cost the search will attempt, in
            the cost attribute's units.
        restriction_attributes (iter): An iterable of network restriction
            attribute names to use.
        travel_from_facility (bool): The flag to indicate performing the
            analysis travelling from (True) or to (False) the facility.

    Yields:
        dict: The next feature's analysis result details.
            Dictionary keys: 'dataset_id', 'facility_id', 'cost', 'geometry',
            'cost' value (float) will match units of the cost_attribute.
            'geometry' (arcpy.Geometry) will match spatial refernece to the
            dataset.
    """
    log_level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(log_level,
            "Start: Generate closest facility in %s to locations in %s.",
            facility_path, dataset_path)
    with arcobj.ArcExtension('Network'):
        arcpy.na.MakeClosestFacilityLayer(
            in_network_dataset=network_path,
            out_network_analysis_layer='closest',
            impedance_attribute=cost_attribute,
            travel_from_to=('travel_from' if kwargs.get('travel_from_facility')
                            else 'travel_to'),
            default_cutoff=kwargs.get('max_cost'),
            UTurn_policy='allow_dead_ends_and_intersections_only',
            restriction_attribute_name=kwargs.get('restriction_attributes'),
            hierarchy='no_hierarchy',
            output_path_shape='true_lines_with_measures'
            )
        # Load facilities.
        facility_info = {
            'view_name': dataset.create_view(
                helpers.unique_name('facility_view'), facility_path,
                dataset_where_sql=kwargs.get('facility_where_sql'),
                log_level=None
                ),
            'id_field': arcobj.field_metadata(facility_path,
                                              facility_id_field_name),
            }
        arcpy.na.AddFieldToAnalysisLayer(
            in_network_analysis_layer='closest', sub_layer='Facilities',
            field_name='facility_id',
            field_type=facility_info['id_field']['type'],
            field_precision=facility_info['id_field']['precision'],
            field_scale=facility_info['id_field']['scale'],
            field_length=facility_info['id_field']['length'],
            field_is_nullable=True
            )
        arcpy.na.AddLocations(
            in_network_analysis_layer='closest', sub_layer='Facilities',
            in_table=facility_info['view_name'],
            field_mappings='facility_id {} #'.format(facility_id_field_name),
            append=False, exclude_restricted_elements=True
            )
        dataset.delete(facility_info['view_name'], log_level=None)
        facility_info['oid_id_map'] = attributes.id_map('closest/Facilities',
                                                        'facility_id')
        # Load dataset locations.
        dataset_info = {
            'view_name': dataset.create_view(
                helpers.unique_name('dataset_view'), dataset_path,
                dataset_where_sql=kwargs.get('dataset_where_sql'),
                log_level=None
                ),
            'id_field': arcobj.field_metadata(dataset_path, id_field_name),
            }
        arcpy.na.AddFieldToAnalysisLayer(
            in_network_analysis_layer='closest', sub_layer='Incidents',
            field_name='dataset_id',
            field_type=dataset_info['id_field']['type'],
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
        dataset_info['oid_id_map'] = attributes.id_map('closest/Incidents',
                                                       'dataset_id')
        arcpy.na.Solve(in_network_analysis_layer='closest',
                       ignore_invalids=True, terminate_on_solve_error=True)
    cursor_field_names = ('FacilityID', 'IncidentID',
                          'total_{}'.format(cost_attribute), 'shape@')
    with arcpy.da.SearchCursor('closest/Routes', cursor_field_names) as cursor:
        for facility_oid, incident_oid, cost, geometry in cursor:
            closest_info = {
                'dataset_id': dataset_info['oid_id_map'][incident_oid],
                'facility_id': facility_info['oid_id_map'][facility_oid],
                'cost': cost, 'geometry': geometry,
                }
            yield closest_info
    dataset.delete('closest', log_level=None)
    LOG.log(log_level, "End: Generate.")
# pylint: enable=too-many-arguments


def generate_service_areas(dataset_path, output_path, network_path,
                           cost_attribute, max_distance, **kwargs):
    """Create network service area features.

    Args:
        dataset_path (str): The path of the dataset.
        output_path (str): The path of the output service areas dataset.
        network_path (str): The path of the network dataset.
        cost_attribute (str): The name of the network cost attribute to use.
        max_distance (float): The distance in travel from the facility the outer
            ring will extend to, in the dataset's units.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): The SQL where-clause for dataset subselection.
        detailed_features (bool): The flag to generate high-detail features.
        id_field_name (str): The name of facility ID field.
        log_level (str): The level to log the function at.
        overlap_facilities (bool): The flag to overlap different facility
            service areas.
        restriction_attributes (iter): An iterable of network restriction
            attribute names to use.
        travel_from_facility (bool): The flag to indicate performing the
            analysis travelling from (True) or to (False) the facility.
        trim_value (float): The distance from the network features to trim
            service areas at.

    Returns:
        str: The path of the output service areas dataset.
    """
    log_level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(log_level, "Start: Generate service areas for %s.", dataset_path)
    # trim_value assumes meters if not input as linear_unit string.
    if kwargs.get('trim_value') is not None:
        trim_value = arcobj.linear_unit_string(kwargs['trim_value'],
                                               dataset_path)
    else:
        trim_value = None
    dataset_view_name = dataset.create_view(
        helpers.unique_name('view'), dataset_path,
        dataset_where_sql=kwargs.get('dataset_where_sql'), log_level=None
        )
    with arcobj.ArcExtension('Network'):
        arcpy.na.MakeServiceAreaLayer(
            in_network_dataset=network_path,
            out_network_analysis_layer='service_area',
            impedance_attribute=cost_attribute,
            travel_from_to=('travel_from' if kwargs.get('travel_from_facility')
                            else 'travel_to'),
            default_break_values='{}'.format(max_distance),
            polygon_type=('detailed_polys' if kwargs.get('detailed_features')
                          else 'simple_polys'),
            merge=('no_merge' if kwargs.get('overlap_facilities', True)
                   else 'no_overlap'),
            nesting_type='disks',
            UTurn_policy='allow_dead_ends_and_intersections_only',
            restriction_attribute_name=kwargs.get('restriction_attributes'),
            polygon_trim=True if trim_value else False,
            poly_trim_value=trim_value,
            hierarchy='no_hierarchy'
            )
        arcpy.na.AddLocations(
            in_network_analysis_layer="service_area", sub_layer="Facilities",
            in_table=dataset_view_name,
            field_mappings='Name {} #'.format(kwargs.get('id_field_name')),
            search_tolerance=max_distance, match_type='match_to_closest',
            append='clear', snap_to_position_along_network='no_snap',
            exclude_restricted_elements=True
            )
        dataset.delete(dataset_view_name, log_level=None)
        arcpy.na.Solve(in_network_analysis_layer="service_area",
                       ignore_invalids=True, terminate_on_solve_error=True)
    dataset.copy('service_area/Polygons', output_path, log_level=None)
    dataset.delete('service_area', log_level=None)
    if kwargs.get('id_field_name'):
        id_field_meta = arcobj.field_metadata(dataset_path,
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


# pylint: disable=too-many-arguments
def generate_service_rings(dataset_path, output_path, network_path,
                           cost_attribute, ring_width, max_distance, **kwargs):
    """Create facility service ring features using a network dataset.

    Args:
        dataset_path (str): The path of the dataset.
        output_path (str): The path of the output service rings dataset.
        network_path (str): The path of the network dataset.
        cost_attribute (str): The name of the network cost attribute to use.
        ring_width (float): The distance a service ring represents in travel, in
            the dataset's units.
        max_distance (float): The distance in travel from the facility the outer
            ring will extend to, in the dataset's units.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): The SQL where-clause for dataset subselection.
        detailed_features (bool): The flag to generate high-detail features.
        id_field_name (str): The name of facility ID field.
        log_level (str): The level to log the function at.
        overlap_facilities (bool): The flag to overlap different facility
            service areas.
        restriction_attributes (iter): An iterable of network restriction
            attribute names to use.
        travel_from_facility (bool): The flag to indicate performing the
            analysis travelling from (True) or to (False) the facility.
        trim_value (float): The distance from the network features to trim
            service areas at.

    Returns:
        str: The path of the output service rings dataset.
    """
    log_level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(log_level, "Start: Generate service rings for %s.", dataset_path)
    # trim_value assumes meters if not input as linear_unit string.
    if kwargs.get('trim_value') is not None:
        trim_value = arcobj.linear_unit_string(kwargs['trim_value'],
                                               dataset_path)
    else:
        trim_value = None
    dataset_view_name = dataset.create_view(
        helpers.unique_name('view'), dataset_path,
        dataset_where_sql=kwargs.get('dataset_where_sql'), log_level=None
        )
    with arcobj.ArcExtension('Network'):
        arcpy.na.MakeServiceAreaLayer(
            in_network_dataset=network_path,
            out_network_analysis_layer='service_area',
            impedance_attribute=cost_attribute,
            travel_from_to=('travel_from' if kwargs.get('travel_from_facility')
                            else 'travel_to'),
            default_break_values=(
                ' '.join(str(x) for x in range(ring_width,
                                               max_distance + 1, ring_width))
                ),
            polygon_type=('detailed_polys' if kwargs.get('detailed_features')
                          else 'simple_polys'),
            merge=('no_merge' if kwargs.get('overlap_facilities', True)
                   else 'no_overlap'),
            nesting_type='rings',
            UTurn_policy='allow_dead_ends_and_intersections_only',
            restriction_attribute_name=kwargs.get('restriction_attributes'),
            polygon_trim=True if trim_value else False,
            poly_trim_value=trim_value,
            hierarchy='no_hierarchy'
            )
        arcpy.na.AddLocations(
            in_network_analysis_layer="service_area", sub_layer="Facilities",
            in_table=dataset_view_name,
            field_mappings='Name {} #'.format(kwargs.get('id_field_name')),
            search_tolerance=max_distance, match_type='match_to_closest',
            append='clear', snap_to_position_along_network='no_snap',
            exclude_restricted_elements=True
            )
        dataset.delete(dataset_view_name, log_level=None)
        arcpy.na.Solve(in_network_analysis_layer="service_area",
                       ignore_invalids=True, terminate_on_solve_error=True)
    dataset.copy('service_area/Polygons', output_path, log_level=None)
    dataset.delete('service_area', log_level=None)
    if kwargs.get('id_field_name'):
        id_field_meta = arcobj.field_metadata(dataset_path,
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
# pylint: enable=too-many-arguments
