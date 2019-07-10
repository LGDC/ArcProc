"""Network analysis operations."""
import logging

import arcpy

from arcetl import arcobj
from arcetl.arcobj import ArcExtension
from arcetl import attributes
from arcetl import dataset
from arcetl.helpers import leveled_logger
from arcetl import workspace


LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""

TYPE_ID_FUNCTION_MAP = {
    'short': (lambda x: int(x.split(' : ')[0]) if x else None),
    'long': (lambda x: int(x.split(' : ')[0]) if x else None),
    'double': (lambda x: float(x.split(' : ')[0]) if x else None),
    'single': (lambda x: float(x.split(' : ')[0]) if x else None),
    'string': (lambda x: x.split(' : ')[0] if x else None),
    'text': (lambda x: x.split(' : ')[0] if x else None),
}
"""dict: Mapping of ArcGIS field type to function to get ID from analysis layer label."""

arcpy.SetLogHistory(False)


build = workspace.build_network  # pylint: disable=invalid-name


@ArcExtension("Network")
def closest_facility_route(
    dataset_path,
    id_field_name,
    facility_path,
    facility_id_field_name,
    network_path,
    cost_attribute,
    **kwargs
):
    """Generate route info dictionaries for dataset features's closest facility.

    Args:
        dataset_path (str): Path of the dataset.
        id_field_name (str): Name of the dataset ID field.
        facility_path (str): Path of the facilities dataset.
        facility_id_field_name (str): Name of the facility ID field.
        network_path (str): Path of the network dataset.
        cost_attribute (str): Name of the network cost attribute to use.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        facility_where_sql (str): SQL where-clause for facility subselection.
        max_cost (float): Maximum travel cost the search will attempt, in the cost
            attribute's units.
        restriction_attributes (iter): Collection of network restriction attribute
            names to use.
        travel_from_facility (bool): Flag to indicate performing the analysis
            travelling from (True) or to (False) the facility. Default is False.
        log_level (str): Level to log the function at. Default is 'info'.

    Yields:
        dict: The next feature's analysis result details.
            Dictionary keys: 'dataset_id', 'facility_id', 'cost', 'geometry',
            'cost' value (float) will match units of the cost_attribute.
            'geometry' (arcpy.Geometry) will match spatial reference to the dataset.

    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('facility_where_sql')
    kwargs.setdefault('max_cost')
    kwargs.setdefault('restriction_attributes')
    kwargs.setdefault('travel_from_facility', False)
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    log(
        "Start: Generate closest facility in %s to locations in %s.",
        facility_path,
        dataset_path,
    )
    meta = {
        'id_field': {
            'dataset': arcobj.field_metadata(dataset_path, id_field_name),
            'facility': arcobj.field_metadata(facility_path, facility_id_field_name),
        }
    }
    keys = {
        'cursor': [
            'FacilityID', 'IncidentID', 'total_{}'.format(cost_attribute), 'shape@'
        ]
    }
    view = {
        'dataset': arcobj.DatasetView(dataset_path, kwargs['dataset_where_sql']),
        'facility': arcobj.DatasetView(facility_path, kwargs['facility_where_sql']),
    }
    arcpy.na.MakeClosestFacilityLayer(
        in_network_dataset=network_path,
        out_network_analysis_layer='closest',
        impedance_attribute=cost_attribute,
        travel_from_to=(
            'travel_from' if kwargs['travel_from_facility'] else 'travel_to'
        ),
        default_cutoff=kwargs['max_cost'],
        UTurn_policy='allow_dead_ends_and_intersections_only',
        restriction_attribute_name=kwargs['restriction_attributes'],
        hierarchy='no_hierarchy',
        output_path_shape='true_lines_with_measures',
    )
    # Load facilities.
    with view['facility']:
        arcpy.na.AddFieldToAnalysisLayer(
            in_network_analysis_layer='closest',
            sub_layer='Facilities',
            field_name='facility_id',
            field_type=meta['id_field']['facility']['type'],
            field_precision=meta['id_field']['facility']['precision'],
            field_scale=meta['id_field']['facility']['scale'],
            field_length=meta['id_field']['facility']['length'],
            field_is_nullable=True,
        )
        arcpy.na.AddLocations(
            in_network_analysis_layer='closest',
            sub_layer='Facilities',
            in_table=view['facility'].name,
            field_mappings='facility_id {} #'.format(facility_id_field_name),
            append=False,
            exclude_restricted_elements=True,
        )
    facility_oid_id = attributes.id_values_map(
        'closest/Facilities', id_field_names='oid@', field_names='facility_id'
    )
    # Load dataset locations.
    with view['dataset']:
        arcpy.na.AddFieldToAnalysisLayer(
            in_network_analysis_layer='closest',
            sub_layer='Incidents',
            field_name='dataset_id',
            field_type=meta['id_field']['dataset']['type'],
            field_precision=meta['id_field']['dataset']['precision'],
            field_scale=meta['id_field']['dataset']['scale'],
            field_length=meta['id_field']['dataset']['length'],
            field_is_nullable=True,
        )
        arcpy.na.AddLocations(
            in_network_analysis_layer='closest',
            sub_layer='Incidents',
            in_table=view['dataset'].name,
            field_mappings='dataset_id {} #'.format(id_field_name),
            append=False,
            snap_to_position_along_network=False,
            exclude_restricted_elements=True,
        )
    dataset_oid_id = attributes.id_values_map(
        'closest/Incidents', id_field_names='oid@', field_names='dataset_id'
    )
    arcpy.na.Solve(
        in_network_analysis_layer='closest',
        ignore_invalids=True,
        terminate_on_solve_error=True,
    )
    cursor = arcpy.da.SearchCursor('closest/Routes', field_names=keys['cursor'])
    with cursor:
        for row in cursor:
            feat = dict(zip(keys['cursor'], row))
            yield {
                'dataset_id': dataset_oid_id[feat['IncidentID']],
                'facility_id': facility_oid_id[feat['FacilityID']],
                'cost': feat['total_' + cost_attribute],
                'geometry': feat['shape@'],
            }

    dataset.delete('closest', log_level=None)
    log("End: Generate.")


@ArcExtension("Network")
def generate_service_areas(
    dataset_path, output_path, network_path, cost_attribute, max_distance, **kwargs
):
    """Create network service area features.

    Args:
        dataset_path (str): Path of the dataset.
        output_path (str): Path of the output service areas dataset.
        network_path (str): Path of the network dataset.
        cost_attribute (str): Name of the network cost attribute to use.
        max_distance (float): Distance in travel from the facility the outer ring will
            extend to, in the dataset's units.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        id_field_name (str): Name of facility ID field.
        restriction_attributes (iter): Collection of network restriction attribute
            names to use.
        travel_from_facility (bool): Flag to indicate performing the analysis
            travelling from (True) or to (False) the facility. Default is False.
        detailed_features (bool): Flag to generate high-detail features. Default is
            False.
        overlap_facilities (bool): Flag to overlap different facility service areas.
            Default is True.
        trim_value (float): Dstance from the network features to trim service areas at.
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        str: Path of the output service areas dataset.
    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('id_field_name')
    kwargs.setdefault('restriction_attributes')
    kwargs.setdefault('travel_from_facility', False)
    kwargs.setdefault('detailed_features', False)
    kwargs.setdefault('overlap_facilities', True)
    kwargs.setdefault('trim_value')
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    log("Start: Generate service areas for %s.", dataset_path)
    # trim_value assumes meters if not input as linear_unit string.
    if kwargs['trim_value'] is not None:
        trim_value = arcobj.linear_unit_string(kwargs['trim_value'], dataset_path)
    else:
        trim_value = None
    view = {'dataset': arcobj.DatasetView(dataset_path, kwargs['dataset_where_sql'])}
    arcpy.na.MakeServiceAreaLayer(
        in_network_dataset=network_path,
        out_network_analysis_layer='service_area',
        impedance_attribute=cost_attribute,
        travel_from_to=(
            'travel_from' if kwargs['travel_from_facility'] else 'travel_to'
        ),
        default_break_values='{}'.format(max_distance),
        polygon_type=(
            'detailed_polys' if kwargs['detailed_features'] else 'simple_polys'
        ),
        merge=('no_merge' if kwargs['overlap_facilities'] else 'no_overlap'),
        nesting_type='disks',
        UTurn_policy='allow_dead_ends_and_intersections_only',
        restriction_attribute_name=kwargs['restriction_attributes'],
        polygon_trim=(True if trim_value else False),
        poly_trim_value=trim_value,
        hierarchy='no_hierarchy',
    )
    with view['dataset']:
        arcpy.na.AddLocations(
            in_network_analysis_layer="service_area",
            sub_layer="Facilities",
            in_table=view['dataset'].name,
            field_mappings='Name {} #'.format(kwargs['id_field_name']),
            search_tolerance=max_distance,
            match_type='match_to_closest',
            append='clear',
            snap_to_position_along_network='no_snap',
            exclude_restricted_elements=True,
        )
    arcpy.na.Solve(
        in_network_analysis_layer="service_area",
        ignore_invalids=True,
        terminate_on_solve_error=True,
    )
    dataset.copy('service_area/Polygons', output_path, log_level=None)
    dataset.delete('service_area', log_level=None)
    if kwargs['id_field_name']:
        meta = {
            'id_field': arcobj.field_metadata(dataset_path, kwargs['id_field_name'])
        }
        dataset.add_field_from_metadata(output_path, meta['id_field'], log_level=None)
        attributes.update_by_function(
            output_path,
            field_name=meta['id_field']['name'],
            function=TYPE_ID_FUNCTION_MAP[meta['id_field']['type']],
            field_as_first_arg=False,
            arg_field_names=['Name'],
            log_level=None,
        )
    log("End: Generate.")
    return output_path


@ArcExtension("Network")
def generate_service_rings(
    dataset_path,
    output_path,
    network_path,
    cost_attribute,
    ring_width,
    max_distance,
    **kwargs
):
    """Create facility service ring features using a network dataset.

    Args:
        dataset_path (str): Path of the dataset.
        output_path (str): Path of the output service rings dataset.
        network_path (str): Path of the network dataset.
        cost_attribute (str): Name of the network cost attribute to use.
        ring_width (float): Distance a service ring represents in travel, in the
            dataset's units.
        max_distance (float): Distance in travel from the facility the outer ring will
            extend to, in the dataset's units.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        id_field_name (str): Name of facility ID field.
        restriction_attributes (iter): Collection of network restriction attribute
            names to use.
        travel_from_facility (bool): Flag to indicate performing the analysis
            travelling from (True) or to (False) the facility. Default is False.
        detailed_features (bool): Flag to generate high-detail features. Default is
            False.
        overlap_facilities (bool): Flag to overlap different facility service areas.
            Default is True.
        trim_value (float): Dstance from the network features to trim service areas at.
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        str: Path of the output service rings dataset.

    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('id_field_name')
    kwargs.setdefault('restriction_attributes')
    kwargs.setdefault('travel_from_facility', False)
    kwargs.setdefault('detailed_features', False)
    kwargs.setdefault('overlap_facilities', True)
    kwargs.setdefault('trim_value')
    log = leveled_logger(LOG, kwargs.setdefault('log_level', 'info'))
    log("Start: Generate service rings for %s.", dataset_path)
    # trim_value assumes meters if not input as linear_unit string.
    if kwargs['trim_value'] is not None:
        trim_value = arcobj.linear_unit_string(kwargs['trim_value'], dataset_path)
    else:
        trim_value = None
    view = {'dataset': arcobj.DatasetView(dataset_path, kwargs['dataset_where_sql'])}
    arcpy.na.MakeServiceAreaLayer(
        in_network_dataset=network_path,
        out_network_analysis_layer='service_area',
        impedance_attribute=cost_attribute,
        travel_from_to=(
            'travel_from' if kwargs['travel_from_facility'] else 'travel_to'
        ),
        default_break_values=(
            ' '.join(
                str(x) for x in range(ring_width, max_distance + 1, ring_width)
            )
        ),
        polygon_type=(
            'detailed_polys' if kwargs['detailed_features'] else 'simple_polys'
        ),
        merge=('no_merge' if kwargs['overlap_facilities'] else 'no_overlap'),
        nesting_type='rings',
        UTurn_policy='allow_dead_ends_and_intersections_only',
        restriction_attribute_name=kwargs['restriction_attributes'],
        polygon_trim=(True if trim_value else False),
        poly_trim_value=trim_value,
        hierarchy='no_hierarchy',
    )
    with view['dataset']:
        arcpy.na.AddLocations(
            in_network_analysis_layer="service_area",
            sub_layer="Facilities",
            in_table=view['dataset'].name,
            field_mappings='Name {} #'.format(kwargs['id_field_name']),
            search_tolerance=max_distance,
            match_type='match_to_closest',
            append='clear',
            snap_to_position_along_network='no_snap',
            exclude_restricted_elements=True,
        )
    arcpy.na.Solve(
        in_network_analysis_layer="service_area",
        ignore_invalids=True,
        terminate_on_solve_error=True,
    )
    dataset.copy('service_area/Polygons', output_path, log_level=None)
    dataset.delete('service_area', log_level=None)
    if kwargs['id_field_name']:
        meta = {
            'id_field': arcobj.field_metadata(dataset_path, kwargs['id_field_name'])
        }
        dataset.add_field_from_metadata(output_path, meta['id_field'], log_level=None)
        attributes.update_by_function(
            output_path,
            meta['id_field']['name'],
            function=TYPE_ID_FUNCTION_MAP[meta['id_field']['type']],
            field_as_first_arg=False,
            arg_field_names=['Name'],
            log_level=None,
        )
    log("End: Generate.")
    return output_path
