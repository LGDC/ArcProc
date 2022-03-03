"""ArcGIS Server service operations."""
import logging
from typing import Iterable, Iterator, Optional

import arcgis

import arcpy

from arcproc.metadata import SpatialReference, SpatialReferenceSourceItem


LOG: logging.Logger = logging.getLogger(__name__)
"""Module-level logger."""

arcpy.SetLogHistory(False)


def as_dicts(
    url: str,
    field_names: Optional[Iterable[str]] = None,
    service_where_sql: Optional[str] = None,
    include_geometry: bool = True,
    spatial_reference_item: SpatialReferenceSourceItem = None,
) -> Iterator[dict]:
    """Generate mappings of feature attribute name to value.

    Notes:
        Use ArcPy cursor token names for object IDs and geometry objects/properties.

    Args:
        url: URL for the service endpoint.
        field_names: Collection of field names to include in dictionary. If set to None,
            all fields will be included. Do not include geometry field; use
            `include_geometry` to have added to dictionary.
        include_geometry: Add geometry attribute to dictionary under "SHAPE@" key if
            True.
        service_where_sql: SQL where-clause for service subselection.
        spatial_reference_item: Item from which the spatial reference of the output
            geometry will be derived. If set to None, will use spatial reference of the
            service.
    """
    # `spatial_reference_item = None` will return instance with wkid being None.
    wkid = SpatialReference(spatial_reference_item).wkid
    feature_layer = arcgis.features.FeatureLayer(url)
    feature_set = feature_layer.query(
        where=service_where_sql if service_where_sql else "1=1",
        out_fields="*" if field_names is None else list(field_names),
        out_sr=wkid,
    )
    for feature in feature_set.features:
        feature_dict = feature.attributes
        if include_geometry:
            if "spatialReference" not in feature.geometry:
                feature.geometry["spatialReference"] = {"wkid": wkid}
            feature_dict["SHAPE@"] = arcpy.AsShape(feature.geometry, esri_json=True)
        yield feature_dict
