"""ArcGIS Server service operations."""
import logging

import arcgis

import arcpy

from arcproc.helpers import contain
from arcproc.metadata import SpatialReference


LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""


def as_dicts(url, field_names=None, **kwargs):
    """Generate mappings of feature attribute name to value.

    Notes:
        Use ArcPy cursor token names for object IDs and geometry objects/properties.

    Args:
        url (str): URL for the service endpoint.
        field_names (iter): Collection of field names. Names will be the keys in the
            dictionary mapping to their values. If value is None, all attributes fields
            will be used.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        service_where_sql (str): SQL where-clause for service feature subselection.
        spatial_reference_item: Item from which the spatial reference of the output
            geometry will be derived.

    Yields:
        dict
    """
    if field_names is None:
        field_names = "*"
    else:
        field_names = list(contain(field_names))
    wkid = SpatialReference(kwargs.get("spatial_reference_item")).wkid
    feature_layer = arcgis.features.FeatureLayer(url)
    feature_set = feature_layer.query(
        where=kwargs.get("service_where_sql", "1=1"),
        out_fields=field_names,
        out_sr=wkid,
    )
    for feature in feature_set.features:
        feature_dict = feature.attributes
        if "spatialReference" not in feature.geometry:
            feature.geometry["spatialReference"] = {"wkid": wkid}
        feature_dict["SHAPE@"] = arcpy.AsShape(feature.geometry, esri_json=True)
        yield feature_dict
