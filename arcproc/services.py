"""ArcGIS Server service operations."""
import logging
import re

import arcgis
import requests

import arcpy

from arcproc.arcobj import spatial_reference
from arcproc.helpers import contain


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
    if kwargs.get("spatial_reference_item"):
        wkid = spatial_reference(kwargs["spatial_reference_item"]).factoryCode
    else:
        wkid = None
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


def generate_token(server_url, username, password, minutes_active=60, **kwargs):
    """Generate a security token for ArcGIS server.

    Args:
        server_url (str): URL of the ArcGIS Server instance.
        username (str): Name of the user requesting the token.
        password (str): Password for the user listed above.
        minutes_active (int): Number of minutes token will be active.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        referer_url (str): URL of the referring web app.
        requestor_ip (str): IP address of the machine using the token.
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        str: The generated token.
    """
    level = kwargs.get("log_level", logging.INFO)
    LOG.log(level, "Start: Generate token for `%s`.", server_url)
    post_url = requests.compat.urljoin(server_url, "admin/generateToken")
    post_data = {
        "f": "json",
        "username": username,
        "password": password,
        "expiration": minutes_active,
    }
    if "referer_url" in kwargs:
        post_data.update({"client": "referer", "referer": kwargs["referer_url"]})
    elif "requestor_ip" in kwargs:
        post_data.update({"client": "ip", "referer": kwargs["requestor_ip"]})
    else:
        post_data["client"] = "requestip"
    token = requests.post(url=post_url, data=post_data).json()["token"]
    LOG.log(level, """Token = "%s".""", token)
    LOG.log(level, "End: Generate.")
    return token


def toggle_service(
    service_url, token, start_service=False, stop_service=False, **kwargs
):
    """Toggle service to start or stop.

    Args:
        service_url (str): URL for the service endpoint.
        token (str): Security token for REST admininstration.
        start_service (bool): Flag to start service.
        stop_service (bool): Flag to stop service. This will only be used if
            start_service is not flagged.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).

    Returns:
        str: URL for the toggled service.

    Raises:
        requests.HTTPError: An error in the HTTP request occurred.
    """
    if start_service:
        toggle = "start"
    elif stop_service:
        toggle = "stop"
    else:
        raise ValueError(""""start_service" or "stop_service" must be True.""")

    level = kwargs.get("log_level", logging.INFO)
    LOG.log(level, "Start: Toggle-%s service `%s`.", toggle, service_url)
    url_parts = service_url.split("/")
    post_url = re.sub(
        "/arcgis/rest/services/",
        "/arcgis/admin/services/",
        "/".join(url_parts[:-1]) + ".{}/{}".format(url_parts[-1], toggle),
        flags=re.I,
    )
    post_data = {"f": "json", "token": token}
    response = requests.post(url=post_url, data=post_data)
    response.raise_for_status()
    LOG.log(level, "End: Toggle.")
    return service_url
