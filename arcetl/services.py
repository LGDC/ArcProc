"""ArcGIS Server service operations."""
import logging
import re

import requests

from arcetl.helpers import leveled_logger


LOG = logging.getLogger(__name__)


def generate_token(server_url, username, password, minutes_active=60, **kwargs):
    """Generate a security token for ArcGIS server.

    Args:
        server_url (str): URL of the ArcGIS Server instance.
        username (str): Name of the user requesting the token.
        password (str): Password for the user listed above.
        minutes_active (int): Number of minutes token will be active.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (str): Level to log the function at. Defaults to 'info'.
        referer_url (str): URL of the referring web app.
        requestor_ip (str): IP address of the machine using the token.

    Returns:
        str: The generated token.
    """
    log = leveled_logger(LOG, kwargs.get('log_level', 'info'))
    log("Start: Generate token for %s.", server_url)
    post_url = requests.compat.urljoin(server_url, 'admin/generateToken')
    post_data = {'f': 'json', 'username': username, 'password': password,
                 'expiration': minutes_active}
    if 'referer_url' in kwargs:
        post_data['client'] = 'referer'
        post_data['referer'] = kwargs['referer_url']
    elif 'requestor_ip' in kwargs:
        post_data['client'] = 'ip'
        post_data['ip'] = kwargs['requestor_ip']
    else:
        post_data['client'] = 'requestip'
    token = requests.post(url=post_url, data=post_data).json()['token']
    log("Token = %s.", token)
    log("End: Generate.")
    return token


def toggle_service(service_url, token, start_service=False, stop_service=False,
                   **kwargs):
    """Toggle service to start or stop.

    Args:
        service_url (str): URL for the service endpoint.
        token (str): Security token for REST admininstration.
        start_service (bool): Flag to start service.
        stop_service (bool): Flag to stop service. This will only be used if
            start_service is not flagged.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        str: URL for the toggled service.

    Raises:
        requests.HTTPError: An error in the HTTP request occurred.
    """
    if start_service:
        toggle = 'start'
    elif stop_service:
        toggle = 'stop'
    else:
        raise ValueError("start_service or stop_service must be True")
    log = leveled_logger(LOG, kwargs.get('log_level', 'info'))
    log("Start: Toggle-%s service %s.", toggle, service_url)
    url_parts = service_url.split('/')
    post_url = re.sub(
        '/arcgis/rest/services/', '/arcgis/admin/services/',
        '/'.join(url_parts[:-1]) + '.{}/{}'.format(url_parts[-1], toggle),
        flags=re.I
        )
    post_data = {'f': 'json', 'token': token}
    response = requests.post(url=post_url, data=post_data)
    response.raise_for_status()
    log("End: Toggle.")
    return service_url
