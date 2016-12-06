"""ArcGIS Server service operations."""
import logging
import re

import requests

from arcetl import helpers


LOG = logging.getLogger(__name__)


def generate_token(server_url, username, password, minutes_active=60, **kwargs):
    """Generate a security token for ArcGIS server.

    Args:
        server_url (str): The URL of the ArcGIS Server instance.
        username (str): The name of the user requesting the token.
        password (str): The password for the user listed above.
        minutes_active (int): The number of minutes token will be active.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (str): The level to log the function at.
        referer_url (str): The URL of the referring web app.
        requestor_ip (str): The IP address of the machine using the token.

    Returns:
        str: The generated token.
    """
    log_level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(log_level, "Start: Generate token for %s.", server_url)
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
    LOG.log(log_level, "Token = %s.", token)
    LOG.log(log_level, "End: Generate.")
    return token


def toggle_service(service_url, token, start_service=False, stop_service=False,
                   **kwargs):
    """Toggle service to start or stop.

    Args:
        service_url (str): The URL for the service endpoint.
        token (str): The security token for REST admininstration.
        start_service (bool): The flag to start service.
        stop_service (bool): The flag to stop service.
            This will only be used if start_service is not flagged.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        log_level (str): The level to log the function at.

    Returns:
        str: The URL for the toggled service.

    Raises:
        requests.HTTPError: An error in the HTTP request occurred.
    """
    if start_service:
        toggle = 'start'
    elif stop_service:
        toggle = 'stop'
    else:
        raise ValueError("start_service or stop_service must be True")
    log_level = helpers.log_level(kwargs.get('log_level', 'info'))
    LOG.log(log_level, "Start: Toggle-%s service %s.", toggle, service_url)
    url_parts = service_url.split('/')
    post_url = re.sub(
        '/arcgis/rest/services/', '/arcgis/admin/services/',
        '/'.join(url_parts[:-1]) + '.{}/{}'.format(url_parts[-1], toggle),
        flags=re.I
        )
    post_data = {'f': 'json', 'token': token}
    response = requests.post(url=post_url, data=post_data)
    response.raise_for_status()
    LOG.log(log_level, "End: Toggle.")
    return service_url
