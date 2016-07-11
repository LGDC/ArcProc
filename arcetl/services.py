# -*- coding=utf-8 -*-
"""ArcGIS Server service operations."""
import logging
import re

import requests

from . import helpers


LOG = logging.getLogger(__name__)


@helpers.log_function
def generate_token(server_url, username, password, **kwargs):
    """Generate a security token for the given server.

    Args:
        server_url (str): URL for ArcGIS Server instance.
        username (str): Name of user requesting token.
        password (str): Password for user.
    Kwargs:
        minutes_active (int): Number of minutes token will be active.
        referer_url (str): URL of referring web app.
        requestor_ip (str): IP address of machine to use the token.
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('log_level', 'info'), ('minutes_active', None),
                          ('referer_url', None), ('requestor_ip', None)]:
        kwargs.setdefault(*kwarg_default)
    log_level = helpers.LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Generate token for %s.", server_url)
    post_url = requests.compat.urljoin(server_url, 'admin/generateToken')
    post_data = {'f': 'json', 'username': username, 'password': password}
    if kwargs['minutes_active']:
        post_data['expiration'] = kwargs['minutes_active']
    if kwargs['referer_url']:
        post_data['client'] = 'referer'
        post_data['referer'] = kwargs['referer_url']
    elif kwargs['requestor_ip']:
        post_data['client'] = 'ip'
        post_data['ip'] = kwargs['requestor_ip']
    else:
        post_data['client'] = 'requestip'
    token = requests.post(url=post_url, data=post_data).json()['token']
    LOG.log(log_level, "Token = %s.", token)
    LOG.log(log_level, "End: Generate.")
    return token


@helpers.log_function
def toggle_service(service_url, token, start_service=False, stop_service=False,
                   **kwargs):
    """Toggle service to start or stop.

    Args:
        service_url (str): URL for service endpoint.
        token (str): Security token for REST admin.
        start_service (bool): Flag to start service.
        stop_service (bool): Flag to stop service (only used if start_service
            not flagged).
    Kwargs:
        log_level (str): Level at which to log this function.
    Returns:
        str.
    """
    for kwarg_default in [('log_level', 'info')]:
        kwargs.setdefault(*kwarg_default)
    if start_service:
        toggle = 'start'
    elif stop_service:
        toggle = 'stop'
    else:
        raise ValueError("start_service or stop_service must be True")
    log_level = helpers.LOG_LEVEL_MAP[kwargs['log_level']]
    LOG.log(log_level, "Start: Toggle-%s service %s.", toggle, service_url)
    url_parts = service_url.split('/')
    post_url = re.sub(
        '/arcgis/rest/services/', '/arcgis/admin/services/',
        '/'.join(url_parts[:-1]) + '.{}/{}'.format(url_parts[-1], toggle),
        flags=re.I)
    post_data = {'f': 'json', 'token': token}
    response = requests.post(url=post_url, data=post_data)
    response.raise_for_status()
    LOG.log(log_level, "End: Toggle.")
    return service_url
