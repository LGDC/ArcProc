# -*- coding=utf-8 -*-
"""ETL framework library based on ArcGIS/ArcPy."""
#pylint: disable=unused-import
from arcetl.etl import ArcETL
from arcetl.helpers import (
    sexagesimal_angle_to_decimal, unique_ids, unique_name,
    unique_temp_dataset_path
    )


__version__ = '1.0'


##TODO: Find a home for these below.

import logging

LOG = logging.getLogger(__name__)
