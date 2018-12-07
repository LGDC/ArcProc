"""ETL framework library based on ArcGIS/ArcPy."""

# pylint: disable=relative-beyond-top-level, unused-import
from arcetl import arcobj
from arcetl.arcobj import (
    ArcExtension,
    DatasetView,
    Editor,
    TempDatasetCopy,
    spatial_reference_metadata,
)
from arcetl import attributes
from arcetl import combo
from arcetl import convert
from arcetl import dataset
from arcetl import diff
from arcetl import etl
from arcetl.etl import ArcETL
from arcetl import features
from arcetl import geometry
from arcetl import geoset
from arcetl import helpers
from arcetl.helpers import contain, freeze_values, unique_ids, unique_name, unique_path
from arcetl import network
from arcetl import proximity
from arcetl import services

# pylint: enable=relative-beyond-top-level, unused-import


__all__ = []
##TODO: Replace YYYY.MM with semantic versioning.
__version__ = "2016.11"
