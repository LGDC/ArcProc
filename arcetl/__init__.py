"""ETL framework library based on ArcGIS/ArcPy."""

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
from arcetl.etl import (
    ArcETL,
    )
from arcetl import features
from arcetl import geometry
from arcetl import geoset
from arcetl import helpers
from arcetl.helpers import (
    contain,
    unique_ids,
    unique_dataset_path,
    unique_name,
    )
from arcetl import network
from arcetl import proximity
from arcetl import services
# Imported here to enable pass-through access to ArcPy objects.
import arcpy


__all__ = ()
__version__ = '2016.11'
