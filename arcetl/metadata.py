# -*- coding=utf-8 -*-
import logging

from .etl import ArcETL, ArcWorkspace


logger = logging.getLogger(__name__)


# Metadata classes.

class ETLMetadata(object):
    """Metadata class for an extract, transform, and load (ETL) procedure."""
    def __init__(self, etl_name, operations=[]):
        self.name = etl_name
        self.operations = list(operations)
    def add_assertion(self, operation_name, **kwargs):
        """Add assertion check to the operations list."""
        elf.operations.append(OperationMetadata(operation_name, 'assert', kwargs))
    def add_execution(self, function, **kwargs):
        """Add execution of the function provided."""
        self.operations.append(OperationMetadata(function, 'execute', kwargs))
    def add_extraction(self, **kwargs):
        """Add extraction to the operations list."""
        self.operations.append(OperationMetadata('extract', 'extract', kwargs))
    def add_load(self, **kwargs):
        """Add load to the the operations list."""
        self.operations.append(OperationMetadata('load', 'load', kwargs))
    def add_operation(self, operation_name, **kwargs):
        """Add generic operation to the operations list."""
        self.operations.append(OperationMetadata(operation_name, 'operate', kwargs))
    def add_operation_from_metadata(self, *operation_metadata):
        """Add operations to the operations list."""
        self.operations.extend(list(operation_metadata))
    def add_transformation(self, operation_name, **kwargs):
        """Add load to the the operations list."""
        self.operations.append(OperationMetadata(operation_name, 'transform', kwargs))


class JobMetadata(object):
    """Metadata class for a scheduled job (collection of ETLs and other procedures)."""
    def __init__(self, job_name, workspace_path=None, etls=[]):
        self.name = job_name
        self.workspace_path = workspace_path
        self.etls = list(etls)
    def add_etl(self, *etls):
        """Add operations to the operations list."""
        self.etls.extend(list(etls))


class OperationMetadata(object):
    """Metadata class for an individual ETL operation."""
    def __init__(self, operation_name, operation_type, kwargs={}):
        self.name = operation_name
        self.type = operation_type
        self.kwargs = kwargs


# Helper functions.

def create_dataset_from_metadata(dataset_metadata, path_tag, field_tag=None):
    """Build a dataset from the information in a given schema metadata."""
    if not field_tag:
        field_tag = path_tag
    dataset_path = ArcWorkspace().create_dataset(
        dataset_path = dataset_metadata['paths'][path_tag],
        field_metadata=[field for field in dataset_metadata['fields']
                        if field_tag in field.get('tags')],
        geometry_type = dataset_metadata.get('geometry_type'),
        spatial_reference_id = dataset_metadata.get('spatial_reference_id'))
    return dataset_path


def run_job(job_metadata):
    """Perform all actions related to running a processing job."""
    for etl_metadata in job_metadata.etls:
        run_etl(etl_metadata, job_metadata.workspace_path)


def run_etl(etl_metadata, workspace_path=None):
    """Perform all actions related to running an ETL."""
    logger.info("Starting ETL for : {}.".format(etl_metadata.name))
    with ArcETL(ArcWorkspace(workspace_path)) as etl:
        # Perform listed ETL operations.
        for operation in etl_metadata.operations:
            if operation.type == 'assert':
                etl.assert_true(operation_name, **operation.kwargs)
            elif operation.type == 'execute':
                operation.name(**operation.kwargs)
            elif operation.type in ('extract', 'load'):
                getattr(etl, operation.name)(**operation.kwargs)
            elif operation.type == 'operate':
                getattr(etl.workspace, operation.name)(**operation.kwargs)
            elif operation.type == 'transform':
               etl.transform(operation.name, **operation.kwargs)
            else:
                raise ValueError("Invalid operation type: {}.".format(operation.type))
