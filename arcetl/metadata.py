# -*- coding=utf-8 -*-
import logging

from .etl import ArcETL, ArcWorkspace


logger = logging.getLogger(__name__)


# Metadata classes.

class ETLMetadata(object):
    """Metadata for an extract, transform, & load (ETL) procedure."""

    def __init__(self, etl_name, workspace_path=None, operations=[]):
        self.name = etl_name
        self.operations = list(operations)
        self.workspace_path = workspace_path

    def add_assertion(self, operation_name, **kwargs):
        """Add assertion check to the operations list."""
        elf.operations.append(
            OperationMetadata(operation_name, 'assert', kwargs))

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
        self.operations.append(
            OperationMetadata(operation_name, 'operate', kwargs))

    def add_operation_from_metadata(self, *operation_metadata):
        """Add operation metadata to the operations list."""
        self.operations.extend(list(operation_metadata))

    def add_transformation(self, operation_name, **kwargs):
        """Add transformation to the the operations list."""
        self.operations.append(
            OperationMetadata(operation_name, 'transform', kwargs))

    def run(self):
        """Perform all actions related to running an ETL."""
        logger.info("Starting ETL for: {}.".format(self.name))
        with ArcETL(ArcWorkspace(self.workspace_path)) as etl:
            # Perform listed ETL operations.
            for operation in self.operations:
                if operation.type == 'assert':
                    etl.assert_true(operation.name, **operation.kwargs)
                elif operation.type == 'execute':
                    operation.name(**operation.kwargs)
                elif operation.type in ('extract', 'load'):
                    getattr(etl, operation.name)(**operation.kwargs)
                elif operation.type == 'operate':
                    getattr(etl.workspace, operation.name)(**operation.kwargs)
                elif operation.type == 'transform':
                    etl.transform(operation.name, **operation.kwargs)
                else:
                    raise ValueError(
                        "Invalid operation type: {}.".format(operation.type))
        logger.info("End ETL.")


class JobMetadata(object):
    """Metadata for a job (collection of ETLs & other procedures)."""

    def __init__(self, job_name, workspace_path=None, etls=[]):
        self.name = job_name
        self.workspace_path = workspace_path
        self.etls = list(etls)

    def add_etl(self, *etls):
        """Add ETL metadata to the ETL list."""
        self.etls.extend(list(etls))

    def run(self):
        """Perform actions to complete job."""
        for etl in self.etls:
            if not etl.workspace_path:
                etl.workspace_path = self.workspace_path
            etl.run()


class OperationMetadata(object):
    """Metadata for an individual ETL operation."""

    def __init__(self, operation_name, operation_type, kwargs={}):
        self.name = operation_name
        self.type = operation_type
        self.kwargs = kwargs
