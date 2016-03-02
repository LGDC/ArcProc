# -*- coding=utf-8 -*-
"""Metadata objects."""
import logging

from etl import ArcETL, ArcWorkspace


LOG = logging.getLogger(__name__)


class ETLMetadata(object):
    """Metadata for an extract, transform, & load (ETL) procedure."""

    def __init__(self, etl_name, workspace_path=None):
        self.name = etl_name
        self.operations = []
        self.workspace_path = workspace_path
        self.etl = ArcETL(ArcWorkspace(self.workspace_path))

    def add_assertion(self, operation_name, **kwargs):
        """Add assertion check to the operations list."""
        _kwargs = kwargs.copy()
        _kwargs['assertion_name'] = operation_name
        self.operations.append((self.etl.make_asssertion, _kwargs))

    def add_execution(self, function, **kwargs):
        """Add execution of the function provided."""
        self.operations.append((function, kwargs))

    def add_extraction(self, **kwargs):
        """Add extraction to the operations list."""
        self.operations.append((self.etl.extract, kwargs))

    def add_load(self, **kwargs):
        """Add load to the the operations list."""
        self.operations.append((self.etl.load, kwargs))

    def add_operation(self, operation_name, **kwargs):
        """Add generic operation to the operations list.

        Unlike transformation, generic operations must define all arguments.
        """
        self.operations.append(
            (getattr(self.etl.workspace, operation_name), kwargs))

    def add_transformation(self, operation_name, **kwargs):
        """Add transformation to the the operations list."""
        _kwargs = kwargs.copy()
        _kwargs['transform_name'] = operation_name
        self.operations.append((self.etl.transform, _kwargs))

    def run(self):
        """Perform all actions related to running an ETL."""
        LOG.info("Starting ETL for: %s.", self.name)
        # Perform listed ETL operations.
        try:
            for function, kwargs in self.operations:
                function(**kwargs)
        except:
            raise
        finally:
            self.etl.close()
        LOG.info("End ETL.")


class JobMetadata(object):
    """Metadata for a job (collection of ETLs & other procedures)."""

    def __init__(self, job_name, workspace_path=None):
        self.name = job_name
        self.workspace_path = workspace_path
        self.etls = []

    def add_etl(self, *etls):
        """Add ETL metadata to the ETL list."""
        self.etls.extend(list(etls))

    def run(self):
        """Perform actions to complete job."""
        for etl in self.etls:
            if not etl.workspace_path:
                etl.workspace_path = self.workspace_path
            etl.run()
