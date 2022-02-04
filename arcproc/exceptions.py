"""Exceptions subpackage."""
from pathlib import Path


class DatasetNotFoundError(Exception):
    """Exception raised when dataset not found at given path."""

    def __init__(
        self, dataset_path: Path, message: str = "Dataset not found at path"
    ) -> None:
        self.dataset_path: Path = dataset_path
        """Path to the not-found dataset."""
        self.message: str = message
        """Explanation of the error."""
        super().__init__(self.message)

    def __str__(self) -> str:
        return f"{self.dataset_path} -> {self.message}"


class DomainNotFoundError(Exception):
    """Exception raised when domain not found in given geodatabase."""

    def __init__(
        self,
        geodatabase_path: Path,
        domain_name: str,
        message: str = "Domain not found",
    ) -> None:
        self.geodatabase_path: Path = geodatabase_path
        """Path to geodatabase expected to have the domain."""
        self.domain_name: str = domain_name
        """Name of the not-found domain."""
        self.message = message
        """Explanation of the error."""
        super().__init__(self.message)

    def __str__(self) -> str:
        return f"{self.domain_name} in {self.geodatabase_path} -> {self.message}"


class FieldNotFoundError(Exception):
    """Exception raised when field not found on given dataset."""

    def __init__(
        self, dataset_path: Path, field_name: str, message: str = "Field not found",
    ) -> None:
        self.dataset_path = dataset_path
        """Path to dataset expected to carry the field."""
        self.field_name = field_name
        """Name of the not-found field."""
        self.message = message
        """Explanation of the error."""
        super().__init__(self.message)

    def __str__(self) -> str:
        return f"{self.field_name} on {self.dataset_path} -> {self.message}"
