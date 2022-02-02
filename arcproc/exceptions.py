"""Exceptions subpackage."""


class DatasetNotFoundError(Exception):
    """Exception raised when dataset not found at given path.

    Attributes:
        dataset_path: Path to the not-found dataset.
        message: Explanation of the error.
    """

    def __init__(self, dataset_path, message="Dataset not found at path"):
        self.dataset_path = dataset_path
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return f"{self.dataset_path} -> {self.message}"


class FieldNotFoundError(Exception):
    """Exception raised when field not found on given dataset.

    Attributes:
        dataset_path: Path to dataset expected to carry the field.
        field_name: Name of the not-found field.
        message: Explanation of the error.
    """

    def __init__(self, dataset_path, field_name, message="Field not found"):
        self.dataset_path = dataset_path
        self.field_name = field_name
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return f"{self.field_name} on {self.dataset_path} -> {self.message}"
