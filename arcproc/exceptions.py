"""Exceptions subpackage."""


class DatasetNotExistsError(Exception):
    """Exception raised when required field does not exist.

    Attributes:
        dataset_path: Path to the nonexistant dataset.
        message: Explanation of the error.
    """

    def __init__(self, dataset_path, message="Dataset does not exist"):
        self.dataset_path = dataset_path
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return f"{self.dataset_path} -> {self.message}"


class FieldNotExistsError(Exception):
    """Exception raised when required field does not exist.

    Attributes:
        dataset_path: Path to the dataset expected to have the field.
        field_name: Name of the nonexistant field.
        message: Explanation of the error.
    """

    def __init__(self, dataset_path, field_name, message="Field does not exist"):
        self.dataset_path = dataset_path
        self.field_name = field_name
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return f"{self.field_name} on {self.dataset_path} -> {self.message}"
