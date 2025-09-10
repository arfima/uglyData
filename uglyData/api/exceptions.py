from psycopg.errors import ForeignKeyViolation


class AssetNotFound(Exception):
    """Asset not found in database"""


class AssetAlreadyExists(Exception):
    """Asset already exists in database"""


class FieldNotValid(Exception):
    """Field not valid"""

    def __init__(self, error: ForeignKeyViolation):
        value = error.args[0].split("Key (")[1].split("(")[1].split(")")[0]
        column = error.args[0].split("Key (")[1].split(")")[0]
        self.args = (
            f"The value '{value}' you entered for the '{column}' field is not valid. "
            "Please choose one of the options available in our database",
        )


class ForeignKeyViolationError(Exception):
    def __init__(self, error: ForeignKeyViolation):
        value = error.args[0].split("Key (")[1].split("(")[1].split(")")[0]
        column = error.args[0].split("Key (")[1].split(")")[0]
        self.args = (
            "Cannot delete the item because it is associated with other data."
            f" The value '{value}' of the '{column}' column is referenced elsewhere. ",
        )


class DataTypeNotFound(Exception):
    """Data type not found in database"""
