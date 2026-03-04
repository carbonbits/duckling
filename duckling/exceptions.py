"""Custom exceptions for the Duckling ORM."""


class DucklingError(Exception):
    """Base exception for all Duckling errors."""


class DocumentNotFound(DucklingError):
    """Raised when a document is not found."""


class DocumentAlreadyExists(DucklingError):
    """Raised when inserting a document that already exists."""


class NotInitializedError(DucklingError):
    """Raised when Duckling has not been initialized."""


class CollectionNotFound(DucklingError):
    """Raised when a table/collection does not exist."""


class InvalidQueryError(DucklingError):
    """Raised when a query is malformed."""


class ValidationError(DucklingError):
    """Raised when document validation fails."""


class ConnectionError(DucklingError):
    """Raised when a database connection fails."""
