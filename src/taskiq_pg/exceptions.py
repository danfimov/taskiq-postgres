class BaseTaskiqPgError(Exception):
    """Base error for all possible exception in the lib."""


class DatabaseConnectionError(BaseTaskiqPgError):
    """Error if cannot connect to PostgreSQL."""


class ResultIsMissingError(BaseTaskiqPgError):
    """Error if cannot retrieve result from PostgreSQL."""
