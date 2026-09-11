from taskiq_pg._internal.utils import DsnHelper


class AsyncpgDsnHelper(DsnHelper):
    """Helper class to prepare DSN for asyncpg driver."""

    def _preformat_dsn(self, raw_dsn: str) -> str:
        """
        Method to prepare the DSN string to the end format that will be used by the database driver.

        Returns the DSN string.
        """
        if raw_dsn.startswith("postgres+asyncpg://"):
            return raw_dsn.replace("postgres+asyncpg://", "postgresql://", 1)
        return raw_dsn
