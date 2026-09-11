class DsnHelper:
    def _preformat_dsn(self, raw_dsn: str) -> str:
        """
        Method to prepare the DSN string to the end format that will be used by the database driver.

        Returns the DSN string.
        """
        return raw_dsn

    @property
    def dsn(self) -> str:
        """
        Get the DSN string.

        Returns the DSN string or None if not set.
        """
        if callable(self._dsn):  # type: ignore[attr-defined]
            return self._preformat_dsn(self._dsn())  # type: ignore[attr-defined]
        return self._preformat_dsn(self._dsn)  # type: ignore[attr-defined]
