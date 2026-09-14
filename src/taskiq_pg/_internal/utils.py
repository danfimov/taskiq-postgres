import re
import typing as tp


class DsnHelper:
    _sqlalchemy_dialect_suffix_re: tp.ClassVar[re.Pattern[str]] = re.compile(r"^(postgres(?:ql)?)\+[^:]+(://)")

    def _preformat_dsn(self, raw_dsn: str) -> str:
        """Method to prepare the DSN string (strips the SQLAlchemy-style driver suffix if present)."""
        return self._sqlalchemy_dialect_suffix_re.sub(r"\1\2", raw_dsn, count=1)

    @property
    def dsn(self) -> str:
        """Get the DSN string."""
        if callable(self._dsn):  # type: ignore[attr-defined]
            return self._preformat_dsn(self._dsn())  # type: ignore[attr-defined]
        return self._preformat_dsn(self._dsn)  # type: ignore[attr-defined]
