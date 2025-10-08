from __future__ import annotations

import abc
import typing as tp

from taskiq import AsyncResultBackend
from taskiq.serializers import PickleSerializer


if tp.TYPE_CHECKING:
    from taskiq.abc.serializer import TaskiqSerializer


ReturnType = tp.TypeVar("ReturnType")


class BasePostgresResultBackend(AsyncResultBackend[ReturnType], abc.ABC):
    """Base class for PostgreSQL result backends."""

    def __init__(
        self,
        dsn: tp.Callable[[], str] | str | None = "postgres://postgres:postgres@localhost:5432/postgres",
        keep_results: bool = True,
        table_name: str = "taskiq_results",
        field_for_task_id: tp.Literal["VarChar", "Text", "Uuid"] = "VarChar",
        serializer: TaskiqSerializer | None = None,
        **connect_kwargs: tp.Any,
    ) -> None:
        """
        Construct new result backend.

        Args:
            dsn: connection string to PostgreSQL, or callable returning one.
            keep_results: flag to not remove results from the database after reading.
            table_name: name of the table to store results.
            field_for_task_id: type of the field to store task_id.
            serializer: serializer class to serialize/deserialize result from task.
            connect_kwargs: additional arguments for creating connection pool.

        """
        self._dsn: tp.Final = dsn
        self.keep_results: tp.Final = keep_results
        self.table_name: tp.Final = table_name
        self.field_for_task_id: tp.Final = field_for_task_id
        self.connect_kwargs: tp.Final = connect_kwargs
        self.serializer = serializer or PickleSerializer()

    @property
    def dsn(self) -> str | None:
        """
        Get the DSN string.

        Returns the DSN string or None if not set.
        """
        if callable(self._dsn):
            return self._dsn()
        return self._dsn
