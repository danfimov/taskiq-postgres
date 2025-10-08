from __future__ import annotations

import abc
import typing as tp

from taskiq import AsyncBroker, AsyncResultBackend


if tp.TYPE_CHECKING:
    import asyncio


_T = tp.TypeVar("_T")


class BasePostgresBroker(AsyncBroker, abc.ABC):
    """Base class for Postgres brokers."""

    def __init__(
        self,
        dsn: str | tp.Callable[[], str] = "postgresql://postgres:postgres@localhost:5432/postgres",
        result_backend: AsyncResultBackend[_T] | None = None,
        task_id_generator: tp.Callable[[], str] | None = None,
        channel_name: str = "taskiq",
        table_name: str = "taskiq_messages",
        max_retry_attempts: int = 5,
        read_kwargs: dict[str, tp.Any] | None = None,
        write_kwargs: dict[str, tp.Any] | None = None,
    ) -> None:
        """
        Construct a new broker.

        Args:
            dsn: connection string to PostgreSQL, or callable returning one.
            result_backend: Custom result backend.
            task_id_generator: Custom task_id generator.
            channel_name: Name of the channel to listen on.
            table_name: Name of the table to store messages.
            max_retry_attempts: Maximum number of message processing attempts.
            read_kwargs: Additional arguments for read connection creation.
            write_kwargs: Additional arguments for write pool creation.

        """
        super().__init__(
            result_backend=result_backend,
            task_id_generator=task_id_generator,
        )
        self._dsn: str | tp.Callable[[], str] = dsn
        self.channel_name: str = channel_name
        self.table_name: str = table_name
        self.read_kwargs: dict[str, tp.Any] = read_kwargs or {}
        self.write_kwargs: dict[str, tp.Any] = write_kwargs or {}
        self.max_retry_attempts: int = max_retry_attempts
        self._queue: asyncio.Queue[str] | None = None

    @property
    def dsn(self) -> str:
        """
        Get the DSN string.

        Returns:
            A string with dsn or None if dsn isn't set yet.

        """
        if callable(self._dsn):
            return self._dsn()
        return self._dsn
