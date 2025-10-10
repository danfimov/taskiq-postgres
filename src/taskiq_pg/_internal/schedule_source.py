from __future__ import annotations

import typing as tp

from taskiq import ScheduleSource


if tp.TYPE_CHECKING:
    from taskiq.abc.broker import AsyncBroker


class BasePostgresScheduleSource(ScheduleSource):
    def __init__(
        self,
        broker: AsyncBroker,
        dsn: str | tp.Callable[[], str] = "postgresql://postgres:postgres@localhost:5432/postgres",
        table_name: str = "taskiq_schedules",
        **connect_kwargs: tp.Any,
    ) -> None:
        """
        Initialize the PostgreSQL scheduler source.

        Sets up a scheduler source that stores scheduled tasks in a PostgreSQL database.
        This scheduler source manages task schedules, allowing for persistent storage and retrieval of scheduled tasks
        across application restarts.

        Args:
            dsn: PostgreSQL connection string
            table_name: Name of the table to store scheduled tasks. Will be created automatically if it doesn't exist.
            broker: The TaskIQ broker instance to use for finding and managing tasks.
                Required if startup_schedule is provided.
            **connect_kwargs: Additional keyword arguments passed to the database connection pool.

        """
        self._broker: tp.Final = broker
        self._dsn: tp.Final = dsn
        self._table_name: tp.Final = table_name
        self._connect_kwargs: tp.Final = connect_kwargs

    @property
    def dsn(self) -> str | None:
        """
        Get the DSN string.

        Returns the DSN string or None if not set.
        """
        if callable(self._dsn):
            return self._dsn()
        return self._dsn
