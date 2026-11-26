import abc
import typing as tp

from taskiq import ScheduleSource

from taskiq_pg._internal.utils import DsnHelper


class BasePostgresScheduleSource(ScheduleSource, DsnHelper, abc.ABC):
    def __init__(
        self,
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
            **connect_kwargs: Additional keyword arguments passed to the database connection pool.
        """
        self._dsn: tp.Final[str | tp.Callable[[], str]] = dsn
        self._table_name: tp.Final[str] = table_name
        self._connect_kwargs: tp.Final[tp.Any] = connect_kwargs
