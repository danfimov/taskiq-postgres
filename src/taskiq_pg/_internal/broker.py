import abc
import typing as tp

from taskiq import AsyncBroker, AsyncResultBackend

from taskiq_pg._internal.utils import DsnHelper


if tp.TYPE_CHECKING:
    import asyncio


_T = tp.TypeVar("_T")


class BasePostgresBroker(AsyncBroker, DsnHelper, abc.ABC):
    """Base class for Postgres brokers."""

    def __init__(  # noqa: PLR0913
        self,
        dsn: str | tp.Callable[[], str] = "postgresql://postgres:postgres@localhost:5432/postgres",
        result_backend: AsyncResultBackend[_T] | None = None,
        task_id_generator: tp.Callable[[], str] | None = None,
        channel_name: str = "taskiq",
        table_name: str = "taskiq_messages",
        max_retry_attempts: int = 5,
        read_kwargs: dict[str, tp.Any] | None = None,
        write_kwargs: dict[str, tp.Any] | None = None,
        *,
        listen_keepalive_interval: float | None = 30.0,
        listen_keepalive_timeout: float = 5.0,
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
            listen_keepalive_interval: How often, in seconds, to run a query on the broker-owned LISTEN
                connection. It keeps proxies and `idle_session_timeout` from closing an idle connection
                and detects a connection that hangs without being closed. None disables the check.
            listen_keepalive_timeout: How long, in seconds, to wait for the answer before treating
                the LISTEN connection as lost and reconnecting.
        """
        if listen_keepalive_interval is not None and listen_keepalive_interval <= 0:
            msg = "listen_keepalive_interval must be positive or None"
            raise ValueError(msg)
        if listen_keepalive_timeout <= 0:
            msg = "listen_keepalive_timeout must be positive"
            raise ValueError(msg)
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
        self.listen_keepalive_interval: float | None = listen_keepalive_interval
        self.listen_keepalive_timeout: float = listen_keepalive_timeout
        self._queue: asyncio.Queue[str] | None = None


LISTEN_KEEPALIVE_QUERY = "SELECT 1 /* taskiq-pg LISTEN keepalive */"
"""Query used to check a LISTEN connection; the comment makes it recognizable in pg_stat_activity."""
