import abc
import hashlib
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
        poll_interval: float = 5.0,
        processing_timeout: float | None = None,
    ) -> None:
        """
        Construct a new broker.

        Args:
            dsn: connection string to PostgreSQL, or callable returning one.
            result_backend: Custom result backend.
            task_id_generator: Custom task_id generator.
            channel_name: Name of the channel to listen on.
            table_name: Name of the table to store messages.
            max_retry_attempts: With processing_timeout set, how many times a message may be claimed
                before it is marked as failed instead of being delivered again.
            read_kwargs: Additional arguments for read connection creation.
            write_kwargs: Additional arguments for write pool creation.
            listen_keepalive_interval: How often, in seconds, to run a query on the broker-owned LISTEN
                connection. It keeps proxies and `idle_session_timeout` from closing an idle connection
                and detects a connection that hangs without being closed. None disables the check.
            listen_keepalive_timeout: How long, in seconds, to wait for the answer before treating
                the LISTEN connection as lost and reconnecting.
            poll_interval: How often, in seconds, a listening worker looks for pending messages whose
                NOTIFY it missed (sent while it was not listening or reconnecting), including delayed
                messages whose delay has elapsed. It also polls right after start and after reconnecting.
            processing_timeout: Enables at-least-once delivery. A claimed message that is neither acknowledged
                nor rejected within this many seconds (for example, because its worker died) is delivered
                again, up to max_retry_attempts times. Tasks must then be idempotent, and the timeout must be
                longer than the longest task. None (the default) keeps at-most-once delivery: such a message
                stays in the 'processing' state.
        """
        if listen_keepalive_interval is not None and listen_keepalive_interval <= 0:
            msg = "listen_keepalive_interval must be positive or None"
            raise ValueError(msg)
        if listen_keepalive_timeout <= 0:
            msg = "listen_keepalive_timeout must be positive"
            raise ValueError(msg)
        if poll_interval <= 0:
            msg = "poll_interval must be positive"
            raise ValueError(msg)
        if processing_timeout is not None and processing_timeout <= 0:
            msg = "processing_timeout must be positive or None"
            raise ValueError(msg)
        if max_retry_attempts < 1:
            msg = "max_retry_attempts must be at least 1"
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
        self.poll_interval: float = poll_interval
        self.processing_timeout: float | None = processing_timeout
        self._queue: asyncio.Queue[str] | None = None


LISTEN_KEEPALIVE_QUERY = "SELECT 1 /* taskiq-pg LISTEN keepalive */"
"""Query used to check a LISTEN connection; the comment makes it recognizable in pg_stat_activity."""

POLL_NOW = ""
"""Queue item that asks a listening worker to poll for pending messages right away (never a message id)."""


def schema_lock_query(table_name: str) -> str:
    """
    Return a query that serializes schema setup of one messages table.

    Run it first in the transaction that creates the table and its indexes: brokers that start at the same
    time then wait for each other instead of failing on concurrent `CREATE ... IF NOT EXISTS`.
    """
    digest = hashlib.sha256(f"taskiq_pg:messages:{table_name}".encode()).digest()
    return f"SELECT pg_advisory_xact_lock({int.from_bytes(digest[:8], 'big', signed=True)})"


SCHEMA_STATUS_QUERY = """
SELECT
    to_regclass({table}) IS NOT NULL AS has_table,
    (
        SELECT count(*) FROM pg_attribute
        WHERE attrelid = to_regclass({table}) AND attname IN ('claimed_at', 'attempts') AND NOT attisdropped
    ) = 2 AS has_delivery_columns,
    to_regclass({pending_index}) IS NOT NULL AS has_pending_index,
    to_regclass({processing_index}) IS NOT NULL AS has_processing_index
"""
"""
Query that tells which parts of the messages table already exist.

Brokers run DDL only for the missing parts: `ALTER TABLE` and `CREATE INDEX` require owning the table even with
`IF NOT EXISTS`, so a role with only DML privileges can use a table that was prepared in advance.
"""
