import asyncio
import json
import logging
import typing as tp
from collections.abc import AsyncGenerator, Callable
from contextlib import suppress

import psycopg
from psycopg import AsyncConnection, AsyncRawCursor, sql
from psycopg_pool import AsyncConnectionPool
from taskiq import AckableMessage, AsyncResultBackend, BrokerMessage

from taskiq_pg._internal.broker import LISTEN_KEEPALIVE_QUERY, BasePostgresBroker
from taskiq_pg._internal.retry import retry_delays
from taskiq_pg.psycopg.queries import (
    CLAIM_MESSAGE_QUERY,
    CREATE_MESSAGE_TABLE_QUERY,
    DELETE_MESSAGE_QUERY,
    INSERT_MESSAGE_QUERY,
)


logger = logging.getLogger("taskiq.psycopg_broker")

_T = tp.TypeVar("_T")


def _retrieve_exception(task: asyncio.Future[tp.Any]) -> None:
    """Mark an abandoned task's exception as retrieved, so asyncio does not log it."""
    if not task.cancelled():
        task.exception()


class PsycopgBroker(BasePostgresBroker):
    """Broker that uses PostgreSQL and psycopg with LISTEN/NOTIFY."""

    _read_conn: AsyncConnection
    _write_pool: AsyncConnectionPool
    _notifies_iter: tp.AsyncIterator[tp.Any]
    _owns_write_pool: bool
    _owns_read_conn: bool
    _closing: bool = False
    _unanswered_keepalive: asyncio.Task[None] | None = None

    @tp.overload
    def __init__(
        self,
        dsn: str | Callable[[], str] = ...,
        result_backend: AsyncResultBackend[_T] | None = ...,
        task_id_generator: Callable[[], str] | None = ...,
        channel_name: str = ...,
        table_name: str = ...,
        max_retry_attempts: int = ...,
        read_kwargs: dict[str, tp.Any] | None = ...,
        write_kwargs: dict[str, tp.Any] | None = ...,
        *,
        write_pool: None = ...,
        read_connection: None = ...,
        listen_keepalive_interval: float | None = ...,
        listen_keepalive_timeout: float = ...,
        poll_interval: float = ...,
    ) -> None: ...

    @tp.overload
    def __init__(
        self,
        dsn: str | Callable[[], str] = ...,
        result_backend: AsyncResultBackend[_T] | None = ...,
        task_id_generator: Callable[[], str] | None = ...,
        channel_name: str = ...,
        table_name: str = ...,
        max_retry_attempts: int = ...,
        read_kwargs: dict[str, tp.Any] | None = ...,
        write_kwargs: dict[str, tp.Any] | None = ...,
        *,
        write_pool: AsyncConnectionPool,
        read_connection: None = ...,
        listen_keepalive_interval: float | None = ...,
        listen_keepalive_timeout: float = ...,
        poll_interval: float = ...,
    ) -> None: ...

    @tp.overload
    def __init__(
        self,
        dsn: str | Callable[[], str] = ...,
        result_backend: AsyncResultBackend[_T] | None = ...,
        task_id_generator: Callable[[], str] | None = ...,
        channel_name: str = ...,
        table_name: str = ...,
        max_retry_attempts: int = ...,
        read_kwargs: dict[str, tp.Any] | None = ...,
        write_kwargs: dict[str, tp.Any] | None = ...,
        *,
        write_pool: None = ...,
        read_connection: AsyncConnection,
        listen_keepalive_interval: float | None = ...,
        listen_keepalive_timeout: float = ...,
        poll_interval: float = ...,
    ) -> None: ...

    @tp.overload
    def __init__(
        self,
        dsn: str | Callable[[], str] = ...,
        result_backend: AsyncResultBackend[_T] | None = ...,
        task_id_generator: Callable[[], str] | None = ...,
        channel_name: str = ...,
        table_name: str = ...,
        max_retry_attempts: int = ...,
        read_kwargs: dict[str, tp.Any] | None = ...,
        write_kwargs: dict[str, tp.Any] | None = ...,
        *,
        write_pool: AsyncConnectionPool,
        read_connection: AsyncConnection,
        listen_keepalive_interval: float | None = ...,
        listen_keepalive_timeout: float = ...,
        poll_interval: float = ...,
    ) -> None: ...

    def __init__(  # noqa: PLR0913
        self,
        dsn: str | Callable[[], str] = "postgresql://postgres:postgres@localhost:5432/postgres",
        result_backend: AsyncResultBackend[_T] | None = None,
        task_id_generator: Callable[[], str] | None = None,
        channel_name: str = "taskiq",
        table_name: str = "taskiq_messages",
        max_retry_attempts: int = 5,
        read_kwargs: dict[str, tp.Any] | None = None,
        write_kwargs: dict[str, tp.Any] | None = None,
        *,
        write_pool: AsyncConnectionPool | None = None,
        read_connection: AsyncConnection | None = None,
        listen_keepalive_interval: float | None = 30.0,
        listen_keepalive_timeout: float = 5.0,
        poll_interval: float = 5.0,
    ) -> None:
        """
        Construct a new PsycopgBroker.

        Args:
            dsn: PostgreSQL connection string or callable.
            result_backend: Custom result backend.
            task_id_generator: Custom task_id generator.
            channel_name: Name of the LISTEN/NOTIFY channel.
            table_name: Name of the table used to store messages.
            max_retry_attempts: Maximum number of message processing attempts.
            read_kwargs: Extra kwargs forwarded to `AsyncConnection.connect()`.
            write_kwargs: Extra kwargs forwarded to `AsyncConnectionPool()`.
            write_pool: An existing connection pool to reuse for writes.
            read_connection: An existing connection to reuse for LISTEN.
                Its lifecycle, including reconnection, remains the caller's responsibility.
            listen_keepalive_interval: How often, in seconds, to check the broker-owned LISTEN connection
                with a query; None disables the check.
            listen_keepalive_timeout: How long, in seconds, to wait for the check before reconnecting.
            poll_interval: How often, in seconds, to look for pending messages whose NOTIFY was missed.
        """
        super().__init__(
            dsn=dsn,
            result_backend=result_backend,
            task_id_generator=task_id_generator,
            channel_name=channel_name,
            table_name=table_name,
            max_retry_attempts=max_retry_attempts,
            read_kwargs=read_kwargs,
            write_kwargs=write_kwargs,
            listen_keepalive_interval=listen_keepalive_interval,
            listen_keepalive_timeout=listen_keepalive_timeout,
            poll_interval=poll_interval,
        )

        self._owns_write_pool = True
        if write_pool is not None:
            self._write_pool = write_pool
            self._owns_write_pool = False

        self._owns_read_conn = True
        if read_connection is not None:
            self._read_conn = read_connection
            self._owns_read_conn = False

    async def startup(self) -> None:
        """Initialize the broker."""
        await super().startup()
        self._closing = False

        if self._owns_write_pool:
            self._write_pool = AsyncConnectionPool(
                conninfo=self.dsn if self.dsn is not None else "",
                open=False,
                **self.write_kwargs,
            )

        if self._write_pool.closed:
            await self._write_pool.open()

        async with self._write_pool.connection() as connection, connection.cursor() as cursor:
            await cursor.execute(sql.SQL(CREATE_MESSAGE_TABLE_QUERY).format(sql.Identifier(self.table_name)))

        if self._owns_read_conn:
            await self._connect_listener()
        else:
            await self._read_conn.execute(sql.SQL("LISTEN {}").format(sql.Identifier(self.channel_name)))
            self._notifies_iter = self._read_conn.notifies()

    async def _connect_listener(self) -> None:
        """Open the broker-owned read connection and LISTEN on the channel."""
        conn = await AsyncConnection.connect(
            conninfo=self.dsn,
            **self.read_kwargs,
            autocommit=True,
            cursor_factory=AsyncRawCursor,
        )
        try:
            await conn.execute(sql.SQL("LISTEN {}").format(sql.Identifier(self.channel_name)))
        except BaseException:
            await conn.close()
            raise
        self._read_conn = conn
        self._notifies_iter = conn.notifies()

    async def _restore_listener(self) -> bool:
        """
        Reconnect the broker-owned LISTEN connection, retrying with randomized delays of up to 30 seconds.

        Returns False without reconnecting when the broker is shutting down.
        """
        await self._close_read_conn()
        delays = retry_delays()
        while not self._is_closing():
            try:
                await self._connect_listener()
            except Exception:  # noqa: PERF203
                delay = next(delays)
                logger.warning("LISTEN reconnect failed; retrying in %.1f seconds", delay, exc_info=True)
                await asyncio.sleep(delay)
            else:
                if self._is_closing():  # shutdown() ran while connecting and could not close this connection
                    await self._read_conn.close()
                    return False
                logger.info("LISTEN connection restored on channel %s", self.channel_name)
                return True
        return False

    async def _close_read_conn(self) -> None:
        """Close the LISTEN connection, then cancel a keepalive query it never answered."""
        await self._read_conn.close()
        if self._unanswered_keepalive is not None:
            self._unanswered_keepalive.cancel()
            self._unanswered_keepalive = None

    def _is_closing(self) -> bool:
        """
        Tell whether shutdown() has started.

        A method rather than an attribute check, so that type checkers do not assume
        the flag stays the same across awaits.
        """
        return self._closing

    async def shutdown(self) -> None:
        """Close all connections on shutdown."""
        self._closing = True
        await super().shutdown()
        if self._notifies_iter is not None:
            with suppress(RuntimeError):  # RuntimeError: aclose(): asynchronous generator is already running
                await self._notifies_iter.aclose()  # type: ignore[attr-defined]
        if self._read_conn is not None and self._owns_read_conn:
            await self._read_conn.notifies().aclose()
            await self._close_read_conn()
        if self._write_pool is not None and self._owns_write_pool:
            await self._write_pool.close()

    async def kick(self, message: BrokerMessage) -> None:
        """
        Send message to the channel.

        Inserts the message into the database and sends a NOTIFY.

        :param message: Message to send.
        """
        async with self._write_pool.connection() as connection, connection.cursor() as cursor:
            # insert message into db table
            await cursor.execute(
                sql.SQL(INSERT_MESSAGE_QUERY).format(sql.Identifier(self.table_name)),
                [
                    message.task_id,
                    message.task_name,
                    message.message.decode(),
                    json.dumps(message.labels),
                ],
            )
            row = await cursor.fetchone()
            if row is None:
                msg = "failed to insert message"
                raise RuntimeError(msg)
            message_inserted_id = int(row[0])

            delay_value = tp.cast("str | None", message.labels.get("delay"))
            if delay_value is None:
                # Send NOTIFY with message ID as payload
                await cursor.execute(
                    sql.SQL("NOTIFY {}, {}").format(
                        sql.Identifier(self.channel_name),
                        sql.Literal(str(message_inserted_id)),
                    ),
                )
                return

        # The insert is committed once the connection is returned to the pool,
        # so a delayed NOTIFY sent from here never reaches a worker before the row is visible.
        await self._schedule_notification(message_inserted_id, int(delay_value))

    async def _schedule_notification(self, message_id: int, delay_seconds: int) -> None:
        """Schedule a notification to be sent after a delay."""
        await asyncio.sleep(delay_seconds)
        async with self._write_pool.connection() as connection, connection.cursor() as cursor:
            # Send NOTIFY with message ID as payload
            await cursor.execute(
                sql.SQL("NOTIFY {}, {}").format(
                    sql.Identifier(self.channel_name),
                    sql.Literal(str(message_id)),
                )
            )

    async def _notifications(self) -> AsyncGenerator[str, None]:
        """
        Yield notification payloads from the current LISTEN connection.

        A broker-owned connection is read in slices of `listen_keepalive_interval` seconds with a
        keepalive query in between; a query that does not answer in time raises OperationalError.
        """
        interval = self.listen_keepalive_interval
        if interval is None or not self._owns_read_conn:
            async for notify in self._notifies_iter:
                yield notify.payload
            return
        while not self._is_closing():
            self._notifies_iter = self._read_conn.notifies(timeout=interval)
            async for notify in self._notifies_iter:
                yield notify.payload
            if self._is_closing():
                return
            await self._check_listener()

    async def _check_listener(self) -> None:
        """
        Run the keepalive query; raise OperationalError if it does not answer in time.

        The unanswered query is not cancelled here: psycopg waits for the server to acknowledge
        a cancellation, which a hung connection never does. It is cancelled once the connection
        is closed.
        """
        query = asyncio.ensure_future(self._keepalive_query())
        query.add_done_callback(_retrieve_exception)
        try:
            done, _ = await asyncio.wait({query}, timeout=self.listen_keepalive_timeout)
        except asyncio.CancelledError:
            query.cancel()
            raise
        if query in done:
            query.result()
            return
        self._unanswered_keepalive = query
        msg = "LISTEN connection did not answer a keepalive query"
        raise psycopg.OperationalError(msg)

    async def _keepalive_query(self) -> None:
        """Run a trivial query on the LISTEN connection."""
        await self._read_conn.execute(LISTEN_KEEPALIVE_QUERY)

    async def _listen_context(self) -> AsyncGenerator[str, None]:
        while True:
            try:
                async for payload in self._notifications():
                    yield payload
            except psycopg.OperationalError:  # noqa: PERF203
                if self._closing:  # the connection was closed by shutdown()
                    return
                if not self._owns_read_conn:
                    raise
                logger.warning("LISTEN connection lost on channel %s; reconnecting", self.channel_name, exc_info=True)
                if not await self._restore_listener():
                    return
            else:
                return

    async def listen(self) -> AsyncGenerator[AckableMessage, None]:
        """
        Listen to the channel.

        Yields messages as they are received.

        :yields: AckableMessage instances.
        """
        async for message_id_str in self._listen_context():
            message_id = int(message_id_str)  # payload is the message id
            try:
                async with self._write_pool.connection() as connection, connection.cursor() as cursor:
                    await cursor.execute(
                        sql.SQL(CLAIM_MESSAGE_QUERY).format(sql.Identifier(self.table_name)),
                        [message_id],
                    )
                    claimed_message = await cursor.fetchone()
                    if claimed_message is None:
                        continue
            except psycopg.OperationalError:  # message was claimed by another worker
                continue
            message_str = claimed_message[3]
            if not isinstance(message_str, str):
                msg = "Message is not a string"
                raise TypeError(msg)
            message_data = message_str.encode()

            async def ack(*, _message_id: int = message_id) -> None:
                async with self._write_pool.connection() as connection, connection.cursor() as cursor:
                    await cursor.execute(
                        sql.SQL(DELETE_MESSAGE_QUERY).format(sql.Identifier(self.table_name)),
                        [_message_id],
                    )

            yield AckableMessage(data=message_data, ack=ack)
