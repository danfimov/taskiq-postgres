import asyncio
import json
import logging
import typing as tp
from collections.abc import AsyncGenerator, Callable
from contextlib import suppress

import asyncpg
from taskiq import AckableMessage, AsyncResultBackend, BrokerMessage

from taskiq_pg._internal.broker import BasePostgresBroker
from taskiq_pg.asyncpg.queries import (
    CLAIM_MESSAGE_QUERY,
    CREATE_MESSAGE_TABLE_QUERY,
    DELETE_MESSAGE_QUERY,
    INSERT_MESSAGE_QUERY,
)


logger = logging.getLogger("taskiq.asyncpg_broker")

_T = tp.TypeVar("_T")


class AsyncpgBroker(BasePostgresBroker):
    """Broker that uses asyncpg as driver and PostgreSQL with LISTEN/NOTIFY mechanism."""

    _read_conn: asyncpg.Connection | None = None
    _write_pool: asyncpg.pool.Pool | None = None
    _owns_write_pool: bool
    _owns_read_conn: bool
    _listener_task: asyncio.Task[None] | None = None
    _listener_disconnected: asyncio.Event

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
        write_pool: asyncpg.pool.Pool,
        read_connection: None = ...,
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
        read_connection: asyncpg.Connection,
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
        write_pool: asyncpg.pool.Pool,
        read_connection: asyncpg.Connection,
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
        write_pool: asyncpg.pool.Pool | None = None,
        read_connection: asyncpg.Connection | None = None,
    ) -> None:
        """
        Construct a new AsyncpgBroker.

        Args:
            dsn: PostgreSQL connection string or a callable returning one.
            result_backend: Custom result backend.
            task_id_generator: Custom task_id generator.
            channel_name: Name of the LISTEN/NOTIFY channel.
            table_name: Name of the table used to store messages.
            max_retry_attempts: Maximum number of message processing attempts.
            read_kwargs: Extra kwargs forwarded to `asyncpg.connect()`.
            write_kwargs: Extra kwargs forwarded to `asyncpg.create_pool()`.
            write_pool: An existing connection pool to reuse for writes.
            read_connection: An existing connection to reuse for LISTEN.
                Its lifecycle, including reconnection, remains the caller's responsibility.
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
        if self._listener_task is not None and not self._listener_task.done():
            return
        await super().startup()
        self._queue = asyncio.Queue()
        self._listener_disconnected = asyncio.Event()
        try:
            if self._write_pool is None:
                self._write_pool = await asyncpg.create_pool(self.dsn, **self.write_kwargs)
            async with self._write_pool.acquire() as conn:
                await conn.execute(CREATE_MESSAGE_TABLE_QUERY.format(self.table_name))
            await self._connect_listener()
            self._listener_task = asyncio.create_task(self._restore_listener())
        except BaseException:
            await self.shutdown()
            raise

    async def shutdown(self) -> None:
        """Close all connections on shutdown."""
        if self._listener_task is not None:
            self._listener_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._listener_task
            self._listener_task = None
        try:
            await super().shutdown()
        finally:
            if self._read_conn is not None:
                self._read_conn.remove_termination_listener(self._connection_terminated)
                if not self._read_conn.is_closed():
                    await self._read_conn.remove_listener(self.channel_name, self._notification_handler)
                if self._owns_read_conn:
                    await self._read_conn.close()
                    self._read_conn = None
            if self._write_pool is not None and self._owns_write_pool:
                await self._write_pool.close()
                self._write_pool = None

    async def _connect_listener(self) -> None:
        """Restore LISTEN, creating and cleaning up broker-owned connections as needed."""
        self._listener_disconnected.clear()
        conn = self._read_conn
        if conn is None or (self._owns_read_conn and conn.is_closed()):
            conn = await asyncpg.connect(self.dsn, **self.read_kwargs)
        conn.add_termination_listener(self._connection_terminated)
        try:
            await conn.add_listener(self.channel_name, self._notification_handler)
        except BaseException:
            conn.remove_termination_listener(self._connection_terminated)
            if self._owns_read_conn:
                conn.terminate()
                self._read_conn = None
            raise
        self._read_conn = conn
        if self._owns_read_conn and conn.is_closed():
            self._listener_disconnected.set()

    def _connection_terminated(
        self,
        conn: asyncpg.Connection | asyncpg.pool.PoolConnectionProxy,
    ) -> None:
        """Notify the background task when the current LISTEN connection closes."""
        if self._owns_read_conn and conn is self._read_conn:
            self._listener_disconnected.set()

    async def _restore_listener(self) -> None:
        """Restore LISTEN after disconnection with retry delays from one to 30 seconds."""
        while True:
            await self._listener_disconnected.wait()
            delay = 1.0
            while True:
                try:
                    await self._connect_listener()
                except (OSError, asyncio.TimeoutError, asyncpg.PostgresError, asyncpg.InterfaceError):  # noqa: PERF203
                    logger.warning("LISTEN reconnect failed; retrying in %s seconds", delay, exc_info=True)
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 30.0)
                else:
                    logger.info("LISTEN connection restored on channel %s", self.channel_name)
                    break

    def _notification_handler(
        self,
        con_ref: asyncpg.Connection | asyncpg.pool.PoolConnectionProxy,  # noqa: ARG002
        pid: int,  # noqa: ARG002
        channel: str,
        payload: object,
        /,
    ) -> None:
        """
        Handle NOTIFY messages.

        From asyncpg.connection.add_listener docstring:
            A callable or a coroutine function receiving the following arguments:
            **con_ref**: a Connection the callback is registered with;
            **pid**: PID of the Postgres server that sent the notification;
            **channel**: name of the channel the notification was sent to;
            **payload**: the payload.
        """
        logger.debug("Received notification on channel %s: %s", channel, payload)
        if self._queue is not None:
            self._queue.put_nowait(str(payload))

    async def kick(self, message: BrokerMessage) -> None:
        """
        Send message to the channel.

        Inserts the message into the database and sends a NOTIFY.

        :param message: Message to send.
        """
        if self._write_pool is None:
            msg = "Please run startup before kicking."
            raise ValueError(msg)

        async with self._write_pool.acquire() as conn:
            # Insert the message into the database
            message_inserted_id = tp.cast(
                "int",
                await conn.fetchval(
                    INSERT_MESSAGE_QUERY.format(self.table_name),
                    message.task_id,
                    message.task_name,
                    message.message.decode(),
                    json.dumps(message.labels),
                ),
            )

            delay_value = message.labels.get("delay")
            if delay_value is not None:
                delay_seconds = int(delay_value)
                _ = asyncio.create_task(  # noqa: RUF006
                    self._schedule_notification(message_inserted_id, delay_seconds),
                )
            else:
                # Send a NOTIFY with the message ID as payload
                _ = await conn.execute(
                    f"NOTIFY {self.channel_name}, '{message_inserted_id}'",
                )

    async def _schedule_notification(self, message_id: int, delay_seconds: int) -> None:
        """Schedule a notification to be sent after a delay."""
        await asyncio.sleep(delay_seconds)
        if self._write_pool is None:
            return
        async with self._write_pool.acquire() as conn:
            # Send NOTIFY
            _ = await conn.execute(f"NOTIFY {self.channel_name}, '{message_id}'")

    async def listen(self) -> AsyncGenerator[AckableMessage, None]:
        """
        Listen to the channel.

        Yields messages as they are received.

        :yields: AckableMessage instances.
        """
        if self._write_pool is None:
            msg = "Call startup before starting listening."
            raise ValueError(msg)
        if self._queue is None:
            msg = "Startup did not initialize the queue."
            raise ValueError(msg)

        while True:
            try:
                payload = await self._queue.get()
                message_id = int(payload)
                async with self._write_pool.acquire() as conn:
                    claimed = await conn.fetchrow(
                        CLAIM_MESSAGE_QUERY.format(self.table_name),
                        message_id,
                    )
                if claimed is None:
                    continue
                message_str = claimed["message"]
                if not isinstance(message_str, str):
                    msg = "message is not a string"
                    raise TypeError(msg)
                message_data = message_str.encode()

                async def ack(*, _message_id: int = message_id) -> None:
                    if self._write_pool is None:
                        msg = "Call startup before starting listening."
                        raise ValueError(msg)

                    async with self._write_pool.acquire() as conn:
                        _ = await conn.execute(
                            DELETE_MESSAGE_QUERY.format(self.table_name),
                            _message_id,
                        )

                yield AckableMessage(data=message_data, ack=ack)
            except Exception:
                logger.exception("Error processing message")
                continue
