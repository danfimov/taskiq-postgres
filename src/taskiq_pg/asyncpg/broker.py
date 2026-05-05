import asyncio
import json
import logging
import typing as tp
from collections.abc import AsyncGenerator, Callable

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
            read_connection: An existing connection pool to reuse for LISTEN.
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
        await super().startup()

        if not self._read_conn:
            self._read_conn = await asyncpg.connect(self.dsn, **self.read_kwargs)

        if not self._write_pool:
            self._write_pool = await asyncpg.create_pool(self.dsn, **self.write_kwargs)

        if self._read_conn is None:
            raise RuntimeError("Read connection is not initialized")
        if self._write_pool is None:
            raise RuntimeError("Write pool is not initialized")

        async with self._write_pool.acquire() as conn:
            await conn.execute(CREATE_MESSAGE_TABLE_QUERY.format(self.table_name))

        await self._read_conn.add_listener(self.channel_name, self._notification_handler)
        self._queue = asyncio.Queue()

    async def shutdown(self) -> None:
        """Close all connections on shutdown."""
        await super().shutdown()
        if self._read_conn is not None:
            await self._read_conn.remove_listener(self.channel_name, self._notification_handler)
            if self._owns_read_conn:
                await self._read_conn.close()
        if self._write_pool is not None and self._owns_write_pool:
            await self._write_pool.close()

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
