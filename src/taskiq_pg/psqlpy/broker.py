import asyncio
import logging
import typing as tp
from collections.abc import AsyncGenerator, Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime

import psqlpy
from psqlpy.exceptions import ConnectionExecuteError
from psqlpy.extra_types import JSONB
from taskiq import AckableMessage, AsyncResultBackend, BrokerMessage

from taskiq_pg._internal.broker import BasePostgresBroker
from taskiq_pg._internal.retry import retry_delays
from taskiq_pg._internal.tasks import cancel_and_wait
from taskiq_pg.psqlpy.queries import (
    CLAIM_MESSAGE_QUERY,
    CREATE_MESSAGE_TABLE_QUERY,
    DELETE_MESSAGE_QUERY,
    INSERT_MESSAGE_QUERY,
)


logger = logging.getLogger("taskiq.psqlpy_broker")

_T = tp.TypeVar("_T")


@dataclass
class MessageRow:
    """Message in db table."""

    id: int
    task_id: str
    task_name: str
    message: str
    labels: JSONB
    status: str
    created_at: datetime


class PSQLPyBroker(BasePostgresBroker):
    """Broker that uses PostgreSQL and PSQLPy with LISTEN/NOTIFY."""

    _read_conn: psqlpy.Connection
    _write_pool: psqlpy.ConnectionPool
    _listener: psqlpy.Listener
    _listener_task: asyncio.Task[None] | None = None
    _queue: asyncio.Queue
    _owns_write_pool: bool
    _owns_read_conn: bool

    @tp.overload
    def __init__(
        self,
        dsn: str | Callable[[], str] = ...,
        result_backend: AsyncResultBackend[_T] | None = ...,
        task_id_generator: "Callable[[], str] | None" = ...,
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
        write_pool: psqlpy.ConnectionPool,
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
        read_connection: psqlpy.Connection,
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
        write_pool: psqlpy.ConnectionPool,
        read_connection: psqlpy.Connection,
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
        write_pool: psqlpy.ConnectionPool | None = None,
        read_connection: psqlpy.Connection | None = None,
    ) -> None:
        """
        Construct a new PSQLPyBroker.

        Args:
            dsn: PostgreSQL connection string or callable.
            result_backend: Custom result backend.
            task_id_generator: Custom task_id generator.
            channel_name: Name of the LISTEN/NOTIFY channel.
            table_name: Name of the table used to store messages.
            max_retry_attempts: Maximum number of message processing attempts.
            read_kwargs: Extra kwargs forwarded to `psqlpy.connect()`
            write_kwargs: Extra kwargs forwarded to `psqlpy.ConnectionPool()`
            write_pool: An existing pool to reuse for writes.
            read_connection: An existing connection to reuse for LISTEN.
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

        if self._owns_read_conn:
            self._read_conn = await psqlpy.connect(
                dsn=self.dsn,
                **self.read_kwargs,
            )

        if self._owns_write_pool:
            self._write_pool = psqlpy.ConnectionPool(
                dsn=self.dsn,
                **self.write_kwargs,
            )

        # create messages table if it doesn't exist
        async with self._write_pool.acquire() as conn:
            await conn.execute(CREATE_MESSAGE_TABLE_QUERY.format(self.table_name))

        self._queue = asyncio.Queue()

        # listen to notification channel
        self._listener = await self._start_listener()
        self._listener_task = asyncio.create_task(self._consume_notifications())

    async def shutdown(self) -> None:
        """Close all connections on shutdown."""
        await super().shutdown()
        if self._listener_task is not None:
            await cancel_and_wait(self._listener_task)
            self._listener_task = None
        if self._listener is not None:
            await self._listener.shutdown()
        if self._read_conn is not None and self._owns_read_conn:
            self._read_conn.close()
        if self._write_pool is not None and self._owns_write_pool:
            self._write_pool.close()

    async def _start_listener(self) -> psqlpy.Listener:
        """Create a listener on a pool connection and LISTEN on the channel."""
        listener = self._write_pool.listener()
        try:
            await listener.add_callback(self.channel_name, self._subscribe_only)
            await listener.startup()
        except BaseException:
            with suppress(Exception):
                await listener.shutdown()
            raise
        return listener

    async def _subscribe_only(
        self,
        connection: psqlpy.Connection,
        payload: str,
        channel: str,
        process_id: int,
    ) -> None:
        """
        Subscribe the listener to the channel.

        When a listener is iterated, psqlpy delivers notifications through the iterator
        and does not call callbacks, but a callback is still required to LISTEN on a channel.
        """

    async def _consume_notifications(self) -> None:
        """Move notifications into the queue and restore LISTEN after connection loss."""
        while True:
            try:
                async for message in self._listener:
                    await self._notification_handler(
                        message.connection,
                        message.payload,
                        message.channel,
                        message.process_id,
                    )
            except Exception:
                logger.warning("LISTEN connection lost on channel %s; reconnecting", self.channel_name, exc_info=True)
            else:
                logger.warning("LISTEN stopped on channel %s; reconnecting", self.channel_name)
            with suppress(Exception):
                await self._listener.shutdown()
            self._listener = await self._restore_listener()

    async def _restore_listener(self) -> psqlpy.Listener:
        """Create a new listener, retrying with randomized delays of up to 30 seconds."""
        delays = retry_delays()
        while True:
            try:
                listener = await self._start_listener()
            except Exception:  # noqa: PERF203
                delay = next(delays)
                logger.warning("LISTEN reconnect failed; retrying in %.1f seconds", delay, exc_info=True)
                await asyncio.sleep(delay)
            else:
                logger.info("LISTEN connection restored on channel %s", self.channel_name)
                return listener

    async def _notification_handler(
        self,
        connection: psqlpy.Connection,  # noqa: ARG002
        payload: str,
        channel: str,
        process_id: int,  # noqa: ARG002
    ) -> None:
        """
        Handle NOTIFY messages.

        https://psqlpy-python.github.io/components/listener.html#usage
        """
        logger.debug("Received notification on channel %s: %s", channel, payload)
        if self._queue is not None:
            self._queue.put_nowait(payload)

    async def kick(self, message: BrokerMessage) -> None:
        """
        Send message to the channel.

        Inserts the message into the database and sends a NOTIFY.

        :param message: Message to send.
        """
        async with self._write_pool.acquire() as conn:
            # insert message into db table
            message_inserted_id = tp.cast(
                "int",
                await conn.fetch_val(
                    INSERT_MESSAGE_QUERY.format(self.table_name),
                    [
                        message.task_id,
                        message.task_name,
                        message.message.decode(),
                        JSONB(message.labels),
                    ],
                ),
            )

            delay_value = tp.cast("str | None", message.labels.get("delay"))
            if delay_value is not None:
                delay_seconds = int(delay_value)
                asyncio.create_task(  # noqa: RUF006
                    self._schedule_notification(message_inserted_id, delay_seconds),
                )
            else:
                # Send NOTIFY with message ID as payload
                _ = await conn.execute(
                    f"NOTIFY {self.channel_name}, '{message_inserted_id}'",
                )

    async def _schedule_notification(self, message_id: int, delay_seconds: int) -> None:
        """Schedule a notification to be sent after a delay."""
        await asyncio.sleep(delay_seconds)
        async with self._write_pool.acquire() as conn:
            # Send NOTIFY with message ID as payload
            _ = await conn.execute(f"NOTIFY {self.channel_name}, '{message_id}'")

    async def listen(self) -> AsyncGenerator[AckableMessage, None]:
        """
        Listen to the channel.

        Yields messages as they are received.

        :yields: AckableMessage instances.
        """
        while True:
            try:
                payload = await self._queue.get()
                message_id = int(payload)  # payload is the message id
                try:
                    async with self._write_pool.acquire() as conn:
                        claimed_message = await conn.fetch_row(
                            CLAIM_MESSAGE_QUERY.format(self.table_name),
                            [message_id],
                        )
                except ConnectionExecuteError:  # message was claimed by another worker
                    continue
                message_row_result = tp.cast(
                    "MessageRow",
                    tp.cast("object", claimed_message.as_class(MessageRow)),
                )
                message_data = message_row_result.message.encode()

                async def ack(*, _message_id: int = message_id) -> None:
                    async with self._write_pool.acquire() as conn:
                        _ = await conn.execute(
                            DELETE_MESSAGE_QUERY.format(self.table_name),
                            [_message_id],
                        )

                yield AckableMessage(data=message_data, ack=ack)
            except Exception:
                logger.exception("Error processing message")
                continue
