from __future__ import annotations

import asyncio
import json
import logging
import typing as tp

import asyncpg
from taskiq import AckableMessage, AsyncBroker, AsyncResultBackend, BrokerMessage

from taskiq_pg.asyncpg.queries import (
    CREATE_MESSAGE_TABLE_QUERY,
    DELETE_MESSAGE_QUERY,
    INSERT_MESSAGE_QUERY,
    SELECT_MESSAGE_QUERY,
)


if tp.TYPE_CHECKING:
    from collections.abc import AsyncGenerator


_T = tp.TypeVar("_T")
logger = logging.getLogger("taskiq.asyncpg_broker")


class AsyncpgBroker(AsyncBroker):
    """Broker that uses PostgreSQL and asyncpg with LISTEN/NOTIFY."""

    def __init__(
        self,
        dsn: str | tp.Callable[[], str] = "postgresql://postgres:postgres@localhost:5432/postgres",
        result_backend: AsyncResultBackend[_T] | None = None,
        task_id_generator: tp.Callable[[], str] | None = None,
        channel_name: str = "taskiq",
        table_name: str = "taskiq_messages",
        max_retry_attempts: int = 5,
        connection_kwargs: dict[str, tp.Any] | None = None,
        pool_kwargs: dict[str, tp.Any] | None = None,
    ) -> None:
        """
        Construct a new broker.

        :param dsn: connection string to PostgreSQL, or callable returning one.
        :param result_backend: Custom result backend.
        :param task_id_generator: Custom task_id generator.
        :param channel_name: Name of the channel to listen on.
        :param table_name: Name of the table to store messages.
        :param max_retry_attempts: Maximum number of message processing attempts.
        :param connection_kwargs: Additional arguments for asyncpg connection.
        :param pool_kwargs: Additional arguments for asyncpg pool creation.
        """
        super().__init__(
            result_backend=result_backend,
            task_id_generator=task_id_generator,
        )
        self._dsn: str | tp.Callable[[], str] = dsn
        self.channel_name: str = channel_name
        self.table_name: str = table_name
        self.connection_kwargs: dict[str, tp.Any] = (
            connection_kwargs if connection_kwargs else {}
        )
        self.pool_kwargs: dict[str, tp.Any] = pool_kwargs if pool_kwargs else {}
        self.max_retry_attempts: int = max_retry_attempts
        self.read_conn: asyncpg.Connection[asyncpg.Record] | None = None
        self.write_pool: asyncpg.pool.Pool[asyncpg.Record] | None = None
        self._queue: asyncio.Queue[str] | None = None

    @property
    def dsn(self) -> str:
        """
        Get the DSN string.

        Returns the DSN string or None if not set.
        """
        if callable(self._dsn):
            return self._dsn()
        return self._dsn

    async def startup(self) -> None:
        """Initialize the broker."""
        await super().startup()

        self.read_conn = await asyncpg.connect(self.dsn, **self.connection_kwargs)
        self.write_pool = await asyncpg.create_pool(self.dsn, **self.pool_kwargs)

        if self.read_conn is None:
            msg = "read_conn not initialized"
            raise RuntimeError(msg)
        if self.write_pool is None:
            msg = "write_pool not initialized"
            raise RuntimeError(msg)

        async with self.write_pool.acquire() as conn:
            _ = await conn.execute(CREATE_MESSAGE_TABLE_QUERY.format(self.table_name))

        await self.read_conn.add_listener(self.channel_name, self._notification_handler)
        self._queue = asyncio.Queue()

    async def shutdown(self) -> None:
        """Close all connections on shutdown."""
        await super().shutdown()
        if self.read_conn is not None:
            await self.read_conn.close()
        if self.write_pool is not None:
            await self.write_pool.close()

    def _notification_handler(
        self,
        con_ref: asyncpg.Connection[asyncpg.Record] | asyncpg.pool.PoolConnectionProxy[asyncpg.Record],  # noqa: ARG002
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
        if self.write_pool is None:
            msg = "Please run startup before kicking."
            raise ValueError(msg)

        async with self.write_pool.acquire() as conn:
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
        if self.write_pool is None:
            return
        async with self.write_pool.acquire() as conn:
            # Send NOTIFY
            _ = await conn.execute(f"NOTIFY {self.channel_name}, '{message_id}'")

    async def listen(self) -> AsyncGenerator[AckableMessage, None]:
        """
        Listen to the channel.

        Yields messages as they are received.

        :yields: AckableMessage instances.
        """
        if self.read_conn is None:
            msg = "Call startup before starting listening."
            raise ValueError(msg)
        if self._queue is None:
            msg = "Startup did not initialize the queue."
            raise ValueError(msg)

        while True:
            try:
                payload = await self._queue.get()
                message_id = int(payload)
                message_row = await self.read_conn.fetchrow(
                    SELECT_MESSAGE_QUERY.format(self.table_name), message_id,
                )
                if message_row is None:
                    logger.warning(
                        "Message with id %s not found in database.", message_id,
                    )
                    continue
                if message_row.get("message") is None:
                    msg = "Message row does not have 'message' column"
                    raise ValueError(msg)
                message_str = message_row["message"]
                if not isinstance(message_str, str):
                    msg = "message is not a string"
                    raise TypeError(msg)
                message_data = message_str.encode()

                async def ack(*, _message_id: int = message_id) -> None:
                    if self.write_pool is None:
                        msg = "Call startup before starting listening."
                        raise ValueError(msg)

                    async with self.write_pool.acquire() as conn:
                        _ = await conn.execute(
                            DELETE_MESSAGE_QUERY.format(self.table_name),
                            _message_id,
                        )

                yield AckableMessage(data=message_data, ack=ack)
            except Exception:
                logger.exception("Error processing message")
                continue
