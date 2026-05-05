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

from taskiq_pg._internal.broker import BasePostgresBroker
from taskiq_pg.psycopg.queries import (
    CLAIM_MESSAGE_QUERY,
    CREATE_MESSAGE_TABLE_QUERY,
    DELETE_MESSAGE_QUERY,
    INSERT_MESSAGE_QUERY,
)


logger = logging.getLogger("taskiq.psycopg_broker")

_T = tp.TypeVar("_T")


class PsycopgBroker(BasePostgresBroker):
    """Broker that uses PostgreSQL and psycopg with LISTEN/NOTIFY."""

    _read_conn: AsyncConnection
    _write_pool: AsyncConnectionPool
    _notifies_iter: tp.AsyncIterator[tp.Any]
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
        write_pool: AsyncConnectionPool,
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
        read_connection: AsyncConnection,
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
            self._read_conn = await AsyncConnection.connect(
                conninfo=self.dsn,
                **self.read_kwargs,
                autocommit=True,
                cursor_factory=AsyncRawCursor,
            )

        if self._owns_write_pool:
            self._write_pool = AsyncConnectionPool(
                conninfo=self.dsn if self.dsn is not None else "",
                open=False,
                **self.write_kwargs,
            )
            await self._write_pool.open()

        async with self._write_pool.connection() as connection, connection.cursor() as cursor:
            await cursor.execute(sql.SQL(CREATE_MESSAGE_TABLE_QUERY).format(sql.Identifier(self.table_name)))

        await self._read_conn.execute(sql.SQL("LISTEN {}").format(sql.Identifier(self.channel_name)))
        self._notifies_iter = self._read_conn.notifies()

    async def shutdown(self) -> None:
        """Close all connections on shutdown."""
        await super().shutdown()
        if self._notifies_iter is not None:
            with suppress(RuntimeError):  # RuntimeError: aclose(): asynchronous generator is already running
                await self._notifies_iter.aclose()  # type: ignore[attr-defined]
        if self._read_conn is not None and self._owns_read_conn:
                await self._read_conn.notifies().aclose()
                await self._read_conn.close()
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
            if delay_value is not None:
                delay_seconds = int(delay_value)
                await self._schedule_notification(message_inserted_id, delay_seconds)
            else:
                # Send NOTIFY with message ID as payload
                await cursor.execute(
                    sql.SQL("NOTIFY {}, {}").format(
                        sql.Identifier(self.channel_name),
                        sql.Literal(str(message_inserted_id)),
                    ),
                )

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

    async def _listen_context(self) -> AsyncGenerator[str, None]:
        async for notify in self._notifies_iter:
            yield notify.payload

    async def listen(self) -> AsyncGenerator[AckableMessage, None]:
        """
        Listen to the channel.

        Yields messages as they are received.

        :yields: AckableMessage instances.
        """
        while True:
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
