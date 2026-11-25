import asyncio
import json
import logging
import typing as tp
from collections.abc import AsyncGenerator, Callable

import asyncpg
from taskiq import AckableMessage, AsyncResultBackend, BrokerMessage

from taskiq_pg._internal.broker import (
    LISTEN_KEEPALIVE_QUERY,
    POLL_NOW,
    SCHEMA_STATUS_QUERY,
    BasePostgresBroker,
    schema_lock_query,
)
from taskiq_pg._internal.retry import retry_delays
from taskiq_pg._internal.tasks import cancel_and_wait
from taskiq_pg.asyncpg.queries import (
    ADD_DELIVERY_COLUMNS_QUERY,
    CLAIM_MESSAGE_QUERY,
    CLAIM_NEXT_READY_MESSAGE_QUERY,
    CREATE_MESSAGE_TABLE_QUERY,
    CREATE_PENDING_INDEX_QUERY,
    CREATE_PROCESSING_INDEX_QUERY,
    DELETE_MESSAGE_QUERY,
    FAIL_EXHAUSTED_MESSAGES_QUERY,
    INSERT_MESSAGE_QUERY,
    RELEASE_STALE_MESSAGES_QUERY,
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
    _notify_tasks: set[asyncio.Task[None]]

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
        processing_timeout: float | None = ...,
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
        listen_keepalive_interval: float | None = ...,
        listen_keepalive_timeout: float = ...,
        poll_interval: float = ...,
        processing_timeout: float | None = ...,
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
        listen_keepalive_interval: float | None = ...,
        listen_keepalive_timeout: float = ...,
        poll_interval: float = ...,
        processing_timeout: float | None = ...,
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
        listen_keepalive_interval: float | None = ...,
        listen_keepalive_timeout: float = ...,
        poll_interval: float = ...,
        processing_timeout: float | None = ...,
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
        listen_keepalive_interval: float | None = 30.0,
        listen_keepalive_timeout: float = 5.0,
        poll_interval: float = 5.0,
        processing_timeout: float | None = None,
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
            listen_keepalive_interval: How often, in seconds, to check the broker-owned LISTEN connection
                with a query; None disables the check.
            listen_keepalive_timeout: How long, in seconds, to wait for the check before reconnecting.
            poll_interval: How often, in seconds, to look for pending messages whose NOTIFY was missed.
            processing_timeout: Seconds after which an unacknowledged claimed message is delivered again
                (at-least-once delivery); None keeps at-most-once delivery.
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
            processing_timeout=processing_timeout,
        )

        self._owns_write_pool = True
        if write_pool is not None:
            self._write_pool = write_pool
            self._owns_write_pool = False

        self._owns_read_conn = True
        if read_connection is not None:
            self._read_conn = read_connection
            self._owns_read_conn = False

        self._notify_tasks = set()

    async def startup(self) -> None:
        """Initialize the broker."""
        await super().startup()
        self._queue = asyncio.Queue()
        self._listener_disconnected = asyncio.Event()
        if self._write_pool is None:
            self._write_pool = await asyncpg.create_pool(self.dsn, **self.write_kwargs)
        async with self._write_pool.acquire() as conn, conn.transaction():
            await conn.execute(schema_lock_query(self.table_name))
            status = await conn.fetchrow(
                SCHEMA_STATUS_QUERY.format(table="$1", pending_index="$2", processing_index="$3"),
                self.table_name,
                f"{self.table_name}_pending_idx",
                f"{self.table_name}_processing_idx",
            )
            if status is None:
                msg = "schema status query returned no row"
                raise RuntimeError(msg)
            if not status["has_table"]:
                await conn.execute(CREATE_MESSAGE_TABLE_QUERY.format(self.table_name))
            if not status["has_delivery_columns"]:
                await conn.execute(ADD_DELIVERY_COLUMNS_QUERY.format(table=self.table_name))
            if not status["has_pending_index"]:
                await conn.execute(
                    CREATE_PENDING_INDEX_QUERY.format(index=f"{self.table_name}_pending_idx", table=self.table_name),
                )
            if self.processing_timeout is not None and not status["has_processing_index"]:
                await conn.execute(
                    CREATE_PROCESSING_INDEX_QUERY.format(
                        index=f"{self.table_name}_processing_idx",
                        table=self.table_name,
                    ),
                )
        await self._connect_listener()
        self._listener_task = asyncio.create_task(self._restore_listener())

    async def shutdown(self) -> None:
        """Close all connections on shutdown."""
        if self._listener_task is not None:
            await cancel_and_wait(self._listener_task)
            self._listener_task = None
        for task in list(self._notify_tasks):
            await cancel_and_wait(task)
        await super().shutdown()
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
        if conn is not self._read_conn:
            return
        if self._owns_read_conn:
            self._listener_disconnected.set()
        else:
            logger.warning(
                "Supplied LISTEN connection on channel %s was closed; the broker does not restore it, "
                "no messages will be received until the caller reconnects",
                self.channel_name,
            )

    async def _restore_listener(self) -> None:
        """Restore LISTEN after disconnection, retrying with randomized delays of up to 30 seconds."""
        while True:
            await self._wait_for_disconnection()
            delays = retry_delays()
            while True:
                try:
                    await self._connect_listener()
                except Exception:  # noqa: PERF203
                    delay = next(delays)
                    logger.warning("LISTEN reconnect failed; retrying in %.1f seconds", delay, exc_info=True)
                    await asyncio.sleep(delay)
                else:
                    logger.info("LISTEN connection restored on channel %s", self.channel_name)
                    if self._queue is not None:  # notifications sent while reconnecting were missed
                        self._queue.put_nowait(POLL_NOW)
                    break

    async def _wait_for_disconnection(self) -> None:
        """Wait until the LISTEN connection is closed or, if it is broker-owned, stops answering keepalive queries."""
        interval = self.listen_keepalive_interval
        if interval is None or not self._owns_read_conn:
            await self._listener_disconnected.wait()
            return
        while True:
            try:
                await asyncio.wait_for(self._listener_disconnected.wait(), interval)
            except TimeoutError:
                pass
            else:
                return
            conn = self._read_conn
            if conn is None:
                return
            try:
                await asyncio.wait_for(self._keepalive_query(), self.listen_keepalive_timeout)
            except Exception:
                logger.warning(
                    "LISTEN connection on channel %s did not answer a keepalive query; reconnecting",
                    self.channel_name,
                    exc_info=True,
                )
                conn.remove_termination_listener(self._connection_terminated)
                conn.terminate()
                return

    async def _keepalive_query(self) -> None:
        """Run a trivial query on the LISTEN connection."""
        if self._read_conn is not None:
            await self._read_conn.fetchval(LISTEN_KEEPALIVE_QUERY)

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

        # Parse the delay before inserting, so a malformed label does not leave a message behind.
        delay_value = message.labels.get("delay")
        delay_seconds = None if delay_value is None else int(delay_value)

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

            if delay_seconds is not None:
                # Keep a reference, so the task is not garbage collected, and cancel it on shutdown.
                task = asyncio.create_task(self._schedule_notification(message_inserted_id, delay_seconds))
                self._notify_tasks.add(task)
                task.add_done_callback(self._notify_tasks.discard)
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

        queue = self._queue
        loop = asyncio.get_running_loop()
        next_poll = loop.time()  # poll right away: messages may have been kicked while nobody listened
        while True:
            try:
                if loop.time() >= next_poll:
                    next_poll = loop.time() + self.poll_interval
                    await self._release_stale_messages()
                    while (claimed := await self._claim_next_ready()) is not None:
                        yield self._ackable(claimed)
                    continue
                try:
                    payload = await asyncio.wait_for(queue.get(), next_poll - loop.time())
                except TimeoutError:
                    continue
                if payload == POLL_NOW:
                    next_poll = loop.time()
                    continue
                claimed = await self._claim(int(payload))
                if claimed is not None:
                    yield self._ackable(claimed)
            except Exception:
                logger.exception("Error processing message")
                continue

    async def _claim(self, message_id: int) -> asyncpg.Record | None:
        """Claim a pending message by its id."""
        if self._write_pool is None:
            msg = "Call startup before starting listening."
            raise ValueError(msg)
        async with self._write_pool.acquire() as conn:
            return await conn.fetchrow(CLAIM_MESSAGE_QUERY.format(self.table_name), message_id)

    async def _claim_next_ready(self) -> asyncpg.Record | None:
        """Claim the oldest pending message that is ready (its delay, if any, has elapsed)."""
        if self._write_pool is None:
            msg = "Call startup before starting listening."
            raise ValueError(msg)
        async with self._write_pool.acquire() as conn:
            return await conn.fetchrow(CLAIM_NEXT_READY_MESSAGE_QUERY.format(table=self.table_name))

    async def _release_stale_messages(self) -> None:
        """
        With processing_timeout set, return messages claimed too long ago to the queue.

        A message whose attempts are exhausted is marked as failed instead.
        """
        if self.processing_timeout is None or self._write_pool is None:
            return
        async with self._write_pool.acquire() as conn:
            await conn.execute(
                RELEASE_STALE_MESSAGES_QUERY.format(table=self.table_name),
                self.processing_timeout,
                self.max_retry_attempts,
            )
            failed = await conn.fetch(
                FAIL_EXHAUSTED_MESSAGES_QUERY.format(table=self.table_name),
                self.processing_timeout,
                self.max_retry_attempts,
            )
        for row in failed:
            logger.warning(
                "Message %s (task %s, %s) was not acknowledged after %s attempts; marked as failed",
                row["id"],
                row["task_id"],
                row["task_name"],
                row["attempts"],
            )

    def _ackable(self, claimed: asyncpg.Record) -> AckableMessage:
        """Wrap a claimed message; acknowledging it deletes the row."""
        message_str = claimed["message"]
        if not isinstance(message_str, str):
            msg = "message is not a string"
            raise TypeError(msg)
        message_id = int(claimed["id"])
        attempts = int(claimed["attempts"])

        async def ack() -> None:
            if self._write_pool is None:
                msg = "Call startup before starting listening."
                raise ValueError(msg)
            async with self._write_pool.acquire() as conn:
                await conn.execute(DELETE_MESSAGE_QUERY.format(self.table_name), message_id, attempts)

        return AckableMessage(data=message_str.encode(), ack=ack)
