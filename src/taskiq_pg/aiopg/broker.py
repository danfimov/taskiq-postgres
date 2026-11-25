import asyncio
import json
import logging
import typing as tp
from collections.abc import AsyncGenerator, Callable

import aiopg
from taskiq import AckableMessage, AsyncResultBackend, BrokerMessage

from taskiq_pg._internal.broker import LISTEN_KEEPALIVE_QUERY, BasePostgresBroker
from taskiq_pg._internal.retry import retry_delays
from taskiq_pg._internal.tasks import cancel_and_wait, run_until_first_done
from taskiq_pg.aiopg.queries import (
    CLAIM_MESSAGE_QUERY,
    CREATE_MESSAGE_TABLE_QUERY,
    DELETE_MESSAGE_QUERY,
    INSERT_MESSAGE_QUERY,
)


logger = logging.getLogger("taskiq.aiopg_broker")

_T = tp.TypeVar("_T")


class AiopgBroker(BasePostgresBroker):
    """Broker that uses aiopg as driver and PostgreSQL with LISTEN/NOTIFY mechanism."""

    _read_conn: aiopg.Connection | None = None
    _write_pool: aiopg.Pool | None = None
    _owns_write_pool: bool
    _owns_read_conn: bool
    _listener_task: asyncio.Task[None] | None = None
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
        write_pool: aiopg.Pool,
        read_connection: None = ...,
        listen_keepalive_interval: float | None = ...,
        listen_keepalive_timeout: float = ...,
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
        read_connection: aiopg.Connection,
        listen_keepalive_interval: float | None = ...,
        listen_keepalive_timeout: float = ...,
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
        write_pool: aiopg.Pool,
        read_connection: aiopg.Connection,
        listen_keepalive_interval: float | None = ...,
        listen_keepalive_timeout: float = ...,
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
        write_pool: aiopg.Pool | None = None,
        read_connection: aiopg.Connection | None = None,
        listen_keepalive_interval: float | None = 30.0,
        listen_keepalive_timeout: float = 5.0,
    ) -> None:
        """
        Construct a new AiopgBroker.

        Args:
            dsn: PostgreSQL connection string or a callable returning one.
            result_backend: Custom result backend.
            task_id_generator: Custom task_id generator.
            channel_name: Name of the LISTEN/NOTIFY channel.
            table_name: Name of the table used to store messages.
            max_retry_attempts: Maximum number of message processing attempts.
            read_kwargs: Extra kwargs forwarded to `aiopg.connect()`.
            write_kwargs: Extra kwargs forwarded to `aiopg.create_pool()`.
            write_pool: An existing connection pool to reuse for writes.
            read_connection: An existing connection to reuse for LISTEN.
                Its lifecycle, including reconnection, remains the caller's responsibility.
            listen_keepalive_interval: How often, in seconds, to check the broker-owned LISTEN connection
                with a query; None disables the check.
            listen_keepalive_timeout: How long, in seconds, to wait for the check before reconnecting.
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
        if self._write_pool is None:
            self._write_pool = await aiopg.create_pool(self.dsn, **self.write_kwargs)
        async with self._write_pool.acquire() as conn, conn.cursor() as cursor:
            await cursor.execute(CREATE_MESSAGE_TABLE_QUERY.format(self.table_name))
        if self._owns_read_conn:
            self._read_conn = await self._connect_listener()
        elif self._read_conn is not None:
            await self._execute(self._read_conn, f"LISTEN {self.channel_name}")
        self._listener_task = asyncio.create_task(self._consume_notifications())

    async def shutdown(self) -> None:
        """Close all connections on shutdown."""
        if self._listener_task is not None:
            await cancel_and_wait(self._listener_task)
            self._listener_task = None
        for task in list(self._notify_tasks):
            await cancel_and_wait(task)
        await super().shutdown()
        if self._read_conn is not None:
            if self._owns_read_conn:
                await self._read_conn.close()
                self._read_conn = None
            elif not self._read_conn.closed:
                await self._execute(self._read_conn, f"UNLISTEN {self.channel_name}")
        if self._write_pool is not None and self._owns_write_pool:
            self._write_pool.close()
            await self._write_pool.wait_closed()
            self._write_pool = None

    @staticmethod
    async def _execute(conn: aiopg.Connection, query: str) -> None:
        async with conn.cursor() as cursor:
            await cursor.execute(query)

    async def _connect_listener(self) -> aiopg.Connection:
        """Open a broker-owned connection and LISTEN on the channel."""
        conn = await aiopg.connect(self.dsn, **self.read_kwargs)
        try:
            await self._execute(conn, f"LISTEN {self.channel_name}")
        except BaseException:
            await conn.close()
            raise
        return conn

    async def _consume_notifications(self) -> None:
        """Move notifications into the queue and restore LISTEN after connection loss."""
        while self._read_conn is not None:
            conn = self._read_conn
            try:
                if self.listen_keepalive_interval is None or not self._owns_read_conn:
                    await self._move_notifications(conn)
                else:
                    await run_until_first_done(self._move_notifications(conn), self._keepalive())
            except Exception:
                if not self._owns_read_conn:
                    logger.warning(
                        "Supplied LISTEN connection on channel %s was closed; the broker does not restore it, "
                        "no messages will be received until the caller reconnects",
                        self.channel_name,
                    )
                    return
                logger.warning("LISTEN connection lost on channel %s; reconnecting", self.channel_name, exc_info=True)
            await conn.close()
            self._read_conn = await self._restore_listener()

    async def _move_notifications(self, conn: aiopg.Connection) -> None:
        """Move notifications into the queue until the connection is lost."""
        while True:
            notify = await conn.notifies.get()
            logger.debug("Received notification on channel %s: %s", notify.channel, notify.payload)
            if self._queue is not None:
                self._queue.put_nowait(notify.payload)

    async def _keepalive(self) -> None:
        """Run the keepalive query every interval; raise when it does not answer in time."""
        interval = self.listen_keepalive_interval
        if interval is None:
            return
        while True:
            await asyncio.sleep(interval)
            try:
                await asyncio.wait_for(self._keepalive_query(), self.listen_keepalive_timeout)
            except TimeoutError as exc:
                msg = "LISTEN connection did not answer a keepalive query"
                raise ConnectionError(msg) from exc

    async def _keepalive_query(self) -> None:
        """Run a trivial query on the LISTEN connection."""
        if self._read_conn is not None:
            await self._execute(self._read_conn, LISTEN_KEEPALIVE_QUERY)

    async def _restore_listener(self) -> aiopg.Connection:
        """Open a new LISTEN connection, retrying with randomized delays of up to 30 seconds."""
        delays = retry_delays()
        while True:
            try:
                conn = await self._connect_listener()
            except Exception:  # noqa: PERF203
                delay = next(delays)
                logger.warning("LISTEN reconnect failed; retrying in %.1f seconds", delay, exc_info=True)
                await asyncio.sleep(delay)
            else:
                logger.info("LISTEN connection restored on channel %s", self.channel_name)
                return conn

    async def kick(self, message: BrokerMessage) -> None:
        """
        Send message to the channel.

        Inserts the message into the database and sends a NOTIFY.

        :param message: Message to send.
        """
        if self._write_pool is None:
            msg = "Please run startup before kicking."
            raise ValueError(msg)

        async with self._write_pool.acquire() as conn, conn.cursor() as cursor:
            await cursor.execute(
                INSERT_MESSAGE_QUERY.format(self.table_name),
                (message.task_id, message.task_name, message.message.decode(), json.dumps(message.labels)),
            )
            row = await cursor.fetchone()
            if row is None:
                msg = "failed to insert message"
                raise RuntimeError(msg)
            message_id = int(row[0])

            delay_value = message.labels.get("delay")
            if delay_value is None:
                # Send a NOTIFY with the message ID as payload
                await cursor.execute(f"NOTIFY {self.channel_name}, %s", (str(message_id),))
                return

        task = asyncio.create_task(self._schedule_notification(message_id, int(delay_value)))
        self._notify_tasks.add(task)
        task.add_done_callback(self._notify_tasks.discard)

    async def _schedule_notification(self, message_id: int, delay_seconds: int) -> None:
        """Send a notification after a delay."""
        await asyncio.sleep(delay_seconds)
        if self._write_pool is None:
            return
        async with self._write_pool.acquire() as conn, conn.cursor() as cursor:
            await cursor.execute(f"NOTIFY {self.channel_name}, %s", (str(message_id),))

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
                async with self._write_pool.acquire() as conn, conn.cursor() as cursor:
                    await cursor.execute(CLAIM_MESSAGE_QUERY.format(self.table_name), (message_id,))
                    claimed = await cursor.fetchone()
                if claimed is None:
                    continue
                message_str = claimed[1]
                if not isinstance(message_str, str):
                    msg = "message is not a string"
                    raise TypeError(msg)
                message_data = message_str.encode()

                async def ack(*, _message_id: int = message_id) -> None:
                    if self._write_pool is None:
                        msg = "Call startup before starting listening."
                        raise ValueError(msg)
                    async with self._write_pool.acquire() as conn, conn.cursor() as cursor:
                        await cursor.execute(DELETE_MESSAGE_QUERY.format(self.table_name), (_message_id,))

                yield AckableMessage(data=message_data, ack=ack)
            except Exception:
                logger.exception("Error processing message")
                continue
