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

from taskiq_pg._internal.broker import LISTEN_KEEPALIVE_QUERY, POLL_NOW, BasePostgresBroker, schema_lock_query
from taskiq_pg._internal.retry import retry_delays
from taskiq_pg._internal.tasks import cancel_and_wait, run_until_first_done
from taskiq_pg.psqlpy.queries import (
    CLAIM_MESSAGE_QUERY,
    CLAIM_NEXT_READY_MESSAGE_QUERY,
    CREATE_MESSAGE_TABLE_QUERY,
    CREATE_PENDING_INDEX_QUERY,
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
    claimed_at: datetime | None = None
    attempts: int = 0


class PSQLPyBroker(BasePostgresBroker):
    """Broker that uses PostgreSQL and PSQLPy with LISTEN/NOTIFY."""

    _write_pool: psqlpy.ConnectionPool
    _listener: psqlpy.Listener
    _listener_task: asyncio.Task[None] | None = None
    _queue: asyncio.Queue
    _owns_write_pool: bool

    def __init__(  # noqa: PLR0913
        self,
        dsn: str | Callable[[], str] = "postgresql://postgres:postgres@localhost:5432/postgres",
        result_backend: AsyncResultBackend[_T] | None = None,
        task_id_generator: Callable[[], str] | None = None,
        channel_name: str = "taskiq",
        table_name: str = "taskiq_messages",
        max_retry_attempts: int = 5,
        write_kwargs: dict[str, tp.Any] | None = None,
        *,
        write_pool: psqlpy.ConnectionPool | None = None,
        listen_keepalive_interval: float | None = 30.0,
        listen_keepalive_timeout: float = 5.0,
        poll_interval: float = 5.0,
        processing_timeout: float | None = None,
    ) -> None:
        """
        Construct a new PSQLPyBroker.

        The broker LISTENs on a connection from the write pool.

        Args:
            dsn: PostgreSQL connection string or callable.
            result_backend: Custom result backend.
            task_id_generator: Custom task_id generator.
            channel_name: Name of the LISTEN/NOTIFY channel.
            table_name: Name of the table used to store messages.
            max_retry_attempts: Maximum number of message processing attempts.
            write_kwargs: Extra kwargs forwarded to `psqlpy.ConnectionPool()`
            write_pool: An existing pool to reuse for writes and LISTEN.
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

    async def startup(self) -> None:
        """Initialize the broker."""
        await super().startup()

        if self._owns_write_pool:
            self._write_pool = psqlpy.ConnectionPool(
                dsn=self.dsn,
                **self.write_kwargs,
            )

        # create messages table if it doesn't exist
        async with self._write_pool.acquire() as conn, conn.transaction() as transaction:
            await transaction.execute(schema_lock_query(self.table_name))
            await transaction.execute(CREATE_MESSAGE_TABLE_QUERY.format(self.table_name))
            await transaction.execute(
                CREATE_PENDING_INDEX_QUERY.format(index=f"{self.table_name}_pending_idx", table=self.table_name),
            )

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
                if self.listen_keepalive_interval is None:
                    await self._move_notifications(self._listener)
                else:
                    await run_until_first_done(self._move_notifications(self._listener), self._keepalive())
            except Exception:
                logger.warning("LISTEN connection lost on channel %s; reconnecting", self.channel_name, exc_info=True)
            else:
                logger.warning("LISTEN stopped on channel %s; reconnecting", self.channel_name)
            with suppress(Exception):
                await self._listener.shutdown()
            self._listener = await self._restore_listener()
            self._queue.put_nowait(POLL_NOW)  # notifications sent while reconnecting were missed

    async def _move_notifications(self, listener: psqlpy.Listener) -> None:
        """Move notifications from the listener into the queue until it stops or its connection is lost."""
        async for message in listener:
            await self._notification_handler(
                message.connection,
                message.payload,
                message.channel,
                message.process_id,
            )

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
        """Run a trivial query on the listener connection."""
        await self._listener.connection.execute(LISTEN_KEEPALIVE_QUERY)

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
        # Parse the delay before inserting, so a malformed label does not leave a message behind.
        delay_value = message.labels.get("delay")
        delay_seconds = None if delay_value is None else int(delay_value)

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

            if delay_seconds is not None:
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
        loop = asyncio.get_running_loop()
        next_poll = loop.time()  # poll right away: messages may have been kicked while nobody listened
        while True:
            try:
                if loop.time() >= next_poll:
                    next_poll = loop.time() + self.poll_interval
                    while (claimed := await self._claim_next_ready()) is not None:
                        yield self._ackable(claimed)
                    continue
                try:
                    payload = await asyncio.wait_for(self._queue.get(), next_poll - loop.time())
                except TimeoutError:
                    continue
                if payload == POLL_NOW:
                    next_poll = loop.time()
                    continue
                claimed = await self._claim(int(payload))  # payload is the message id
                if claimed is not None:
                    yield self._ackable(claimed)
            except Exception:
                logger.exception("Error processing message")
                continue

    async def _claim(self, message_id: int) -> MessageRow | None:
        """Claim a pending message by its id."""
        try:
            async with self._write_pool.acquire() as conn:
                claimed_message = await conn.fetch_row(CLAIM_MESSAGE_QUERY.format(self.table_name), [message_id])
        except ConnectionExecuteError:  # message was claimed by another worker
            return None
        return tp.cast("MessageRow", tp.cast("object", claimed_message.as_class(MessageRow)))

    async def _claim_next_ready(self) -> MessageRow | None:
        """Claim the oldest pending message that is ready (its delay, if any, has elapsed)."""
        async with self._write_pool.acquire() as conn:
            result = await conn.fetch(CLAIM_NEXT_READY_MESSAGE_QUERY.format(table=self.table_name))
        rows = tp.cast("list[MessageRow]", result.as_class(MessageRow))
        return rows[0] if rows else None

    def _ackable(self, claimed: MessageRow) -> AckableMessage:
        """Wrap a claimed message; acknowledging it deletes the row."""
        message_id = claimed.id

        async def ack() -> None:
            async with self._write_pool.acquire() as conn:
                await conn.execute(DELETE_MESSAGE_QUERY.format(self.table_name), [message_id])

        return AckableMessage(data=claimed.message.encode(), ack=ack)
