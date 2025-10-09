from __future__ import annotations

import asyncio
import logging
import typing as tp
from dataclasses import dataclass

import psqlpy
from psqlpy.exceptions import ConnectionExecuteError
from psqlpy.extra_types import JSONB
from taskiq import AckableMessage, BrokerMessage

from taskiq_pg._internal.broker import BasePostgresBroker
from taskiq_pg.psqlpy.queries import (
    CLAIM_MESSAGE_QUERY,
    CREATE_MESSAGE_TABLE_QUERY,
    DELETE_MESSAGE_QUERY,
    INSERT_MESSAGE_QUERY,
)


if tp.TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from datetime import datetime


logger = logging.getLogger("taskiq.asyncpg_broker")


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
    """Broker that uses PostgreSQL and asyncpg with LISTEN/NOTIFY."""

    read_conn: psqlpy.Connection | None = None
    write_pool: psqlpy.ConnectionPool | None = None

    async def startup(self) -> None:
        """Initialize the broker."""
        await super().startup()
        self.read_conn = await psqlpy.connect(
            dsn=self.dsn,
            **self.read_kwargs,
        )
        self.write_pool = psqlpy.ConnectionPool(
            dsn=self.dsn,
            **self.write_kwargs,
        )

        # create messages table if it doesn't exist
        async with self.write_pool.acquire() as conn:
            _ = await conn.execute(CREATE_MESSAGE_TABLE_QUERY.format(self.table_name))

        # listen to notification channel
        listener = self.write_pool.listener()
        await listener.add_callback(self.channel_name, self._notification_handler)
        await listener.startup()
        listener.listen()

        self._queue = asyncio.Queue()

    async def shutdown(self) -> None:
        """Close all connections on shutdown."""
        await super().shutdown()
        if self.read_conn is not None:
            self.read_conn.close()
        if self.write_pool is not None:
            self.write_pool.close()

    async def _notification_handler(
        self,
        connection: psqlpy.Connection,  # noqa: ARG002
        payload: str,
        channel: str,
        process_id: int,  # noqa: ARG002
    ) -> None:
        """
        Handle NOTIFY messages.

        From asyncpg.connection.add_listener docstring:
            A callable or a coroutine function receiving the following arguments:
            **connection**: a Connection the callback is registered with;
            **pid**: PID of the Postgres server that sent the notification;
            **channel**: name of the channel the notification was sent to;
            **payload**: the payload.
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
        if self.write_pool is None:
            msg = "Please run startup before kicking."
            raise ValueError(msg)

        async with self.write_pool.acquire() as conn:
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
                _ = asyncio.create_task(  # noqa: RUF006
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
        if self.write_pool is None:
            msg = "Call startup before starting listening."
            raise ValueError(msg)
        async with self.write_pool.acquire() as conn:
            # Send NOTIFY with message ID as payload
            _ = await conn.execute(f"NOTIFY {self.channel_name}, '{message_id}'")

    async def listen(self) -> AsyncGenerator[AckableMessage, None]:
        """
        Listen to the channel.

        Yields messages as they are received.

        :yields: AckableMessage instances.
        """
        if self.write_pool is None:
            msg = "Call startup before starting listening."
            raise ValueError(msg)
        if self._queue is None:
            msg = "Startup did not initialize the queue."
            raise ValueError(msg)

        while True:
            try:
                payload = await self._queue.get()
                message_id = int(payload)  # payload is the message id
                try:
                    async with self.write_pool.acquire() as conn:
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
                    if self.write_pool is None:
                        msg = "Call startup before starting listening"
                        raise ValueError(msg)

                    async with self.write_pool.acquire() as conn:
                        _ = await conn.execute(
                            DELETE_MESSAGE_QUERY.format(self.table_name),
                            [_message_id],
                        )

                yield AckableMessage(data=message_data, ack=ack)
            except Exception:
                logger.exception("Error processing message")
                continue
