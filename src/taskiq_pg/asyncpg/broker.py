from __future__ import annotations

import asyncio
import json
import logging
import typing as tp

import asyncpg
from taskiq import AckableMessage, BrokerMessage

from taskiq_pg._internal.broker import BasePostgresBroker
from taskiq_pg.asyncpg.queries import (
    CLAIM_MESSAGE_QUERY,
    CREATE_MESSAGE_TABLE_QUERY,
    DELETE_MESSAGE_QUERY,
    INSERT_MESSAGE_QUERY,
)


if tp.TYPE_CHECKING:
    from collections.abc import AsyncGenerator


logger = logging.getLogger("taskiq.asyncpg_broker")


class AsyncpgBroker(BasePostgresBroker):
    """Broker that uses asyncpg as driver and PostgreSQL with LISTEN/NOTIFY mechanism."""

    read_conn: asyncpg.Connection[asyncpg.Record] | None = None
    write_pool: asyncpg.pool.Pool[asyncpg.Record] | None = None

    async def startup(self) -> None:
        """Initialize the broker."""
        await super().startup()

        self.read_conn = await asyncpg.connect(self.dsn, **self.read_kwargs)
        self.write_pool = await asyncpg.create_pool(self.dsn, **self.write_kwargs)

        if self.read_conn is None:
            msg = "read_conn not initialized"
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
                async with self.write_pool.acquire() as conn:
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
