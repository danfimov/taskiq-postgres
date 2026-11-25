import asyncio
import typing as tp
import uuid

import asyncpg
import pytest
from taskiq import BrokerMessage

from taskiq_pg.aiopg import AiopgBroker
from taskiq_pg.asyncpg import AsyncpgBroker
from taskiq_pg.psqlpy import PSQLPyBroker
from taskiq_pg.psycopg import PsycopgBroker


LISTEN_BACKENDS_QUERY = """
SELECT pid FROM pg_stat_activity
WHERE query ILIKE 'LISTEN%' AND query LIKE '%' || $1 || '%' AND state = 'idle' AND pid <> pg_backend_pid()
"""


async def listen_backends(connection: asyncpg.Connection, channel: str) -> set[int]:
    """Return PIDs of the backends that have issued LISTEN on the channel."""
    return {row["pid"] for row in await connection.fetch(LISTEN_BACKENDS_QUERY, channel)}


async def wait_for_new_listen_backend(connection: asyncpg.Connection, channel: str, previous: set[int]) -> None:
    """Wait until another backend issues LISTEN on the channel."""
    while not (await listen_backends(connection, channel) - previous):  # noqa: ASYNC110
        await asyncio.sleep(0.05)


@pytest.mark.integration
@pytest.mark.parametrize(
    "broker_class",
    [
        AsyncpgBroker,
        PsycopgBroker,
        PSQLPyBroker,
        AiopgBroker,
    ],
)
async def test_when_listen_connection_was_terminated__then_worker_received_next_message(
    pg_dsn: str,
    connection: asyncpg.Connection,
    broker_class: type[AiopgBroker | AsyncpgBroker | PsycopgBroker | PSQLPyBroker],
) -> None:
    # given
    channel = f"reconnect_{uuid.uuid4().hex}"
    broker = broker_class(dsn=pg_dsn, channel_name=channel)
    await broker.startup()
    listener = broker.listen()
    task: asyncio.Task[tp.Any] = asyncio.create_task(listener.__anext__())
    try:
        original = await listen_backends(connection, channel)
        assert original, "Broker should LISTEN on its channel after startup"

        # when
        await connection.execute("SELECT pg_terminate_backend(pid) FROM unnest($1::int[]) AS pid", list(original))
        await asyncio.wait_for(wait_for_new_listen_backend(connection, channel, original), timeout=5)
        message = BrokerMessage(
            task_id=uuid.uuid4().hex,
            task_name="example:after_reconnect",
            message=b'{"restored":true}',
            labels={},
        )
        await broker.kick(message)
        done, _ = await asyncio.wait({task}, timeout=5.0)

        # then
        assert task in done, "Worker should receive a message kicked after reconnection"
        assert task.result().data == message.message
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await broker.shutdown()
