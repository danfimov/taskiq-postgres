import asyncio
import typing as tp
import uuid

import asyncpg
import psycopg
import pytest
from taskiq import BrokerMessage

from tests.integration.test_listener_reconnect import listen_backends, wait_for_new_listen_backend

from taskiq_pg.psycopg import PsycopgBroker


@pytest.mark.integration
async def test_when_reconnect_attempts_fail__then_listener_keeps_retrying(
    pg_dsn: str,
    connection: asyncpg.Connection,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # given
    channel = f"reconnect_{uuid.uuid4().hex}"
    broker = PsycopgBroker(dsn=pg_dsn, channel_name=channel)
    await broker.startup()
    connect_listener = broker._connect_listener  # noqa: SLF001
    attempts = 0

    async def temporarily_unavailable() -> None:
        """Fail the first two reconnect attempts, then connect for real."""
        nonlocal attempts
        attempts += 1
        if attempts <= 2:
            msg = "database is temporarily unavailable"
            raise psycopg.OperationalError(msg)
        await connect_listener()

    monkeypatch.setattr(broker, "_connect_listener", temporarily_unavailable)
    listener = broker.listen()
    task: asyncio.Task[tp.Any] = asyncio.create_task(listener.__anext__())
    try:
        original = await listen_backends(connection, channel)

        # when
        await connection.execute("SELECT pg_terminate_backend(pid) FROM unnest($1::int[]) AS pid", list(original))
        await asyncio.wait_for(wait_for_new_listen_backend(connection, channel, original), timeout=10)
        message = BrokerMessage(
            task_id=uuid.uuid4().hex,
            task_name="example:after_retries",
            message=b'{"restored":true}',
            labels={},
        )
        await broker.kick(message)
        done, _ = await asyncio.wait({task}, timeout=5.0)

        # then
        assert attempts == 3
        assert task in done, "Worker should receive a message kicked after reconnection"
        assert task.result().data == message.message
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await broker.shutdown()


@pytest.mark.integration
async def test_when_supplied_read_connection_was_terminated__then_error_is_raised(
    pg_dsn: str,
    connection: asyncpg.Connection,
) -> None:
    # given
    channel = f"reconnect_{uuid.uuid4().hex}"
    read_connection = await psycopg.AsyncConnection.connect(pg_dsn, autocommit=True)
    broker = PsycopgBroker(dsn=pg_dsn, channel_name=channel, read_connection=read_connection)
    await broker.startup()
    listener = broker.listen()
    task: asyncio.Task[tp.Any] = asyncio.create_task(listener.__anext__())
    try:
        original = await listen_backends(connection, channel)

        # when
        await connection.execute("SELECT pg_terminate_backend(pid) FROM unnest($1::int[]) AS pid", list(original))
        done, _ = await asyncio.wait({task}, timeout=5.0)

        # then
        assert task in done, "Listener should fail when a supplied connection is lost"
        assert isinstance(task.exception(), psycopg.OperationalError)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await broker.shutdown()
        await read_connection.close()
