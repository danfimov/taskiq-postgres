import asyncio
import typing as tp
import uuid

import asyncpg
import psqlpy
import pytest
from taskiq import BrokerMessage

from tests.integration.test_listener_reconnect import listen_backends, wait_for_new_listen_backend

from taskiq_pg.psqlpy import PSQLPyBroker


@pytest.mark.integration
async def test_when_reconnect_attempts_fail__then_listener_keeps_retrying(
    pg_dsn: str,
    connection: asyncpg.Connection,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # given
    channel = f"reconnect_{uuid.uuid4().hex}"
    broker = PSQLPyBroker(dsn=pg_dsn, channel_name=channel)
    await broker.startup()
    start_listener = broker._start_listener  # noqa: SLF001
    attempts = 0

    async def temporarily_unavailable() -> psqlpy.Listener:
        """Fail the first two reconnect attempts, then start a real listener."""
        nonlocal attempts
        attempts += 1
        if attempts <= 2:
            msg = "database is temporarily unavailable"
            raise OSError(msg)
        return await start_listener()

    monkeypatch.setattr(broker, "_start_listener", temporarily_unavailable)
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
async def test_when_shutdown_during_reconnect__then_reconnect_is_cancelled(
    pg_dsn: str,
    connection: asyncpg.Connection,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # given
    channel = f"reconnect_{uuid.uuid4().hex}"
    broker = PSQLPyBroker(dsn=pg_dsn, channel_name=channel)
    await broker.startup()
    connecting = asyncio.Event()
    cancelled = asyncio.Event()

    async def unavailable() -> psqlpy.Listener:
        """Hang like a connection attempt to an unreachable server."""
        connecting.set()
        try:
            return await asyncio.Future()
        finally:
            cancelled.set()

    monkeypatch.setattr(broker, "_start_listener", unavailable)
    original = await listen_backends(connection, channel)
    await connection.execute("SELECT pg_terminate_backend(pid) FROM unnest($1::int[]) AS pid", list(original))
    await asyncio.wait_for(connecting.wait(), timeout=5)

    # when
    await asyncio.wait_for(broker.shutdown(), timeout=5)

    # then
    assert cancelled.is_set()
