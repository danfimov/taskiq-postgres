import asyncio
import typing as tp
import uuid

import asyncpg
import pytest
from taskiq import BrokerMessage

from tests.integration.test_listener_reconnect import listen_backends

from taskiq_pg.aiopg import AiopgBroker
from taskiq_pg.asyncpg import AsyncpgBroker
from taskiq_pg.psqlpy import PSQLPyBroker
from taskiq_pg.psycopg import PsycopgBroker


Broker: tp.TypeAlias = AsyncpgBroker | PsycopgBroker | PSQLPyBroker | AiopgBroker

FAST_KEEPALIVE: dict[str, tp.Any] = {"listen_keepalive_interval": 0.2, "listen_keepalive_timeout": 0.5}


# Make the server close the LISTEN session after one second without client queries.
async def alive(connection: asyncpg.Connection, pids: set[int]) -> set[int]:
    """Return which of the backends still exist (a LISTEN backend's last query may be the keepalive query)."""
    rows = await connection.fetch("SELECT pid FROM pg_stat_activity WHERE pid = ANY($1::int[])", list(pids))
    return {row["pid"] for row in rows}


async def wait_until_gone(connection: asyncpg.Connection, pids: set[int]) -> None:
    while await alive(connection, pids):  # noqa: ASYNC110
        await asyncio.sleep(0.05)


IDLE_TIMEOUT_READ_KWARGS: list[tp.Any] = [
    pytest.param(AsyncpgBroker, {"server_settings": {"idle_session_timeout": "1000"}}, id="AsyncpgBroker"),
    pytest.param(PsycopgBroker, {"options": "-c idle_session_timeout=1000"}, id="PsycopgBroker"),
    pytest.param(AiopgBroker, {"options": "-c idle_session_timeout=1000"}, id="AiopgBroker"),
]


@pytest.mark.integration
@pytest.mark.parametrize("broker_class", [AsyncpgBroker, PsycopgBroker, PSQLPyBroker, AiopgBroker])
async def test_when_keepalive_query_hangs__then_listener_reconnects(
    pg_dsn: str,
    connection: asyncpg.Connection,
    monkeypatch: pytest.MonkeyPatch,
    broker_class: type[Broker],
) -> None:
    # given
    channel = f"keepalive_{uuid.uuid4().hex}"
    broker = broker_class(dsn=pg_dsn, channel_name=channel, **FAST_KEEPALIVE)
    keepalive_query = broker._keepalive_query  # noqa: SLF001
    hung = asyncio.Event()

    async def hang_once() -> None:
        """Hang like a connection that silently stopped answering, then behave normally."""
        if not hung.is_set():
            hung.set()
            await asyncio.Future()
        await keepalive_query()

    monkeypatch.setattr(broker, "_keepalive_query", hang_once)
    await broker.startup()
    listener = broker.listen()
    task: asyncio.Task[tp.Any] = asyncio.create_task(listener.__anext__())
    try:
        original = await listen_backends(connection, channel)

        assert original

        # when
        await asyncio.wait_for(hung.wait(), timeout=5)
        await asyncio.wait_for(wait_until_gone(connection, original), timeout=5)
        message = BrokerMessage(
            task_id=uuid.uuid4().hex,
            task_name="example:after_keepalive",
            message=b'{"restored":true}',
            labels={},
        )
        await broker.kick(message)
        done, _ = await asyncio.wait({task}, timeout=5.0)

        # then
        assert task in done, "Worker should receive a message kicked after the hung connection was replaced"
        assert task.result().data == message.message
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await broker.shutdown()


async def _listen_backends_after_idle_period(
    pg_dsn: str,
    connection: asyncpg.Connection,
    broker_class: type[Broker],
    read_kwargs: dict[str, tp.Any],
    keepalive_interval: float | None,
) -> tuple[set[int], set[int]]:
    """Return the LISTEN backends right after startup and which of them survived the server's idle timeout twice."""
    channel = f"keepalive_{uuid.uuid4().hex}"
    broker = broker_class(
        dsn=pg_dsn,
        channel_name=channel,
        read_kwargs=read_kwargs,
        listen_keepalive_interval=keepalive_interval,
        listen_keepalive_timeout=1.0,
    )
    await broker.startup()
    listener = broker.listen()
    task: asyncio.Task[tp.Any] = asyncio.create_task(listener.__anext__())
    try:
        original = await listen_backends(connection, channel)
        await asyncio.sleep(2.5)
        return original, await alive(connection, original)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await broker.shutdown()


@pytest.mark.integration
@pytest.mark.parametrize(("broker_class", "read_kwargs"), IDLE_TIMEOUT_READ_KWARGS)
async def test_when_server_closes_idle_sessions__then_keepalive_keeps_listen_connection_open(
    pg_dsn: str,
    connection: asyncpg.Connection,
    broker_class: type[Broker],
    read_kwargs: dict[str, tp.Any],
) -> None:
    # when
    original, after = await _listen_backends_after_idle_period(pg_dsn, connection, broker_class, read_kwargs, 0.3)

    # then
    assert original
    assert after == original, "The LISTEN connection should survive the server's idle timeout"


@pytest.mark.integration
@pytest.mark.parametrize(("broker_class", "read_kwargs"), IDLE_TIMEOUT_READ_KWARGS)
async def test_when_server_closes_idle_sessions_without_keepalive__then_listen_connection_is_closed(
    pg_dsn: str,
    connection: asyncpg.Connection,
    broker_class: type[Broker],
    read_kwargs: dict[str, tp.Any],
) -> None:
    """Control case: proves that the idle timeout in read_kwargs really closes the LISTEN connection."""
    # when
    original, after = await _listen_backends_after_idle_period(pg_dsn, connection, broker_class, read_kwargs, None)

    # then
    assert original
    assert not after, "Without keepalive the server should have closed the idle LISTEN connection"
