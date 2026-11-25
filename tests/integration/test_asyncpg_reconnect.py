import asyncio
import typing as tp
import uuid
from urllib.parse import urlsplit

import asyncpg
import pytest
from taskiq import BrokerMessage

from taskiq_pg.asyncpg import AsyncpgBroker


if tp.TYPE_CHECKING:
    from collections.abc import Callable


async def wait_for_listener(broker: AsyncpgBroker, previous: asyncpg.Connection) -> None:
    """Wait for a new connection with a restored LISTEN subscription."""

    async def wait() -> None:
        """Poll until the subscription is restored."""
        while True:
            conn = broker._read_conn  # noqa: SLF001
            if conn is not None and conn is not previous and not conn.is_closed():
                channels = await conn.fetch("SELECT pg_listening_channels() AS channel")
                if any(row["channel"] == broker.channel_name for row in channels):
                    return
            await asyncio.sleep(0.01)

    await asyncio.wait_for(wait(), timeout=10)


@pytest.mark.integration
@pytest.mark.parametrize("failures", [0, 2])
async def test_listener_receives_messages_after_connection_loss(
    pg_dsn: str,
    connection: asyncpg.Connection,
    monkeypatch: pytest.MonkeyPatch,
    failures: int,
) -> None:
    """Resume receiving tasks after disconnection, including temporary database unavailability."""
    broker = AsyncpgBroker(dsn=pg_dsn)
    await broker.startup()
    original = broker._read_conn  # noqa: SLF001
    assert original is not None
    connect = asyncpg.connect
    attempts = 0

    async def temporarily_unavailable(*args: tp.Any, **kwargs: tp.Any) -> asyncpg.Connection:
        """Simulate connection failures, then open a real connection."""
        nonlocal attempts
        attempts += 1
        if attempts <= failures:
            raise ConnectionRefusedError
        return await connect(*args, **kwargs)

    monkeypatch.setattr(asyncpg, "connect", temporarily_unavailable)
    try:
        await connection.execute("SELECT pg_terminate_backend($1)", original.get_server_pid())
        await wait_for_listener(broker, original)
        message = BrokerMessage(
            task_id=uuid.uuid4().hex,
            task_name="test:reconnect",
            message=b'{"restored":true}',
            labels={},
        )
        await broker.kick(message)
        listener = broker.listen()
        try:
            received = await asyncio.wait_for(anext(listener), timeout=5)
            assert received.data == message.message
            acknowledgement = received.ack()
            assert acknowledgement is not None
            await acknowledgement
        finally:
            await listener.aclose()
    finally:
        await broker.shutdown()


@pytest.mark.integration
async def test_shutdown_cancels_pending_reconnect(
    pg_dsn: str,
    connection: asyncpg.Connection,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stop the broker during reconnection without leaving a background task."""
    broker = AsyncpgBroker(dsn=pg_dsn)
    await broker.startup()
    original = broker._read_conn  # noqa: SLF001
    assert original is not None
    connecting = asyncio.Event()
    cancelled = asyncio.Event()

    async def unavailable(*_args: object, **_kwargs: object) -> asyncpg.Connection:
        """Wait for cancellation while connecting to an unavailable server."""
        connecting.set()
        try:
            return await asyncio.Future[asyncpg.Connection]()
        finally:
            cancelled.set()

    monkeypatch.setattr(asyncpg, "connect", unavailable)
    try:
        await connection.execute("SELECT pg_terminate_backend($1)", original.get_server_pid())
        await asyncio.wait_for(connecting.wait(), timeout=5)
    finally:
        await asyncio.wait_for(broker.shutdown(), timeout=5)
    assert cancelled.is_set()


@pytest.mark.integration
async def test_listener_recovers_when_resubscription_fails(
    pg_dsn: str,
    connection: asyncpg.Connection,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retry LISTEN after a subscription failure and close the failed connection."""
    broker = AsyncpgBroker(dsn=pg_dsn)
    await broker.startup()
    original = broker._read_conn  # noqa: SLF001
    assert original is not None
    add_listener = asyncpg.Connection.add_listener
    failed_connections: list[asyncpg.Connection] = []

    async def fail_once(
        conn: asyncpg.Connection,
        channel: str,
        callback: "Callable[[asyncpg.Connection | asyncpg.pool.PoolConnectionProxy, int, str, object], None]",
    ) -> None:
        """Fail the first subscription attempt, then execute a real LISTEN."""
        if not failed_connections:
            failed_connections.append(conn)
            raise asyncpg.ConnectionDoesNotExistError
        await add_listener(conn, channel, callback)

    monkeypatch.setattr(asyncpg.Connection, "add_listener", fail_once)
    try:
        for _ in range(2):
            await connection.execute("SELECT pg_terminate_backend($1)", original.get_server_pid())
            await wait_for_listener(broker, original)
            original = broker._read_conn  # noqa: SLF001
            assert original is not None
            queue = broker._queue  # noqa: SLF001
            assert queue is not None
            await connection.execute(f"NOTIFY {broker.channel_name}, '42'")
            assert await asyncio.wait_for(queue.get(), timeout=5) == "42"
        assert len(failed_connections) == 1
        assert failed_connections[0].is_closed()
    finally:
        await broker.shutdown()


@pytest.mark.integration
async def test_listener_keeps_retrying_after_unexpected_error(
    pg_dsn: str,
    connection: asyncpg.Connection,
) -> None:
    """Keep restoring LISTEN when a reconnect attempt fails with an error outside asyncpg."""
    broker = AsyncpgBroker(dsn=pg_dsn)
    await broker.startup()
    original = broker._read_conn  # noqa: SLF001
    assert original is not None
    password = urlsplit(pg_dsn).password
    calls = 0

    def flaky_password() -> str | None:
        """Fail like a broken secret provider once, then return the real password."""
        nonlocal calls
        calls += 1
        if calls == 1:
            msg = "Password provider failed"
            raise RuntimeError(msg)
        return password

    broker.read_kwargs["password"] = flaky_password
    try:
        await connection.execute("SELECT pg_terminate_backend($1)", original.get_server_pid())
        await wait_for_listener(broker, original)
        assert calls == 2
        listener_task = broker._listener_task  # noqa: SLF001
        assert listener_task is not None
        assert not listener_task.done()
    finally:
        await broker.shutdown()


SUPPLIED_CONNECTION_CLOSED = "Supplied LISTEN connection"


@pytest.mark.integration
async def test_when_supplied_read_connection_was_terminated__then_warning_is_logged(
    pg_dsn: str,
    connection: asyncpg.Connection,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Warn that a supplied LISTEN connection is lost, since the broker does not restore it."""
    read_connection = await asyncpg.connect(pg_dsn)
    broker = AsyncpgBroker(dsn=pg_dsn, read_connection=read_connection)
    await broker.startup()
    try:
        with caplog.at_level("WARNING", logger="taskiq.asyncpg_broker"):
            await connection.execute("SELECT pg_terminate_backend($1)", read_connection.get_server_pid())

            async def warned() -> None:
                while SUPPLIED_CONNECTION_CLOSED not in caplog.text:  # noqa: ASYNC110
                    await asyncio.sleep(0.01)

            await asyncio.wait_for(warned(), timeout=5)
        assert broker._read_conn is read_connection  # noqa: SLF001
    finally:
        await broker.shutdown()
        await read_connection.close()


@pytest.mark.integration
async def test_when_supplied_read_connection_is_closed_after_shutdown__then_no_warning_is_logged(
    pg_dsn: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Closing a supplied connection after shutdown is the caller's normal cleanup, not a failure."""
    read_connection = await asyncpg.connect(pg_dsn)
    broker = AsyncpgBroker(dsn=pg_dsn, read_connection=read_connection)
    await broker.startup()
    await broker.shutdown()

    with caplog.at_level("WARNING", logger="taskiq.asyncpg_broker"):
        await read_connection.close()
        await asyncio.sleep(0.1)

    assert SUPPLIED_CONNECTION_CLOSED not in caplog.text
