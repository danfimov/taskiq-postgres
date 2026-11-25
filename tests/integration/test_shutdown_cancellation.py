import asyncio

import pytest

from taskiq_pg.aiopg import AiopgBroker
from taskiq_pg.asyncpg import AsyncpgBroker
from taskiq_pg.psqlpy import PSQLPyBroker


async def slow_to_cancel() -> None:
    """Run until cancelled, then take a while to clean up, like a reconnect in progress."""
    try:
        await asyncio.Future()
    except asyncio.CancelledError:
        await asyncio.sleep(0.5)
        raise


@pytest.mark.integration
@pytest.mark.parametrize(
    ("broker_class", "listener_method"),
    [
        (AsyncpgBroker, "_restore_listener"),
        (PSQLPyBroker, "_consume_notifications"),
        (AiopgBroker, "_consume_notifications"),
    ],
)
async def test_when_shutdown_is_cancelled__then_cancellation_is_propagated(
    pg_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    broker_class: type[AiopgBroker | AsyncpgBroker | PSQLPyBroker],
    listener_method: str,
) -> None:
    # given
    broker = broker_class(dsn=pg_dsn)
    monkeypatch.setattr(broker, listener_method, slow_to_cancel)
    await broker.startup()
    try:
        shutdown = asyncio.create_task(broker.shutdown())
        await asyncio.sleep(0.1)

        # when
        shutdown.cancel()
        await asyncio.gather(shutdown, return_exceptions=True)

        # then
        assert shutdown.cancelled(), "shutdown() must not swallow its own cancellation"
    finally:
        await broker.shutdown()
