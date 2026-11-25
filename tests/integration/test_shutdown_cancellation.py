import asyncio
import gc
import typing as tp
import uuid

import pytest
from taskiq import BrokerMessage

from taskiq_pg.aiopg import AiopgBroker
from taskiq_pg.asyncpg import AsyncpgBroker
from taskiq_pg.psqlpy import PSQLPyBroker
from taskiq_pg.psycopg import PsycopgBroker


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


@pytest.mark.integration
@pytest.mark.parametrize("broker_class", [AsyncpgBroker, PsycopgBroker, PSQLPyBroker, AiopgBroker])
async def test_when_broker_shuts_down_before_a_delayed_notification__then_the_notification_is_cancelled(
    pg_dsn: str,
    broker_class: type[AiopgBroker | AsyncpgBroker | PsycopgBroker | PSQLPyBroker],
) -> None:
    # given
    suffix = uuid.uuid4().hex[:12]
    broker = broker_class(dsn=pg_dsn, table_name=f"delayed_{suffix}", channel_name=f"delayed_{suffix}")
    await broker.startup()
    errors: list[dict[str, tp.Any]] = []
    loop = asyncio.get_running_loop()
    previous_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: errors.append(context))
    try:
        message = BrokerMessage(
            task_id=uuid.uuid4().hex,
            task_name="example:delayed",
            message=b"{}",
            labels={"delay": 1},
        )
        await broker.kick(message)

        # when: the producer shuts down and keeps running past the delay
        await asyncio.wait_for(broker.shutdown(), timeout=5)
        await asyncio.sleep(1.5)
        gc.collect()

        # then: the delayed NOTIFY did not run against the closed pool
        assert [context.get("message") for context in errors] == []
    finally:
        loop.set_exception_handler(previous_handler)
