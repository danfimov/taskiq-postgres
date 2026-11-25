import asyncio
import typing as tp

import asyncpg
import pytest
from taskiq import AckableMessage

from tests.integration.test_polling import insert_pending, unique_names

from taskiq_pg.aiopg import AiopgBroker
from taskiq_pg.asyncpg import AsyncpgBroker
from taskiq_pg.psqlpy import PSQLPyBroker
from taskiq_pg.psycopg import PsycopgBroker


Broker: tp.TypeAlias = AsyncpgBroker | PsycopgBroker | PSQLPyBroker | AiopgBroker
BROKERS: list[tp.Any] = [AsyncpgBroker, PsycopgBroker, PSQLPyBroker, AiopgBroker]

FAST_POLL: dict[str, float] = {"poll_interval": 0.2}


async def next_message(listener: tp.AsyncIterator[AckableMessage], timeout: float) -> AckableMessage:
    return await asyncio.wait_for(anext(listener), timeout)


async def status_of(connection: asyncpg.Connection, table: str, data: str) -> str | None:
    return tp.cast(
        "str | None",
        await connection.fetchval(f"SELECT status FROM {table} WHERE message = $1", data),  # noqa: S608
    )


@pytest.mark.integration
@pytest.mark.parametrize("broker_class", BROKERS)
async def test_when_claimed_message_is_not_acknowledged__then_it_is_delivered_again_after_processing_timeout(
    pg_dsn: str,
    connection: asyncpg.Connection,
    broker_class: type[Broker],
) -> None:
    # given
    names = unique_names()
    broker = broker_class(dsn=pg_dsn, processing_timeout=0.5, **FAST_POLL, **names)
    await broker.startup()
    listener = broker.listen()
    try:
        await insert_pending(connection, names["table_name"], '{"n":1}', {})
        first = await next_message(listener, timeout=3)

        # when: the worker "dies" without acknowledging the message
        second = await next_message(listener, timeout=5)

        # then
        assert first.data == second.data == b'{"n":1}'
    finally:
        await listener.aclose()
        await broker.shutdown()


@pytest.mark.integration
@pytest.mark.parametrize("broker_class", BROKERS)
async def test_when_processing_timeout_is_not_set__then_claimed_message_is_not_delivered_again(
    pg_dsn: str,
    connection: asyncpg.Connection,
    broker_class: type[Broker],
) -> None:
    # given
    names = unique_names()
    broker = broker_class(dsn=pg_dsn, **FAST_POLL, **names)
    await broker.startup()
    listener = broker.listen()
    pending: asyncio.Task[AckableMessage] | None = None
    try:
        await insert_pending(connection, names["table_name"], '{"n":1}', {})
        await next_message(listener, timeout=3)

        # when
        pending = asyncio.create_task(next_message(listener, timeout=1.5))
        await asyncio.gather(pending, return_exceptions=True)

        # then
        assert isinstance(pending.exception(), TimeoutError), "At-most-once: the message must not come back"
        assert await status_of(connection, names["table_name"], '{"n":1}') == "processing"
    finally:
        await listener.aclose()
        await broker.shutdown()


@pytest.mark.integration
@pytest.mark.parametrize("broker_class", BROKERS)
async def test_when_attempts_are_exhausted__then_message_is_marked_as_failed(
    pg_dsn: str,
    connection: asyncpg.Connection,
    broker_class: type[Broker],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # given
    names = unique_names()
    broker = broker_class(dsn=pg_dsn, processing_timeout=0.3, max_retry_attempts=2, **FAST_POLL, **names)
    await broker.startup()
    listener = broker.listen()
    try:
        await insert_pending(connection, names["table_name"], '{"n":1}', {})
        await next_message(listener, timeout=3)
        await next_message(listener, timeout=5)

        # when
        with caplog.at_level("WARNING"):
            third = asyncio.create_task(next_message(listener, timeout=1.5))
            await asyncio.gather(third, return_exceptions=True)

        # then
        assert isinstance(third.exception(), TimeoutError), "No delivery beyond max_retry_attempts"
        assert await status_of(connection, names["table_name"], '{"n":1}') == "failed"
        assert "marked as failed" in caplog.text
    finally:
        await listener.aclose()
        await broker.shutdown()


@pytest.mark.integration
@pytest.mark.parametrize("broker_class", BROKERS)
async def test_when_message_is_acknowledged__then_it_is_not_delivered_again(
    pg_dsn: str,
    connection: asyncpg.Connection,
    broker_class: type[Broker],
) -> None:
    # given
    names = unique_names()
    broker = broker_class(dsn=pg_dsn, processing_timeout=0.3, **FAST_POLL, **names)
    await broker.startup()
    listener = broker.listen()
    try:
        await insert_pending(connection, names["table_name"], '{"n":1}', {})
        message = await next_message(listener, timeout=3)

        # when
        acknowledgement = message.ack()
        assert acknowledgement is not None
        await acknowledgement
        again = asyncio.create_task(next_message(listener, timeout=1.2))
        await asyncio.gather(again, return_exceptions=True)

        # then
        assert isinstance(again.exception(), TimeoutError)
        assert await status_of(connection, names["table_name"], '{"n":1}') is None
    finally:
        await listener.aclose()
        await broker.shutdown()


OLD_SCHEMA = """
CREATE TABLE {} (
    id SERIAL PRIMARY KEY,
    task_id VARCHAR NOT NULL,
    task_name VARCHAR NOT NULL,
    message TEXT NOT NULL,
    labels JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
)
"""


@pytest.mark.integration
@pytest.mark.parametrize("broker_class", BROKERS)
async def test_when_table_has_the_old_schema__then_it_is_migrated_and_stuck_messages_are_delivered(
    pg_dsn: str,
    connection: asyncpg.Connection,
    broker_class: type[Broker],
) -> None:
    # given: a table from an older version with a message stuck in 'processing' for an hour
    names = unique_names()
    table = names["table_name"]
    await connection.execute(OLD_SCHEMA.format(table))
    await connection.execute(
        f"INSERT INTO {table} (task_id, task_name, message, labels, status, created_at)"  # noqa: S608
        " VALUES ('old', 'example:old', '{\"old\":true}', '{}', 'processing', now() - interval '1 hour')",
    )
    broker = broker_class(dsn=pg_dsn, processing_timeout=60, **FAST_POLL, **names)

    # when
    await broker.startup()
    listener = broker.listen()
    try:
        message = await next_message(listener, timeout=3)

        # then
        assert message.data == b'{"old":true}'
        columns = await connection.fetch(
            "SELECT column_name FROM information_schema.columns WHERE table_name = $1",
            table,
        )
        assert {"claimed_at", "attempts"} <= {row["column_name"] for row in columns}
    finally:
        await listener.aclose()
        await broker.shutdown()
