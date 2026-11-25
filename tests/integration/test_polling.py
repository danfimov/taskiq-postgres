import asyncio
import json
import typing as tp
import uuid

import asyncpg
import pytest
from taskiq import AckableMessage, BrokerMessage

from taskiq_pg.aiopg import AiopgBroker
from taskiq_pg.asyncpg import AsyncpgBroker
from taskiq_pg.psqlpy import PSQLPyBroker
from taskiq_pg.psycopg import PsycopgBroker


Broker: tp.TypeAlias = AsyncpgBroker | PsycopgBroker | PSQLPyBroker | AiopgBroker
BROKERS: list[tp.Any] = [AsyncpgBroker, PsycopgBroker, PSQLPyBroker, AiopgBroker]


def unique_names() -> dict[str, str]:
    suffix = uuid.uuid4().hex[:12]
    return {"table_name": f"poll_{suffix}", "channel_name": f"poll_{suffix}"}


async def insert_pending(connection: asyncpg.Connection, table: str, data: str, labels: dict[str, tp.Any]) -> int:
    """Insert a pending message without sending a NOTIFY, like a kick whose notification was missed."""
    return tp.cast(
        "int",
        await connection.fetchval(
            f"INSERT INTO {table} (task_id, task_name, message, labels) VALUES ($1, $2, $3, $4) RETURNING id",  # noqa: S608
            uuid.uuid4().hex,
            "example:polled",
            data,
            json.dumps(labels),
        ),
    )


async def take(listener: tp.AsyncIterator[AckableMessage], count: int, timeout: float) -> list[bytes]:
    """Read `count` messages from a listener within `timeout` seconds."""

    async def read() -> list[bytes]:
        return [(await anext(listener)).data for _ in range(count)]

    return await asyncio.wait_for(read(), timeout)


@pytest.mark.integration
@pytest.mark.parametrize("broker_class", BROKERS)
async def test_when_message_was_inserted_without_notify__then_worker_claims_it_by_polling(
    pg_dsn: str,
    connection: asyncpg.Connection,
    broker_class: type[Broker],
) -> None:
    # given
    names = unique_names()
    broker = broker_class(dsn=pg_dsn, poll_interval=0.2, **names)
    await broker.startup()
    listener = broker.listen()
    try:
        # when
        await insert_pending(connection, names["table_name"], '{"polled":true}', {})

        # then
        assert await take(listener, 1, timeout=3) == [b'{"polled":true}']
    finally:
        await listener.aclose()
        await broker.shutdown()


@pytest.mark.integration
@pytest.mark.parametrize("broker_class", BROKERS)
async def test_when_messages_were_kicked_before_worker_started__then_worker_processes_them(
    pg_dsn: str,
    broker_class: type[Broker],
) -> None:
    # given
    names = unique_names()
    producer = broker_class(dsn=pg_dsn, **names)
    await producer.startup()
    expected = {f'{{"n":{n}}}'.encode() for n in range(5)}
    for data in expected:
        await producer.kick(
            BrokerMessage(task_id=uuid.uuid4().hex, task_name="example:early", message=data, labels={}),
        )
    worker = broker_class(dsn=pg_dsn, **names)

    # when
    await worker.startup()
    listener = worker.listen()
    try:
        received = await take(listener, len(expected), timeout=3)

        # then
        assert set(received) == expected
    finally:
        await listener.aclose()
        await worker.shutdown()
        await producer.shutdown()


@pytest.mark.integration
@pytest.mark.parametrize("broker_class", BROKERS)
async def test_when_delayed_message_was_not_notified__then_it_is_claimed_after_its_delay(
    pg_dsn: str,
    connection: asyncpg.Connection,
    broker_class: type[Broker],
) -> None:
    # given
    names = unique_names()
    broker = broker_class(dsn=pg_dsn, poll_interval=0.2, **names)
    await broker.startup()
    listener = broker.listen()
    pending: asyncio.Task[AckableMessage] = asyncio.create_task(anext(listener))  # type: ignore[arg-type]
    try:
        # when
        await insert_pending(connection, names["table_name"], '{"delayed":true}', {"delay": 1})
        done, _ = await asyncio.wait({pending}, timeout=0.6)

        # then
        assert not done, "A delayed message must not be claimed before its delay elapses"
        done, _ = await asyncio.wait({pending}, timeout=3)
        assert pending in done
        assert pending.result().data == b'{"delayed":true}'
    finally:
        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)
        await broker.shutdown()


@pytest.mark.integration
@pytest.mark.parametrize("broker_class", BROKERS)
async def test_when_two_workers_poll__then_each_message_is_claimed_once(
    pg_dsn: str,
    connection: asyncpg.Connection,
    broker_class: type[Broker],
) -> None:
    # given
    names = unique_names()
    first = broker_class(dsn=pg_dsn, poll_interval=0.2, **names)
    second = broker_class(dsn=pg_dsn, poll_interval=0.2, **names)
    await first.startup()
    await second.startup()
    expected = [f'{{"n":{n}}}' for n in range(20)]
    for data in expected:
        await insert_pending(connection, names["table_name"], data, {})
    received: list[bytes] = []

    async def consume(broker: Broker) -> None:
        async for message in broker.listen():
            received.append(message.data)  # noqa: PERF401 - the loop runs until cancelled

    # when
    consumers = [asyncio.create_task(consume(first)), asyncio.create_task(consume(second))]
    try:

        async def all_received() -> None:
            while len(received) < len(expected):  # noqa: ASYNC110
                await asyncio.sleep(0.05)

        await asyncio.wait_for(all_received(), timeout=5)
        await asyncio.sleep(0.5)  # give a duplicate the chance to show up

        # then
        assert sorted(received) == sorted(data.encode() for data in expected)
    finally:
        for consumer in consumers:
            consumer.cancel()
        await asyncio.gather(*consumers, return_exceptions=True)
        await first.shutdown()
        await second.shutdown()


@pytest.mark.integration
@pytest.mark.parametrize("broker_class", BROKERS)
async def test_when_broker_starts__then_pending_index_is_created(
    pg_dsn: str,
    connection: asyncpg.Connection,
    broker_class: type[Broker],
) -> None:
    # given
    names = unique_names()
    broker = broker_class(dsn=pg_dsn, **names)

    # when
    await broker.startup()
    try:
        definition = await connection.fetchval(
            "SELECT indexdef FROM pg_indexes WHERE indexname = $1",
            f"{names['table_name']}_pending_idx",
        )

        # then
        assert definition is not None
        assert "WHERE (status = 'pending'::text)" in definition
    finally:
        await broker.shutdown()


@pytest.mark.integration
@pytest.mark.parametrize("broker_class", BROKERS)
async def test_when_pending_rows_have_malformed_delay_labels__then_polling_still_claims_messages(
    pg_dsn: str,
    connection: asyncpg.Connection,
    broker_class: type[Broker],
) -> None:
    # given
    names = unique_names()
    table = names["table_name"]
    broker = broker_class(dsn=pg_dsn, poll_interval=0.2, **names)
    await broker.startup()
    await insert_pending(connection, table, '{"label":"text"}', {"delay": "abc"})
    await insert_pending(connection, table, '{"label":"negative"}', {"delay": "-3"})
    await insert_pending(connection, table, '{"label":"huge"}', {"delay": "9" * 400})
    await insert_pending(connection, table, '{"label":"none"}', {})
    listener = broker.listen()
    try:
        # when
        received = await take(listener, 3, timeout=5)

        # then
        assert received == [b'{"label":"text"}', b'{"label":"negative"}', b'{"label":"none"}']
        pending = await connection.fetchval(f"SELECT message FROM {table} WHERE status = 'pending'")  # noqa: S608
        assert pending == '{"label":"huge"}'
    finally:
        await listener.aclose()
        await broker.shutdown()


@pytest.mark.integration
@pytest.mark.parametrize("broker_class", BROKERS)
async def test_when_delay_label_is_malformed__then_kick_fails_without_inserting_the_message(
    pg_dsn: str,
    connection: asyncpg.Connection,
    broker_class: type[Broker],
) -> None:
    # given
    names = unique_names()
    broker = broker_class(dsn=pg_dsn, **names)
    await broker.startup()
    message = BrokerMessage(task_id=uuid.uuid4().hex, task_name="example:bad", message=b"{}", labels={"delay": "abc"})
    try:
        # when
        with pytest.raises(ValueError, match="invalid literal"):
            await broker.kick(message)

        # then
        assert await connection.fetchval(f"SELECT count(*) FROM {names['table_name']}") == 0  # noqa: S608
    finally:
        await broker.shutdown()
