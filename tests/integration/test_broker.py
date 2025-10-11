from __future__ import annotations

import asyncio
import typing as tp
import uuid

import pytest
from taskiq import BrokerMessage

from taskiq_pg.asyncpg import AsyncpgBroker
from taskiq_pg.psqlpy import PSQLPyBroker
from taskiq_pg.psycopg import PsycopgBroker


if tp.TYPE_CHECKING:
    import asyncpg


@pytest.fixture
async def broker(
    pg_dsn: str,
    request: pytest.FixtureRequest,
) -> tp.AsyncGenerator[AsyncpgBroker | PSQLPyBroker | PsycopgBroker, None]:
    broker = request.param(dsn=pg_dsn)
    await broker.startup()
    try:
        yield broker
    finally:
        await broker.shutdown()


@pytest.fixture
async def broker_2(
    pg_dsn: str,
    request: pytest.FixtureRequest,
) -> tp.AsyncGenerator[AsyncpgBroker | PSQLPyBroker | PsycopgBroker, None]:
    broker = request.param(dsn=pg_dsn)
    await broker.startup()
    try:
        yield broker
    finally:
        await broker.shutdown()


@pytest.mark.parametrize(
    "broker",
    [
        AsyncpgBroker,
        PSQLPyBroker,
        PsycopgBroker,
    ],
    indirect=True,
)
async def test_when_message_was_published__then_worker_received_message(
    broker,
) -> None:
    # given
    message: BrokerMessage = BrokerMessage(
        task_id=uuid.uuid4().hex,
        task_name="example:best_task_ever",
        message=b'{"hello":"world"}',
        labels={},
    )
    await broker.startup()
    listener = broker.listen()
    task: asyncio.Task = asyncio.create_task(listener.__anext__())

    # when
    await broker.kick(message)
    done, pending = await asyncio.wait(
        {task},
        timeout=5.0,
    )

    # then
    assert len(done) == 1, "Worker should receive message"
    assert len(pending) == 0, "Worker should receive message"


@pytest.mark.integration
@pytest.mark.parametrize(
    ("broker", "broker_2"),
    [
        (AsyncpgBroker, AsyncpgBroker),
        (PSQLPyBroker, PSQLPyBroker),
        (PsycopgBroker, PsycopgBroker),
    ],
    indirect=True,
)
async def test_when_two_workers_listen__then_single_message_processed_once(
    connection: asyncpg.Connection,
    broker,
    broker_2,
) -> None:
    # given
    table_name: str = "taskiq_messages"
    task_id: str = uuid.uuid4().hex
    message: BrokerMessage = BrokerMessage(
        task_id=task_id,
        task_name="example:best_task_ever",
        message=b'{"hello":"world"}',
        labels={},
    )

    # when
    await broker.startup()
    await broker_2.startup()

    # Запускаем ожидание первого сообщения у обоих слушателей до публикации,
    # чтобы оба гарантированно получили NOTIFY.
    task_1: asyncio.Task = asyncio.create_task(broker.listen().__anext__())
    task_2: asyncio.Task = asyncio.create_task(broker_2.listen().__anext__())

    await broker.kick(message)

    done, _ = await asyncio.wait(
        {task_1, task_2},
        timeout=5.0,
        return_when=asyncio.FIRST_COMPLETED,
    )

    # Then: только один слушатель получает сообщение
    assert len(done) == 1, "Ровно один воркер должен получить сообщение"
    winner_task: asyncio.Task = next(iter(done))
    ack_message = tp.cast("tp.Any", winner_task.result())

    # До подтверждения проверяем, что статус в таблице = 'processing'
    row = await connection.fetchrow(
        f"SELECT id, status FROM {table_name} WHERE task_id = $1",
        task_id,
    )
    assert row is not None, "Сообщение должно существовать в таблице"
    assert row["status"] == "processing", "Сообщение должно быть помечено как processing после claim"

    # Подтверждаем обработку победившим воркером
    await ack_message.ack()

    # И проверяем, что запись удалена
    cnt: int = tp.cast(
        "int",
        await connection.fetchval(
            f"SELECT COUNT(*) FROM {table_name} WHERE task_id = $1",
            task_id,
        ),
    )
    assert cnt == 0, "Запись должна быть удалена после ack"
