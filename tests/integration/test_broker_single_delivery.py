from __future__ import annotations

import asyncio
import typing as tp
import uuid
from contextlib import suppress

import asyncpg
import pytest
from taskiq import BrokerMessage

from taskiq_pg.asyncpg import AsyncpgBroker
from taskiq_pg.psqlpy import PSQLPyBroker


@pytest.mark.integration
@pytest.mark.parametrize(
    "broker_class",
    [
        AsyncpgBroker,
        PSQLPyBroker,
    ],
)
async def test_when_two_workers_listen__then_single_message_processed_once(
    pg_dsn: str,
    broker_class: type[AsyncpgBroker | PSQLPyBroker],
) -> None:
    # Given: уникальные имена таблицы и канала, два брокера, одна задача
    table_name: str = f"taskiq_messages_{uuid.uuid4().hex}"
    channel_name: str = f"taskiq_channel_{uuid.uuid4().hex}"
    task_id: str = uuid.uuid4().hex

    broker1 = broker_class(dsn=pg_dsn, table_name=table_name, channel_name=channel_name)
    broker2 = broker_class(dsn=pg_dsn, table_name=table_name, channel_name=channel_name)

    # Подключение для проверок состояния в таблице
    conn: asyncpg.Connection = await asyncpg.connect(dsn=pg_dsn)

    # Сообщение для публикации
    message: BrokerMessage = BrokerMessage(
        task_id=task_id,
        task_name="example:best_task_ever",
        message=b'{"hello":"world"}',
        labels={},
    )

    # When: стартуем брокеры и два слушателя, публикуем одно сообщение
    await broker1.startup()
    await broker2.startup()

    agen1 = broker1.listen()
    agen2 = broker2.listen()

    # Запускаем ожидание первого сообщения у обоих слушателей до публикации,
    # чтобы оба гарантированно получили NOTIFY.
    t1: asyncio.Task = asyncio.create_task(agen1.__anext__())
    t2: asyncio.Task = asyncio.create_task(agen2.__anext__())

    try:
        await broker1.kick(message)

        done, _ = await asyncio.wait(
            {t1, t2},
            timeout=5.0,
            return_when=asyncio.FIRST_COMPLETED,
        )

        # Then: только один слушатель получает сообщение
        assert len(done) == 1, "Ровно один воркер должен получить сообщение"
        winner_task: asyncio.Task = next(iter(done))
        ack_message = tp.cast("tp.Any", winner_task.result())

        # До подтверждения проверяем, что статус в таблице = 'processing'
        row = await conn.fetchrow(
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
            await conn.fetchval(
                f"SELECT COUNT(*) FROM {table_name} WHERE task_id = $1",
                task_id,
            ),
        )
        assert cnt == 0, "Запись должна быть удалена после ack"
    finally:
        with suppress(Exception):
            await broker1.shutdown()
            await broker2.shutdown()

        try:
            await conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        finally:
            await conn.close()
