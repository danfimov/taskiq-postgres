from __future__ import annotations

import json
import typing as tp
import uuid

import asyncpg
import pytest

from taskiq_pg.aiopg import AiopgScheduleSource
from taskiq_pg.asyncpg import AsyncpgScheduleSource
from taskiq_pg.psqlpy import PSQLPyBroker, PSQLPyScheduleSource


if tp.TYPE_CHECKING:
    from taskiq import ScheduledTask


@pytest.mark.integration
@pytest.mark.parametrize(
    "schedule_source_class",
    [
        PSQLPyScheduleSource,
        AiopgScheduleSource,
        AsyncpgScheduleSource,
    ],
)
async def test_when_broker_has_tasks_with_schedule_labels__then_startup_persists_schedules_in_db(
    pg_dsn: str,
    schedule_source_class: type[PSQLPyScheduleSource | AiopgScheduleSource | AsyncpgScheduleSource],
) -> None:
    # Given: unique table, broker with a task having schedule labels, schedule source
    table_name: str = f"taskiq_schedules_{uuid.uuid4().hex}"
    broker = PSQLPyBroker(dsn=pg_dsn)

    @broker.task(
        task_name="tests:scheduled_task",
        schedule=[
            {
                "cron": "*/5 * * * *",
                "cron_offset": None,
                "time": None,
                "args": [1, 2],
                "kwargs": {"a": "b"},
                "labels": {"source": "test"},
            },
            {
                "cron": "0 0 * * *",
                "cron_offset": None,
                "time": None,
                "args": [],
                "kwargs": {},
                "labels": {},
            },
        ],
    )
    async def scheduled_task() -> None:  # noqa: ARG001
        return None

    source = schedule_source_class(dsn=pg_dsn, broker=broker, table_name=table_name)
    conn: asyncpg.Connection = await asyncpg.connect(dsn=pg_dsn)

    try:
        # When: starting schedule source should truncate table and insert schedules from broker labels
        await source.startup()

        # Then: schedules are persisted in DB
        cnt: int = tp.cast("int", await conn.fetchval(f"SELECT COUNT(*) FROM {table_name}"))
        assert cnt == 2

        rows = await conn.fetch(
            f"SELECT task_name, schedule FROM {table_name} ORDER BY task_name, schedule->>'cron'",
        )
        assert len(rows) == 2  # noqa: PLR2004
        assert all(r["task_name"] == "tests:scheduled_task" for r in rows)

        first_schedule = json.loads(tp.cast("str", rows[0]["schedule"]))
        assert first_schedule["cron"] == "0 0 * * *"
        assert first_schedule["args"] == []
        assert first_schedule["kwargs"] == {}
        assert first_schedule["labels"] == {}

        second_schedule = json.loads(tp.cast("str", rows[1]["schedule"]))
        assert second_schedule["cron"] == "*/5 * * * *"
        assert second_schedule["args"] == [1, 2]
        assert second_schedule["kwargs"] == {"a": "b"}
        assert second_schedule["labels"] == {"source": "test"}
    finally:
        await source.shutdown()
        try:
            await conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        finally:
            await conn.close()


@pytest.mark.integration
@pytest.mark.parametrize(
    "schedule_source_class",
    [
        PSQLPyScheduleSource,
        AiopgScheduleSource,
        AsyncpgScheduleSource,
    ],
)
async def test_when_schedule_rows_exist_in_db__then_get_schedules_returns_scheduled_tasks(
    pg_dsn: str,
    schedule_source_class: type[PSQLPyScheduleSource | AiopgScheduleSource | AsyncpgScheduleSource],
) -> None:
    # Given: unique table, started schedule source with empty broker schedules, and rows inserted directly into DB
    table_name: str = f"taskiq_schedules_{uuid.uuid4().hex}"
    broker = PSQLPyBroker(dsn=pg_dsn)
    source = schedule_source_class(dsn=pg_dsn, broker=broker, table_name=table_name)
    conn: asyncpg.Connection = await asyncpg.connect(dsn=pg_dsn)

    try:
        # Create table and ensure it's empty by starting up the source
        await source.startup()

        schedule_id1 = uuid.uuid4()
        schedule_id2 = uuid.uuid4()

        schedule_payload1 = {
            "cron": "*/10 * * * *",
            "cron_offset": None,
            "time": None,
            "args": [42],
            "kwargs": {"x": 10},
            "labels": {"foo": "bar"},
        }
        schedule_payload2 = {
            "cron": "0 1 * * *",
            "cron_offset": None,
            "time": None,
            "args": [],
            "kwargs": {},
            "labels": {},
        }

        await conn.execute(
            f"INSERT INTO {table_name} (id, task_name, schedule) VALUES ($1, $2, $3)",
            str(schedule_id1),
            "tests:other_task",
            json.dumps(schedule_payload1),
        )
        await conn.execute(
            f"INSERT INTO {table_name} (id, task_name, schedule) VALUES ($1, $2, $3)",
            str(schedule_id2),
            "tests:other_task",
            json.dumps(schedule_payload2),
        )

        # When: fetching schedules via schedule source
        schedules: list[ScheduledTask] = await source.get_schedules()

        # Then: schedules are correctly deserialized and returned
        assert len(schedules) == 2  # noqa: PLR2004

        ids = {s.schedule_id for s in schedules}
        assert str(schedule_id1) in ids
        assert str(schedule_id2) in ids

        s1 = next(s for s in schedules if s.schedule_id == str(schedule_id1))
        assert s1.task_name == "tests:other_task"
        assert s1.args == [42]
        assert s1.kwargs == {"x": 10}
        assert s1.labels == {"foo": "bar"}
        assert s1.cron == "*/10 * * * *"

        s2 = next(s for s in schedules if s.schedule_id == str(schedule_id2))
        assert s2.task_name == "tests:other_task"
        assert s2.args == []
        assert s2.kwargs == {}
        assert s2.labels == {}
        assert s2.cron == "0 1 * * *"
    finally:
        await source.shutdown()
        try:
            await conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        finally:
            await conn.close()
