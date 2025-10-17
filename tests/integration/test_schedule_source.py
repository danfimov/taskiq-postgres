from __future__ import annotations

import typing as tp
import uuid
from contextlib import asynccontextmanager
from datetime import timedelta

import pytest
from polyfactory.factories.pydantic_factory import ModelFactory
from sqlalchemy_utils.types.enriched_datetime.arrow_datetime import datetime
from taskiq import ScheduledTask

from taskiq_pg.aiopg import AiopgScheduleSource
from taskiq_pg.asyncpg import AsyncpgScheduleSource
from taskiq_pg.psqlpy import PSQLPyBroker, PSQLPyScheduleSource
from taskiq_pg.psycopg import PsycopgScheduleSource


if tp.TYPE_CHECKING:
    from taskiq_pg._internal import BasePostgresBroker, BasePostgresScheduleSource



class ScheduledTaskFactory(ModelFactory[ScheduledTask]):
    """Factory for ScheduledTask."""

    __model__ = ScheduledTask
    __check_model__ = True

    @classmethod
    def schedule_id(cls) -> str:
        """Generate unique schedule ID."""
        return uuid.uuid4().hex


@asynccontextmanager
async def _get_schedule_source(
    schedule_source_class: type[BasePostgresScheduleSource],
    broker: BasePostgresBroker,
    dsn: str,
):
    schedule_source = schedule_source_class(broker, dsn)
    try:
        yield schedule_source
    finally:
        await schedule_source.shutdown()


@pytest.fixture
def broker_with_scheduled_tasks(pg_dsn: str) -> PSQLPyBroker:
    """Test broker with two tasks: one with one schedule and second with two schedules."""
    broker = PSQLPyBroker(dsn=pg_dsn)

    @broker.task(
        task_name="tests:two_schedules",
        schedule=[
            {
                "cron": "*/10 * * * *",
                "cron_offset": "Europe/Berlin",
                "time": None,
                "args": [42],
                "kwargs": {"x": 10},
                "labels": {"foo": "bar"},
            },
            {
                "cron": "0 1 * * *",
                "cron_offset": timedelta(hours=1),
                "time": None,
                "args": [],
                "kwargs": {},
                "labels": {},
            },
        ],
    )
    async def _two_schedules() -> None:
        return None

    @broker.task(
        task_name="tests:one_schedule",
        schedule=[
            {
                "cron_offset": None,
                "time": datetime(2024, 1, 1, 12, 0, 0),
                "args": [],
                "kwargs": {},
                "labels": {},
            },
        ],
    )
    async def _one_schedule() -> None:
        return None

    @broker.task(
        task_name="tests:without_schedule",
    )
    async def _without_schedule() -> None:
        return None

    @broker.task(task_name="tests:invalid_schedule", schedule={})
    async def _invalid_schedule() -> None:
        return None

    @broker.task(
        task_name="tests:invalid_schedule_2",
        schedule=[
            {
                "invalid": "data",
            }
        ],
    )
    async def _invalid_schedule_2() -> None:
        return None

    return broker


@pytest.mark.integration
@pytest.mark.parametrize(
    "schedule_source_class",
    [
        PSQLPyScheduleSource,
        AiopgScheduleSource,
        AsyncpgScheduleSource,
        PsycopgScheduleSource,
    ],
)
async def test_when_labels_contain_schedules__then_get_schedules_returns_scheduled_tasks(
    pg_dsn: str,
    broker_with_scheduled_tasks: PSQLPyBroker,
    schedule_source_class: type[PSQLPyScheduleSource | AiopgScheduleSource | AsyncpgScheduleSource],
) -> None:
    # When
    async with _get_schedule_source(schedule_source_class, broker_with_scheduled_tasks, pg_dsn) as schedule_source:
        await schedule_source.startup()
        schedules: list[ScheduledTask] = await schedule_source.get_schedules()

    # Then
    assert len(schedules) == 3
    assert {item.cron for item in schedules} == {"*/10 * * * *", "0 1 * * *", None}
    assert {item.cron_offset for item in schedules} == {None, "Europe/Berlin", "PT1H"}
    assert {item.task_name for item in schedules} == {"tests:one_schedule", "tests:two_schedules"}
    assert {item.time for item in schedules} == {datetime(2024, 1, 1, 12, 0, 0), None}
    assert all(item.schedule_id is not None for item in schedules)


@pytest.mark.integration
@pytest.mark.parametrize(
    "schedule_source_class",
    [
        PSQLPyScheduleSource,
        AiopgScheduleSource,
        AsyncpgScheduleSource,
        PsycopgScheduleSource,
    ],
)
async def test_when_call_add_schedule__then_schedule_creates(
    pg_dsn: str,
    broker_with_scheduled_tasks: PSQLPyBroker,
    schedule_source_class: type[PSQLPyScheduleSource | AiopgScheduleSource | AsyncpgScheduleSource],
) -> None:
    # Given
    new_schedule = ScheduledTaskFactory.build(task_name="tests:added_schedule", cron="*/5 * * * *")
    async with _get_schedule_source(schedule_source_class, broker_with_scheduled_tasks, pg_dsn) as schedule_source:
        await schedule_source.startup()

        # When
        await schedule_source.add_schedule(new_schedule)

        # Then
        schedules: list[ScheduledTask] = await schedule_source.get_schedules()
        assert len(schedules) == 4
        added_schedule = None
        for task in schedules:
            if task.task_name == "tests:added_schedule":
                added_schedule = task
                break
        assert added_schedule is not None



@pytest.mark.integration
@pytest.mark.parametrize(
    "schedule_source_class",
    [
        PSQLPyScheduleSource,
        AiopgScheduleSource,
        AsyncpgScheduleSource,
        PsycopgScheduleSource,
    ],
)
async def test_when_call_delete_schedule__then_schedule_deleted(
    pg_dsn: str,
    broker_with_scheduled_tasks: PSQLPyBroker,
    schedule_source_class: type[PSQLPyScheduleSource | AiopgScheduleSource | AsyncpgScheduleSource],
) -> None:
    # Given
    async with _get_schedule_source(schedule_source_class, broker_with_scheduled_tasks, pg_dsn) as schedule_source:
        await schedule_source.startup()
        schedules: list[ScheduledTask] = await schedule_source.get_schedules()
        schedule_id_to_delete = str(schedules[0].schedule_id)

        # When
        await schedule_source.delete_schedule(schedule_id_to_delete)

        # Then
        schedules: list[ScheduledTask] = await schedule_source.get_schedules()
        assert len(schedules) == 2
        assert all(
            task.schedule_id != schedule_id_to_delete
            for task in schedules
        )
