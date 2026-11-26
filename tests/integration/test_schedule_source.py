import uuid
from contextlib import asynccontextmanager

import pytest
from polyfactory.factories.pydantic_factory import ModelFactory
from taskiq import ScheduledTask

from taskiq_pg._internal import BasePostgresScheduleSource
from taskiq_pg.aiopg import AiopgScheduleSource
from taskiq_pg.asyncpg import AsyncpgScheduleSource
from taskiq_pg.psqlpy import PSQLPyScheduleSource
from taskiq_pg.psycopg import PsycopgScheduleSource


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
    dsn: str,
    table_name: str,
):
    schedule_source = schedule_source_class(dsn=dsn, table_name=table_name)
    try:
        yield schedule_source
    finally:
        await schedule_source.shutdown()


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
    schedule_source_class: type[PSQLPyScheduleSource | AiopgScheduleSource | AsyncpgScheduleSource],
) -> None:
    # Given
    new_schedule = ScheduledTaskFactory.build(
        task_name="tests:added_schedule",
        cron="*/5 * * * *",
        interval=None,
        schedule_id=str(uuid.uuid4()),
    )
    table_name = f"taskiq_schedules_{uuid.uuid4().hex}"
    async with _get_schedule_source(schedule_source_class, pg_dsn, table_name) as schedule_source:
        await schedule_source.startup()

        # When
        await schedule_source.add_schedule(new_schedule)

        # Then
        schedules: list[ScheduledTask] = await schedule_source.get_schedules()
        assert len(schedules) == 1
        assert schedules[0].schedule_id == new_schedule.schedule_id


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
    schedule_source_class: type[PSQLPyScheduleSource | AiopgScheduleSource | AsyncpgScheduleSource],
) -> None:
    # Given
    first_schedule = ScheduledTaskFactory.build(
        task_name="tests:first_schedule",
        cron="*/5 * * * *",
        interval=None,
        schedule_id=str(uuid.uuid4()),
    )
    second_schedule = ScheduledTaskFactory.build(
        task_name="tests:second_schedule",
        cron="0 1 * * *",
        interval=None,
        schedule_id=str(uuid.uuid4()),
    )
    table_name = f"taskiq_schedules_{uuid.uuid4().hex}"
    async with _get_schedule_source(schedule_source_class, pg_dsn, table_name) as schedule_source:
        await schedule_source.startup()
        await schedule_source.add_schedule(first_schedule)
        await schedule_source.add_schedule(second_schedule)
        schedule_id_to_delete = first_schedule.schedule_id

        # When
        await schedule_source.delete_schedule(schedule_id_to_delete)

        # Then
        schedules: list[ScheduledTask] = await schedule_source.get_schedules()
        assert len(schedules) == 1
        assert all(task.schedule_id != schedule_id_to_delete for task in schedules)


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
async def test_when_starting_source_again__then_schedules_from_previous_run_kept(
    pg_dsn: str,
    schedule_source_class: type[PSQLPyScheduleSource | AiopgScheduleSource | AsyncpgScheduleSource],
) -> None:
    # Given
    new_schedule = ScheduledTaskFactory.build(
        task_name="tests:added_schedule",
        cron="*/5 * * * *",
        interval=None,
        schedule_id=str(uuid.uuid4()),
    )
    table_name = f"taskiq_schedules_{uuid.uuid4().hex}"
    async with _get_schedule_source(schedule_source_class, pg_dsn, table_name) as schedule_source:
        await schedule_source.startup()
        await schedule_source.add_schedule(new_schedule)
        assert len(await schedule_source.get_schedules()) == 1

    # When
    async with _get_schedule_source(schedule_source_class, pg_dsn, table_name) as schedule_source:
        await schedule_source.startup()

        # Then
        schedules: list[ScheduledTask] = await schedule_source.get_schedules()
        assert len(schedules) == 1
        assert any(task.schedule_id == new_schedule.schedule_id for task in schedules)
