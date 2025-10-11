import uuid
from logging import getLogger

from psycopg_pool import AsyncConnectionPool
from pydantic import ValidationError
from taskiq import ScheduledTask

from taskiq_pg._internal import BasePostgresScheduleSource
from taskiq_pg.psycopg.queries import (
    CREATE_SCHEDULES_TABLE_QUERY,
    DELETE_ALL_SCHEDULES_QUERY,
    INSERT_SCHEDULE_QUERY,
    SELECT_SCHEDULES_QUERY,
)


logger = getLogger("taskiq_pg.psycopg_schedule_source")


class PsycopgScheduleSource(BasePostgresScheduleSource):
    """Schedule source that uses psycopg to store schedules in PostgreSQL."""

    _database_pool: AsyncConnectionPool

    async def _update_schedules_on_startup(self, schedules: list[ScheduledTask]) -> None:
        """Update schedules in the database on startup: truncate table and insert new ones."""
        async with self._database_pool.connection() as connection, connection.cursor() as cursor:
            await cursor.execute(DELETE_ALL_SCHEDULES_QUERY.format(self._table_name))
            data_to_insert: list = [
                [
                    uuid.UUID(schedule.schedule_id),
                    schedule.task_name,
                    schedule.model_dump_json(
                        exclude={"schedule_id", "task_name"},
                    ),
                ]
                for schedule in schedules
            ]
            await cursor.executemany(
                INSERT_SCHEDULE_QUERY.format(self._table_name),
                data_to_insert,
            )

    def _get_schedules_from_broker_tasks(self) -> list[ScheduledTask]:
        """Extract schedules from the broker's registered tasks."""
        scheduled_tasks_for_creation: list[ScheduledTask] = []
        for task_name, task in self._broker.get_all_tasks().items():
            if "schedule" not in task.labels:
                logger.debug("Task %s has no schedule, skipping", task_name)
                continue
            if not isinstance(task.labels["schedule"], list):
                logger.warning(
                    "Schedule for task %s is not a list, skipping",
                    task_name,
                )
                continue
            for schedule in task.labels["schedule"]:
                try:
                    new_schedule = ScheduledTask.model_validate(
                        {
                            "task_name": task_name,
                            "labels": schedule.get("labels", {}),
                            "args": schedule.get("args", []),
                            "kwargs": schedule.get("kwargs", {}),
                            "schedule_id": str(uuid.uuid4()),
                            "cron": schedule.get("cron", None),
                            "cron_offset": schedule.get("cron_offset", None),
                            "time": schedule.get("time", None),
                        },
                    )
                    scheduled_tasks_for_creation.append(new_schedule)
                except ValidationError:
                    logger.exception(
                        "Schedule for task %s is not valid, skipping",
                        task_name,
                    )
                    continue
        return scheduled_tasks_for_creation

    async def startup(self) -> None:
        """
        Initialize the schedule source.

        Construct new connection pool, create new table for schedules if not exists
        and fill table with schedules from task labels.
        """
        self._database_pool = AsyncConnectionPool(
            conninfo=self.dsn if self.dsn is not None else "",
            open=False,
            **self._connect_kwargs,
        )
        await self._database_pool.open()

        async with self._database_pool.connection() as connection, connection.cursor() as cursor:
            await cursor.execute(
                CREATE_SCHEDULES_TABLE_QUERY.format(self._table_name),
            )
        scheduled_tasks_for_creation = self._get_schedules_from_broker_tasks()
        await self._update_schedules_on_startup(scheduled_tasks_for_creation)

    async def shutdown(self) -> None:
        """Close the connection pool."""
        if getattr(self, "_database_pool", None) is not None:
            await self._database_pool.close()

    async def get_schedules(self) -> list["ScheduledTask"]:
        """Fetch schedules from the database."""
        schedules = []
        async with self._database_pool.connection() as connection, connection.cursor() as cursor:
            rows_with_schedules = await cursor.execute(
                SELECT_SCHEDULES_QUERY.format(self._table_name),
            )
            rows = await rows_with_schedules.fetchall()
            for schedule_id, task_name, schedule in rows:
                schedules.append(
                    ScheduledTask.model_validate(
                        {
                            "schedule_id": str(schedule_id),
                            "task_name": task_name,
                            "labels": schedule["labels"],
                            "args": schedule["args"],
                            "kwargs": schedule["kwargs"],
                            "cron": schedule["cron"],
                            "cron_offset": schedule["cron_offset"],
                            "time": schedule["time"],
                        },
                    ),
                )
        return schedules
