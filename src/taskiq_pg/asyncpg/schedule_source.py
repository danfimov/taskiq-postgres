import json
from logging import getLogger

import asyncpg
from taskiq import ScheduledTask

from taskiq_pg._internal import BasePostgresScheduleSource
from taskiq_pg.asyncpg.queries import (
    CREATE_SCHEDULES_TABLE_QUERY,
    DELETE_ALL_SCHEDULES_QUERY,
    DELETE_SCHEDULE_QUERY,
    INSERT_SCHEDULE_QUERY,
    SELECT_SCHEDULES_QUERY,
)


logger = getLogger("taskiq_pg.asyncpg_schedule_source")


class AsyncpgScheduleSource(BasePostgresScheduleSource):
    """Schedule source that uses asyncpg to store schedules in PostgreSQL."""

    _database_pool: "asyncpg.Pool[asyncpg.Record]"

    async def _update_schedules_on_startup(self, schedules: list[ScheduledTask]) -> None:
        """Update schedules in the database on startup: truncate table and insert new ones."""
        async with self._database_pool.acquire() as connection, connection.transaction():
            await connection.execute(DELETE_ALL_SCHEDULES_QUERY.format(self._table_name))
            for schedule in schedules:
                await self._database_pool.execute(
                    INSERT_SCHEDULE_QUERY.format(self._table_name),
                    str(schedule.schedule_id),
                    schedule.task_name,
                    schedule.model_dump_json(
                        exclude={"schedule_id", "task_name"},
                    ),
                )

    async def startup(self) -> None:
        """
        Initialize the schedule source.

        Construct new connection pool, create new table for schedules if not exists
        and fill table with schedules from task labels.
        """
        self._database_pool = await asyncpg.create_pool(
            dsn=self.dsn,
            **self._connect_kwargs,
        )
        await self._database_pool.execute(
            CREATE_SCHEDULES_TABLE_QUERY.format(
                self._table_name,
            ),
        )
        scheduled_tasks_for_creation = self.extract_scheduled_tasks_from_broker()
        await self._update_schedules_on_startup(scheduled_tasks_for_creation)

    async def shutdown(self) -> None:
        """Close the connection pool."""
        if getattr(self, "_database_pool", None) is not None:
            await self._database_pool.close()

    async def get_schedules(self) -> list["ScheduledTask"]:
        """Fetch schedules from the database."""
        async with self._database_pool.acquire() as conn:
            rows_with_schedules = await conn.fetch(
                SELECT_SCHEDULES_QUERY.format(self._table_name),
            )
        schedules = []
        for row in rows_with_schedules:
            schedule = json.loads(row["schedule"])
            schedules.append(
                ScheduledTask.model_validate(
                    {
                        "schedule_id": str(row["id"]),
                        "task_name": row["task_name"],
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

    async def add_schedule(self, schedule: "ScheduledTask") -> None:
        """
        Add a new schedule.

        Args:
            schedule: schedule to add.
        """
        await self._database_pool.execute(
            INSERT_SCHEDULE_QUERY.format(self._table_name),
            str(schedule.schedule_id),
            schedule.task_name,
            schedule.model_dump_json(
                exclude={"schedule_id", "task_name"},
            ),
        )

    async def delete_schedule(self, schedule_id: str) -> None:
        """
        Method to delete schedule by id.

        This is useful for schedule cancelation.

        Args:
            schedule_id: id of schedule to delete.
        """
        await self._database_pool.execute(
            DELETE_SCHEDULE_QUERY.format(self._table_name),
            schedule_id,
        )

    async def post_send(self, task: ScheduledTask) -> None:
        """Delete a task after it's completed."""
        if task.time is not None:
            await self.delete_schedule(task.schedule_id)
