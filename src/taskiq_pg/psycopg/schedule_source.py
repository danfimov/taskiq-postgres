import uuid
from logging import getLogger

from psycopg import sql
from psycopg_pool import AsyncConnectionPool
from taskiq import ScheduledTask

from taskiq_pg._internal import BasePostgresScheduleSource
from taskiq_pg.psycopg.queries import (
    CREATE_SCHEDULES_TABLE_QUERY,
    DELETE_ALL_SCHEDULES_QUERY,
    DELETE_SCHEDULE_QUERY,
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
            await cursor.execute(sql.SQL(DELETE_ALL_SCHEDULES_QUERY).format(sql.Identifier(self._table_name)))
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
                sql.SQL(INSERT_SCHEDULE_QUERY).format(sql.Identifier(self._table_name)),
                data_to_insert,
            )

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
                sql.SQL(CREATE_SCHEDULES_TABLE_QUERY).format(sql.Identifier(self._table_name)),
            )
        scheduled_tasks_for_creation = self.extract_scheduled_tasks_from_broker()
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
                sql.SQL(SELECT_SCHEDULES_QUERY).format(sql.Identifier(self._table_name)),
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

    async def add_schedule(self, schedule: "ScheduledTask") -> None:
        """
        Add a new schedule.

        Args:
            schedule: schedule to add.
        """
        async with self._database_pool.connection() as connection, connection.cursor() as cursor:
            await cursor.execute(
                sql.SQL(INSERT_SCHEDULE_QUERY).format(sql.Identifier(self._table_name)),
                [
                    uuid.UUID(schedule.schedule_id),
                    schedule.task_name,
                    schedule.model_dump_json(
                        exclude={"schedule_id", "task_name"},
                    ),
                ]
            )

    async def delete_schedule(self, schedule_id: str) -> None:
        """
        Method to delete schedule by id.

        This is useful for schedule cancelation.

        Args:
            schedule_id: id of schedule to delete.
        """
        async with self._database_pool.connection() as connection, connection.cursor() as cursor:
            await cursor.execute(
                sql.SQL(DELETE_SCHEDULE_QUERY).format(sql.Identifier(self._table_name)),
                [schedule_id],
            )

    async def post_send(self, task: ScheduledTask) -> None:
        """Delete a task after it's completed."""
        if task.time is not None:
            await self.delete_schedule(task.schedule_id)
