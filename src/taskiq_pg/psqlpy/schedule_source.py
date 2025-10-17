import uuid
from logging import getLogger

from psqlpy import ConnectionPool
from psqlpy.extra_types import JSONB
from taskiq import ScheduledTask

from taskiq_pg._internal import BasePostgresScheduleSource
from taskiq_pg.psqlpy.queries import (
    CREATE_SCHEDULES_TABLE_QUERY,
    DELETE_ALL_SCHEDULES_QUERY,
    DELETE_SCHEDULE_QUERY,
    INSERT_SCHEDULE_QUERY,
    SELECT_SCHEDULES_QUERY,
)


logger = getLogger("taskiq_pg.psqlpy_schedule_source")


class PSQLPyScheduleSource(BasePostgresScheduleSource):
    """Schedule source that uses psqlpy to store schedules in PostgreSQL."""

    _database_pool: ConnectionPool

    async def _update_schedules_on_startup(self, schedules: list[ScheduledTask]) -> None:
        """Update schedules in the database on startup: truncate table and insert new ones."""
        async with self._database_pool.acquire() as connection, connection.transaction():
            await connection.execute(DELETE_ALL_SCHEDULES_QUERY.format(self._table_name))
            data_to_insert: list = []
            for schedule in schedules:
                schedule_dict = schedule.model_dump(
                    mode="json",
                    exclude={"schedule_id", "task_name"},
                )
                data_to_insert.append(
                    [
                        uuid.UUID(schedule.schedule_id),
                        schedule.task_name,
                        JSONB(schedule_dict),
                    ]
                )

            await connection.execute_many(
                INSERT_SCHEDULE_QUERY.format(self._table_name),
                data_to_insert,
            )

    async def startup(self) -> None:
        """
        Initialize the schedule source.

        Construct new connection pool, create new table for schedules if not exists
        and fill table with schedules from task labels.
        """
        self._database_pool = ConnectionPool(
            dsn=self.dsn,
            **self._connect_kwargs,
        )
        async with self._database_pool.acquire() as connection:
            await connection.execute(
                CREATE_SCHEDULES_TABLE_QUERY.format(
                    self._table_name,
                ),
            )
        scheduled_tasks_for_creation = self.extract_scheduled_tasks_from_broker()
        await self._update_schedules_on_startup(scheduled_tasks_for_creation)

    async def shutdown(self) -> None:
        """Close the connection pool."""
        if getattr(self, "_database_pool", None) is not None:
            self._database_pool.close()

    async def get_schedules(self) -> list["ScheduledTask"]:
        """Fetch schedules from the database."""
        async with self._database_pool.acquire() as connection:
            rows_with_schedules = await connection.fetch(
                SELECT_SCHEDULES_QUERY.format(self._table_name),
            )
        schedules = []
        for row in rows_with_schedules.result():
            schedule = row["schedule"]
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
        async with self._database_pool.acquire() as connection:
            schedule_dict = schedule.model_dump(
                mode="json",
                exclude={"schedule_id", "task_name"},
            )
            await connection.execute(
                INSERT_SCHEDULE_QUERY.format(self._table_name),
                [
                    uuid.UUID(schedule.schedule_id),
                    schedule.task_name,
                    JSONB(schedule_dict),
                ]
            )

    async def delete_schedule(self, schedule_id: str) -> None:
        """
        Method to delete schedule by id.

        This is useful for schedule cancelation.

        Args:
            schedule_id: id of schedule to delete.
        """
        async with self._database_pool.acquire() as connection:
            await connection.execute(
                DELETE_SCHEDULE_QUERY.format(self._table_name),
                [uuid.UUID(schedule_id)],
            )

    async def post_send(self, task: ScheduledTask) -> None:
        """Delete a task after it's completed."""
        if task.time is not None:
            await self.delete_schedule(task.schedule_id)
