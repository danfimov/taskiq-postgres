import json
import typing as tp
import uuid
from logging import getLogger

import asyncpg
from pydantic import ValidationError
from taskiq import ScheduledTask, ScheduleSource
from taskiq.abc.broker import AsyncBroker

from taskiq_pg.asyncpg.queries import (
    CREATE_SCHEDULES_TABLE_QUERY,
    DELETE_ALL_SCHEDULES_QUERY,
    INSERT_SCHEDULE_QUERY,
    SELECT_SCHEDULES_QUERY,
)


logger = getLogger("taskiq_pg.asyncpg_schedule_source")


class AsyncpgScheduleSource(ScheduleSource):
    """Schedule source that uses asyncpg to store schedules in PostgreSQL."""

    _database_pool: "asyncpg.Pool[asyncpg.Record]"

    def __init__(
        self,
        broker: AsyncBroker,
        dsn: str | tp.Callable[[], str] = "postgresql://postgres:postgres@localhost:5432/postgres",
        table_name: str = "taskiq_schedules",
        **connect_kwargs: tp.Any,
    ) -> None:
        """
        Initialize the PostgreSQL scheduler source.

        Sets up a scheduler source that stores scheduled tasks in a PostgreSQL database.
        This scheduler source manages task schedules, allowing for persistent storage and retrieval of scheduled tasks
        across application restarts.

        Args:
            dsn: PostgreSQL connection string
            table_name: Name of the table to store scheduled tasks. Will be created automatically if it doesn't exist.
            broker: The TaskIQ broker instance to use for finding and managing tasks.
                Required if startup_schedule is provided.
            **connect_kwargs: Additional keyword arguments passed to the database connection pool.

        """
        self._broker: tp.Final = broker
        self._dsn: tp.Final = dsn
        self._table_name: tp.Final = table_name
        self._connect_kwargs: tp.Final = connect_kwargs

    @property
    def dsn(self) -> str | None:
        """
        Get the DSN string.

        Returns the DSN string or None if not set.
        """
        if callable(self._dsn):
            return self._dsn()
        return self._dsn

    async def _update_schedules_on_startup(self, schedules: list[ScheduledTask]) -> None:
        """Update schedules in the database on startup: trancate table and insert new ones."""
        async with self._database_pool.acquire() as connection, connection.transaction():
            await connection.execute(DELETE_ALL_SCHEDULES_QUERY.format(self._table_name))
            for schedule in schedules:
                schedule.model_dump_json(
                    exclude={"schedule_id", "task_name"},
                )
                await self._database_pool.execute(
                    INSERT_SCHEDULE_QUERY.format(self._table_name),
                    str(schedule.schedule_id),
                    schedule.task_name,
                    schedule.model_dump_json(
                        exclude={"schedule_id", "task_name"},
                    ),
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
        self._database_pool = await asyncpg.create_pool(
            dsn=self.dsn,
            **self._connect_kwargs,
        )
        await self._database_pool.execute(
            CREATE_SCHEDULES_TABLE_QUERY.format(
                self._table_name,
            ),
        )
        scheduled_tasks_for_creation = self._get_schedules_from_broker_tasks()
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
