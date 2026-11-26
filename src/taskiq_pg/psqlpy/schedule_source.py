import typing as tp
import uuid

from psqlpy import ConnectionPool
from psqlpy.extra_types import JSONB
from taskiq import ScheduledTask

from taskiq_pg._internal import BasePostgresScheduleSource
from taskiq_pg.psqlpy.queries import (
    CREATE_SCHEDULES_TABLE_QUERY,
    DELETE_SCHEDULE_QUERY,
    INSERT_SCHEDULE_QUERY,
    SELECT_SCHEDULES_QUERY,
)


class PSQLPyScheduleSource(BasePostgresScheduleSource):
    """Schedule source that uses psqlpy to store schedules in PostgreSQL."""

    _database_pool: ConnectionPool
    _owns_pool: bool

    @tp.overload
    def __init__(
        self,
        dsn: "str | tp.Callable[[], str]" = ...,
        table_name: str = ...,
        *,
        pool: None = ...,
        **connect_kwargs: tp.Any,
    ) -> None: ...

    @tp.overload
    def __init__(
        self,
        dsn: "str | tp.Callable[[], str]" = ...,
        table_name: str = ...,
        *,
        pool: ConnectionPool,
    ) -> None: ...

    def __init__(
        self,
        dsn: "str | tp.Callable[[], str]" = "postgresql://postgres:postgres@localhost:5432/postgres",
        table_name: str = "taskiq_schedules",
        *,
        pool: ConnectionPool | None = None,
        **connect_kwargs: tp.Any,
    ) -> None:
        """
        Construct a new PSQLPyScheduleSource.

        Args:
            dsn: PostgreSQL connection string or callable. Ignored in pool mode.
            table_name: Table to store schedules in.
            pool: An existing connection pool to reuse.
            **connect_kwargs: Extra kwargs for connection pool creation.
        """
        if pool is not None:
            self._owns_pool = False
            self._database_pool = pool
        else:
            self._owns_pool = True

        super().__init__(
            dsn=dsn,
            table_name=table_name,
            **connect_kwargs,
        )

    async def startup(self) -> None:
        """
        Initialize the schedule source.

        Construct new connection pool (if not provided externally), create new table for
        schedules if not exists.
        """
        if self._owns_pool:
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

    async def shutdown(self) -> None:
        """Close the connection pool if was created by this schedule source."""
        if self._owns_pool and getattr(self, "_database_pool", None) is not None:
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
                        "interval": schedule["interval"],
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
                ],
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
