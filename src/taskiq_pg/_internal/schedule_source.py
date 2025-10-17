from __future__ import annotations

import typing as tp
import uuid
from logging import getLogger

from pydantic import ValidationError
from taskiq import ScheduleSource
from taskiq.scheduler.scheduled_task import ScheduledTask


if tp.TYPE_CHECKING:
    from taskiq.abc.broker import AsyncBroker


logger = getLogger("taskiq_pg")


class BasePostgresScheduleSource(ScheduleSource):
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

    def extract_scheduled_tasks_from_broker(self) -> list[ScheduledTask]:
        """
        Extract schedules from tasks that were registered in broker.

        Returns:
            A list of ScheduledTask instances extracted from the task's labels.
        """
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
                except ValidationError:  # noqa: PERF203
                    logger.exception(
                        "Schedule for task %s is not valid, skipping",
                        task_name,
                    )
                    continue
        return scheduled_tasks_for_creation
