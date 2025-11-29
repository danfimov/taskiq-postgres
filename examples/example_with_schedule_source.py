"""
How to run:

    1) Run worker in one terminal:
        uv run taskiq worker examples.example_with_schedule_source:broker --workers 1

    2) Run scheduler in another terminal:
        uv run taskiq scheduler examples.example_with_schedule_source:scheduler
"""

import asyncio

from taskiq import TaskiqScheduler

from taskiq_pg.psycopg import PsycopgBroker, PsycopgScheduleSource


dsn = "postgres://taskiq_postgres:look_in_vault@localhost:5432/taskiq_postgres"
broker = PsycopgBroker(dsn)
scheduler = TaskiqScheduler(
    broker=broker,
    sources=[
        PsycopgScheduleSource(
            dsn=dsn,
            broker=broker,
        ),
    ],
)


@broker.task(
    task_name="solve_all_problems",
    schedule=[
        {
            "cron": "*/1 * * * *",  # type: str, either cron or time should be specified.
            "cron_offset": None,  # type: str | timedelta | None, can be omitted.
            "time": None,  # type: datetime | None, either cron or time should be specified.
            "args": [],  # type list[Any] | None, can be omitted.
            "kwargs": {},  # type: dict[str, Any] | None, can be omitted.
            "labels": {},  # type: dict[str, Any] | None, can be omitted.
        },
        {
            "interval": 10, # type: int | timedelta | None
        },
    ],
)
async def best_task_ever() -> None:
    """Solve all problems in the world."""
    await asyncio.sleep(2)
    print("All problems are solved!")
