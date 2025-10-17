---
title: Schedule Source
---

## Basic usage

The easiest way to schedule task with this library is to add `schedule` label to task. Schedule source will automatically
parse this label and add new schedule to database on start of scheduler.

You can define your scheduled task like this:

  ```python
  import asyncio
  from taskiq import TaskiqScheduler
  from taskiq_pg.asyncpg import AsyncpgBroker, AsyncpgScheduleSource


  dsn = "postgres://taskiq_postgres:look_in_vault@localhost:5432/taskiq_postgres"
  broker = AsyncpgBroker(dsn)
  scheduler = TaskiqScheduler(
      broker=broker,
      sources=[AsyncpgScheduleSource(
          dsn=dsn,
          broker=broker,
      )],
  )


  @broker.task(
      task_name="solve_all_problems",
      schedule=[
          {
              "cron": "*/1 * * * *",  # type: str, either cron or time should be specified.
              "cron_offset": None,  # type: str | None, can be omitted. For example "Europe/Berlin".
              "time": None,  # type: datetime | None, either cron or time should be specified.
              "args": [], # type list[Any] | None, can be omitted.
              "kwargs": {}, # type: dict[str, Any] | None, can be omitted.
              "labels": {}, # type: dict[str, Any] | None, can be omitted.
          },
      ],
  )
  async def best_task_ever() -> None:
      """Solve all problems in the world."""
      await asyncio.sleep(2)
      print("All problems are solved!")
  ```


## Adding schedule in runtime

You can also add schedules in runtime using `add_schedule` method of the schedule source:


  ```python
  import asyncio
  from taskiq import TaskiqScheduler, ScheduledTask
  from taskiq_pg.asyncpg import AsyncpgBroker, AsyncpgScheduleSource


  dsn = "postgres://taskiq_postgres:look_in_vault@localhost:5432/taskiq_postgres"
  broker = AsyncpgBroker(dsn)
  schedule_source = AsyncpgScheduleSource(
      dsn=dsn,
      broker=broker,
  )
  scheduler = TaskiqScheduler(
      broker=broker,
      sources=[schedule_source],
  )


  @broker.task(
      task_name="solve_all_problems",
  )
  async def best_task_ever() -> None:
      """Solve all problems in the world."""
      await asyncio.sleep(2)
      print("All problems are solved!")

  # Call this function somewhere in your code to add new schedule
  async def add_new_schedule() -> None:
      await schedule_source.add_schedule(ScheduledTask(...))
  ```

## Using multiple schedules

You can use multiple schedules for one task. Just provide a list of schedules to the `@broker.task` decorator:

```python
import asyncio
from taskiq import TaskiqScheduler
from taskiq_pg.asyncpg import AsyncpgBroker, AsyncpgScheduleSource


dsn = "postgres://taskiq_postgres:look_in_vault@localhost:5432/taskiq_postgres"
broker = AsyncpgBroker(dsn)
scheduler = TaskiqScheduler(
    broker=broker,
    sources=[AsyncpgScheduleSource(
        dsn=dsn,
        broker=broker,
    )],
)


@broker.task(
    task_name="solve_all_problems",
    schedule=[
        {
            "cron": "*/1 * * * *",
            "cron_offset": None,
            "time": None,
            "args": ["every minute"],
            "kwargs": {},
            "labels": {},
        },
        {
            "cron": "0 */1 * * *",
            "cron_offset": None,
            "time": None,
            "args": ["every hour"],
            "kwargs": {},
            "labels": {},
        },
    ],
)
async def best_task_ever(message: str) -> None:
    await asyncio.sleep(2)
    print(f"I run {message}")
```
