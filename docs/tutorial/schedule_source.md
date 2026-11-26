---
title: Schedule Source
---

## Basic usage

The PostgreSQL schedule source stores schedules in a database table: the schedules you add in runtime
(see below) and the ones you pre-populate the table with. They all survive application restarts.

Schedules declared in task labels are not read by this source: they are handled by taskiq's built-in
`LabelScheduleSource`. Add both sources to your scheduler:

  ```python
  import asyncio
  from taskiq import TaskiqScheduler
  from taskiq.schedule_sources import LabelScheduleSource
  from taskiq_pg.asyncpg import AsyncpgBroker, AsyncpgScheduleSource


  dsn = "postgres://taskiq_postgres:look_in_vault@localhost:5432/taskiq_postgres"
  broker = AsyncpgBroker(dsn)
  scheduler = TaskiqScheduler(
      broker=broker,
      sources=[
          LabelScheduleSource(broker),    # schedules declared in task labels
          AsyncpgScheduleSource(dsn=dsn), # schedules stored in PostgreSQL
      ],
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
  schedule_source = AsyncpgScheduleSource(dsn=dsn)
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

Schedules stored in the database are never deleted on startup — they survive restarts.
One-shot (`time`) schedules are removed automatically after they fire.

## Using multiple schedules

You can use multiple schedules for one task. Just provide a list of schedules to the `@broker.task` decorator.
These label-based schedules are picked up by `LabelScheduleSource`; the PostgreSQL source only stores
the schedules you add via `add_schedule`:

```python
import asyncio
from taskiq import TaskiqScheduler
from taskiq.schedule_sources import LabelScheduleSource
from taskiq_pg.asyncpg import AsyncpgBroker, AsyncpgScheduleSource


dsn = "postgres://taskiq_postgres:look_in_vault@localhost:5432/taskiq_postgres"
broker = AsyncpgBroker(dsn)
scheduler = TaskiqScheduler(
    broker=broker,
    sources=[
        LabelScheduleSource(broker),
        AsyncpgScheduleSource(dsn=dsn),
    ],
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
