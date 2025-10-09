---
title: Schedule Source
---


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
