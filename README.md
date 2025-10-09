[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/taskiq-postgres?style=for-the-badge&logo=python)](https://pypi.org/project/taskiq-postgres/)
[![PyPI](https://img.shields.io/pypi/v/taskiq-postgres?style=for-the-badge&logo=pypi)](https://pypi.org/project/taskiq-postgres/)
[![Checks](https://img.shields.io/github/check-runs/danfimov/taskiq-postgres/main?nameFilter=Tests%20(3.12)&style=for-the-badge)](https://github.com/danfimov/taskiq-postgres)

<div align="center">
<a href="https://github.com/danfimov/taskiq-postgres/"><img src="https://raw.githubusercontent.com/danfimov/taskiq-postgres/main/assets/logo.png" width=400></a>
<hr/>
</div>

PostgreSQL integration for Taskiq with support for asyncpg, psqlpy and aiopg drivers.

See more example of usage in [the documentation](https://danfimov.github.io/taskiq-postgres/) or [examples directory](https://github.com/danfimov/taskiq-postgres/examples).

## Installation

Depending on your preferred PostgreSQL driver, you can install this library with the corresponding extra:

```bash
# with asyncpg
pip install taskiq-postgres[asyncpg]

# with psqlpy
pip install taskiq-postgres[psqlpy]

# with aiopg
pip install taskiq-postgres[aiopg]
```

## Quick start

### Basic task processing

1. Define your broker with [asyncpg](https://github.com/MagicStack/asyncpg):

  ```python
  # broker_example.py
  import asyncio
  from taskiq_pg.asyncpg import AsyncpgBroker, AsyncpgResultBackend


  dsn = "postgres://taskiq_postgres:look_in_vault@localhost:5432/taskiq_postgres"
  broker = AsyncpgBroker(dsn).with_result_backend(AsyncpgResultBackend(dsn))


  @broker.task("solve_all_problems")
  async def best_task_ever() -> None:
      """Solve all problems in the world."""
      await asyncio.sleep(2)
      print("All problems are solved!")


  async def main():
      await broker.startup()
      task = await best_task_ever.kiq()
      print(await task.wait_result())
      await broker.shutdown()


  if __name__ == "__main__":
      asyncio.run(main())
  ```

2. Start a worker to process tasks (by default taskiq runs two instances of worker):

  ```bash
  taskiq worker broker_example:broker
  ```

3. Run `broker_example.py` file to send a task to the worker:

  ```bash
  python broker_example.py
  ```

Your experience with other drivers will be pretty similar. Just change the import statement and that's it.

### Task scheduling

1. Define your broker and schedule source:

  ```python
  # scheduler_example.py
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
              "cron_offset": None,  # type: str | timedelta | None, can be omitted.
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

2. Start worker processes:

  ```bash
  taskiq worker scheduler_example:broker
  ```

3. Run scheduler process:

  ```bash
  taskiq scheduler scheduler_example:scheduler
  ```

## Motivation

There are too many libraries for PostgreSQL and Taskiq integration. Although they have different view on interface and different functionality.
To address this issue I created this library with a common interface for most popular PostgreSQL drivers that handle similarity across functionality of result backends, brokers and schedule sources.
