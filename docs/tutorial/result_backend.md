---
title: Result Backend
---

## Basic usage

You can store task results in Postgres using one of result backend classes from this package.

You can define your broker with result backend like this:

```python
import asyncio
from taskiq import TaskiqBroker
# 1. Import AsyncpgBroker and AsyncpgResultBackend (or other result backend you want to use)
from taskiq_pg.asyncpg import AsyncpgBroker, AsyncpgResultBackend

# 2. Define your broker with result backend
dsn = "postgres://taskiq_postgres:look_in_vault@localhost:5432/taskiq_postgres"
broker = AsyncpgBroker(dsn).with_result_backend(AsyncpgResultBackend(dsn=dsn)

# 3. Register task
@broker.task(task_name="answer_for_everything")
async def answer_for_everything() -> None:
    await asyncio.sleep(2)
    return 42

async def main():
    # 4. Start broker, call task and wait for result
    await broker.startup()
    task = await best_task_ever.kiq()
    print(await task.wait_result())
    await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
```

After running this code, you should see `42` printed in the console. Plus the result will be stored in the Postgres database in `taskiq_results` (by default).

## Customization

You can customize the result backend by providing additional parameters to the constructor.

- `keep_results` - whatever to keep results after they are fetched. Default is `True`. Suitable if you don't want to store results forever.
- `table_name` - name of the table to store results in. Default is `taskiq_results`.
- `field_for_task_id` - type of the field to store task IDs. Default is `VarChar`. But you can pick `Uuid` or `Text` if you want.
- `serializer` - serializer to use for serializing results. Default is `PickleSerializer`. But if you want human readable results you can use `JsonSerializer` from `taskiq.serializers` for example.

## Task progress

You can also store task progress using result backend. To do this, you need to use `set_progress` method from `ProgressTracker`:

```python
import asyncio
from taskiq import TaskiqBroker
# 1. Import AsyncpgBroker and AsyncpgResultBackend (or other result backend you want to use)
from taskiq_pg.asyncpg import AsyncpgBroker, AsyncpgResultBackend

# 2. Define your broker with result backend
dsn = "postgres://taskiq_postgres:look_in_vault@localhost:5432/taskiq_postgres"
broker = AsyncpgBroker(dsn).with_result_backend(AsyncpgResultBackend(dsn=dsn)

# 3. Register task
@broker.task("solve_all_problems")
async def best_task_ever(
    progress_tracker: ProgressTracker[Any] = TaskiqDepends(),  # noqa: B008
) -> int:
    # 4. Set progress with state
    state_dict = {"start_message": "Starting to solve problems"}
    await progress_tracker.set_progress(TaskState.STARTED, state_dict)

    await asyncio.sleep(2)

    # You can also use custom states, but progress will be rewritten on each call (it's update not merge)
    state_dict.update({"halfway_message": "Halfway done!"})
    await progress_tracker.set_progress("halfway", state_dict)
    await progress_tracker.set_progress(TaskState.STARTED, state_dict)

    await asyncio.sleep(2)

    return 42

async def main():
    # 5. Start broker
    await broker.startup()
    task = await best_task_ever.kiq()

    # 6. Check progress on start
    await asyncio.sleep(1)
    print(await task.get_progress())

    # 7. Check progress on halfway
    await asyncio.sleep(2)
    print(await task.get_progress())

    # 8. Get final result
    print(await task.wait_result())

    await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
```

If you run this code, you should see something like this in the console:

```bash
> uv run python -m examples.example_with_progress

state='STARTED' meta={'start_message': 'Starting to solve problems'}
state='STARTED' meta={'start_message': 'Starting to solve problems', 'halfway_message': 'Halfway done!'}
is_err=False log=None return_value=42 execution_time=4.01 labels={} error=None
```
