"""
How to run:

    1) Run worker in one terminal:
        uv run taskiq worker examples.example_with_progress:broker --workers 1

    2) Run this script in another terminal:
        uv run python -m examples.example_with_progress
"""

import asyncio
from typing import Any

from taskiq import TaskiqDepends
from taskiq.depends.progress_tracker import ProgressTracker, TaskState

from taskiq_pg.psycopg import PsycopgBroker, PsycopgResultBackend


dsn = "postgres://taskiq_postgres:look_in_vault@localhost:5432/taskiq_postgres"
broker = PsycopgBroker(dsn).with_result_backend(PsycopgResultBackend(dsn))


@broker.task("solve_all_problems")
async def best_task_ever(
    progress_tracker: ProgressTracker[Any] = TaskiqDepends(),  # noqa: B008
) -> int:
    state_dict = {"start_message": "Starting to solve problems"}
    await progress_tracker.set_progress(TaskState.STARTED, state_dict)

    await asyncio.sleep(2)

    state_dict.update({"halfway_message": "Halfway done!"})
    await progress_tracker.set_progress("halfway", state_dict)
    await progress_tracker.set_progress(TaskState.STARTED, state_dict)

    await asyncio.sleep(2)

    return 42


async def main():
    await broker.startup()
    task = await best_task_ever.kiq()

    # check progress on start
    await asyncio.sleep(1)
    print(await task.get_progress())

    # check progress on halfway
    await asyncio.sleep(2)
    print(await task.get_progress())

    # get final result
    print(await task.wait_result())

    await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
