"""
How to run:

    1) Run worker in one terminal:
        uv run taskiq worker examples.example_with_broker:broker

    2) Run this script in another terminal:
        uv run python -m examples.example_with_broker
"""

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
