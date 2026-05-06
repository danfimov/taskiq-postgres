"""
How to run:

    1) Run worker in one terminal:
        uv run taskiq worker examples.example_with_shared_pool:get_broker --workers 1

    2) Run this script in another terminal:
        uv run python -m examples.example_with_shared_pool
"""

import asyncio

from psycopg import AsyncConnection, AsyncRawCursor
from psycopg_pool import AsyncConnectionPool
from taskiq import async_shared_broker

from taskiq_pg.psycopg import PsycopgBroker, PsycopgResultBackend


DSN = "postgres://taskiq_postgres:look_in_vault@localhost:5432/taskiq_postgres"


def create_pool() -> AsyncConnectionPool:
    return AsyncConnectionPool(conninfo=DSN, open=False, timeout=5)


async def create_connection() -> AsyncConnection:
    return await AsyncConnection.connect(
        conninfo=DSN,
        autocommit=True,
        cursor_factory=AsyncRawCursor,
    )


def make_broker(pool: AsyncConnectionPool, connection: AsyncConnection) -> PsycopgBroker:
    broker = PsycopgBroker(
        write_pool=pool,
        read_connection=connection,
    ).with_result_backend(PsycopgResultBackend(pool=pool))
    async_shared_broker.default_broker(broker)
    return broker


@async_shared_broker.task("solve_all_problems")
async def best_task_ever() -> None:
    """Solve all problems in the world."""
    await asyncio.sleep(2)
    print("All problems are solved!")


def get_broker() -> PsycopgBroker:
    """Sync factory used by the taskiq worker CLI."""
    pool = create_pool()
    connection = asyncio.run(create_connection())
    return make_broker(pool, connection)


async def main() -> None:
    pool = create_pool()
    connection = await create_connection()
    broker = make_broker(pool, connection)

    await broker.startup()
    task = await best_task_ever.kiq()
    print(await task.wait_result())
    await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
