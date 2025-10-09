---
title: Broker
---

To use broker with PostgreSQL you need to import broker and result backend from this library and provide a address for  connection. For example, lets create a file `broker.py` with the following content:

=== "asyncpg"

    ```python
    import asyncio
    from taskiq_pg.asyncpg import AsyncpgResultBackend, AsyncpgBroker


    dsn = "postgres://postgres:postgres@localhost:5432/postgres"
    broker = AsyncpgBroker(dsn).with_result_backend(AsyncpgResultBackend(dsn))


    @broker.task
    async def best_task_ever() -> None:
        """Solve all problems in the world."""
        await asyncio.sleep(5.5)
        print("All problems are solved!")


    async def main():
        await broker.startup()
        task = await best_task_ever.kiq()
        print(await task.wait_result())
        await broker.shutdown()


    if __name__ == "__main__":
        asyncio.run(main())
    ```

=== "psqlpy"

    ```python
    import asyncio
    from taskiq_pg.psqlpy import PSQLPyResultBackend, PSQLPyBroker


    dsn = "postgres://postgres:postgres@localhost:5432/postgres"
    broker = PSQLPyBroker(dsn).with_result_backend(PSQLPyResultBackend(dsn))


    @broker.task
    async def best_task_ever() -> None:
        """Solve all problems in the world."""
        await asyncio.sleep(5.5)
        print("All problems are solved!")


    async def main():
        await broker.startup()
        task = await best_task_ever.kiq()
        print(await task.wait_result())
        await broker.shutdown()


    if __name__ == "__main__":
        asyncio.run(main())
    ```

Then you can run this file with:

```bash
python broker.py
```
