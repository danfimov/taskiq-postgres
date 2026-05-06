---
title: Sharing a Connection Pool
---

By default, each component (broker, result backend, schedule source) creates and manages its own connection pool. If you are integrating taskiq-postgres into an application that already maintains a pool — or if you simply want to reduce the total number of database connections — you can pass a single pool to all three components.

## How it works

`PsycopgBroker`, `PsycopgResultBackend`, and `PsycopgScheduleSource` each accept an optional `pool` (or `write_pool`) keyword argument. When a pool is provided:

- The component sets `_owns_pool = False` and uses the pool as-is.
- `startup()` opens the pool if it is not yet open, but will **not** close it on `shutdown()`.
- Lifecycle management (opening, closing) is your responsibility.

## Basic example

```python
import asyncio

from psycopg import AsyncConnection, AsyncRawCursor
from psycopg_pool import AsyncConnectionPool
from taskiq import TaskiqScheduler

from taskiq_pg.psycopg import PsycopgBroker, PsycopgResultBackend, PsycopgScheduleSource

DSN = "postgres://user:password@localhost:5432/mydb"

async def main() -> None:
    # Create one pool shared by all components.
    pool = AsyncConnectionPool(conninfo=DSN, open=False)
    # A dedicated connection is required by the broker for LISTEN/NOTIFY.
    read_conn = await AsyncConnection.connect(
        conninfo=DSN, autocommit=True, cursor_factory=AsyncRawCursor
    )

    broker = PsycopgBroker(
        write_pool=pool,
        read_connection=read_conn,
    ).with_result_backend(
        PsycopgResultBackend(pool=pool)
    )

    schedule_source = PsycopgScheduleSource(broker=broker, pool=pool)
    scheduler = TaskiqScheduler(broker=broker, sources=[schedule_source])

    await broker.startup()
    # ... run your application ...
    await broker.shutdown()

    # Close shared resources after all components have shut down.
    await read_conn.close()
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
```

You can see fully working example inside repository in `examples/example_with_shared_pool.py`.
