[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/taskiq-postgres?style=for-the-badge&logo=python)](https://pypi.org/project/taskiq-postgres/)
[![PyPI](https://img.shields.io/pypi/v/taskiq-postgres?style=for-the-badge&logo=pypi)](https://pypi.org/project/taskiq-postgres/)
[![Checks](https://img.shields.io/github/check-runs/danfimov/taskiq-postgres/main?nameFilter=Tests%20(3.12)&style=for-the-badge)](https://github.com/danfimov/taskiq-postgres)

<div align="center">
<a href="https://github.com/danfimov/taskiq-postgres/"><img src="https://raw.githubusercontent.com/danfimov/taskiq-postgres/main/assets/logo.png" width=400></a>
<hr/>
</div>

PostgreSQL integration for Taskiq with support for asyncpg, psqlpy and aiopg drivers.

See more example of usage in [the documentation](https://danfimov.github.io/taskiq-postgres/).

## Installation

Depend on your preferred PostgreSQL driver, you can install this library:

=== "asyncpg"

    ```bash
    pip install taskiq-postgres[asyncpg]
    ```

=== "psqlpy"

    ```bash
    pip install taskiq-postgres[psqlpy]
    ```

=== "aiopg"

    ```bash
    pip install taskiq-postgres[aiopg]
    ```


## Usage example

Simple example of usage with [asyncpg](https://github.com/MagicStack/asyncpg):

```python
# broker.py
import asyncio

from taskiq_pg.asyncpg import AsyncpgResultBackend, AsyncpgBroker

result_backend = AsyncpgResultBackend(
    dsn="postgres://postgres:postgres@localhost:5432/postgres",
)

broker = AsyncpgBroker(
    dsn="postgres://postgres:postgres@localhost:5432/postgres",
).with_result_backend(result_backend)


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

Your experience with other drivers will be pretty similar. Just change the import statement and that's it.

## Motivation

There are too many libraries for PostgreSQL and Taskiq integration. Although they have different view on interface and different functionality.
To address this issue I created this library with a common interface for most popular PostgreSQL drivers that handle similarity across functionality of result backends, brokers and schedule sources.
