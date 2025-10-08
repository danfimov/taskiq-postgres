import os

import pytest


@pytest.fixture(scope="session")
def pg_dsn() -> str:
    return os.getenv(
        "TASKIQ_PG_DSN",
        "postgres://taskiq_postgres:look_in_vault@localhost:5432/taskiq_postgres",
    )
