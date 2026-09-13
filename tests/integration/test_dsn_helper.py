import uuid

import pytest

from taskiq_pg.aiopg import AiopgResultBackend
from taskiq_pg.asyncpg import AsyncpgBroker, AsyncpgResultBackend
from taskiq_pg.psqlpy import PSQLPyBroker, PSQLPyResultBackend
from taskiq_pg.psycopg import PsycopgBroker, PsycopgResultBackend


@pytest.mark.integration
@pytest.mark.parametrize(
    "dialect_suffix",
    [
        "postgres+asyncpg",
        "postgresql+asyncpg",
        "postgres+psycopg",
        "postgresql+psqlpy",
    ],
)
@pytest.mark.parametrize(
    "broker_class",
    [
        AsyncpgBroker,
        PSQLPyBroker,
        PsycopgBroker,
    ],
)
async def test_when_dsn_has_sqlalchemy_style_driver_suffix__then_broker_still_connects(
    pg_dsn: str,
    broker_class: type[AsyncpgBroker | PSQLPyBroker | PsycopgBroker],
    dialect_suffix: str,
) -> None:
    dsn_with_driver_suffix = pg_dsn.replace("postgres://", f"{dialect_suffix}://", 1)
    broker = broker_class(dsn=dsn_with_driver_suffix)

    try:
        await broker.startup()
    finally:
        await broker.shutdown()


@pytest.mark.integration
@pytest.mark.parametrize(
    "dialect_suffix",
    [
        "postgres+asyncpg",
        "postgresql+asyncpg",
        "postgres+psycopg",
        "postgresql+psqlpy",
        "postgres+aiopg",
    ],
)
@pytest.mark.parametrize(
    "result_backend_class",
    [
        AsyncpgResultBackend,
        AiopgResultBackend,
        PSQLPyResultBackend,
        PsycopgResultBackend,
    ],
)
async def test_when_dsn_has_sqlalchemy_style_driver_suffix__then_result_backend_still_connects(
    pg_dsn: str,
    result_backend_class: type[AsyncpgResultBackend | AiopgResultBackend | PSQLPyResultBackend | PsycopgResultBackend],
    dialect_suffix: str,
) -> None:
    dsn_with_driver_suffix = pg_dsn.replace("postgres://", f"{dialect_suffix}://", 1)
    table_name = f"taskiq_results_{uuid.uuid4().hex}"
    backend = result_backend_class(dsn=dsn_with_driver_suffix, table_name=table_name)

    try:
        await backend.startup()
    finally:
        await backend.shutdown()
