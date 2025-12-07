import os
import typing as tp
import uuid

import asyncpg
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy_utils import create_database, database_exists
from taskiq import InMemoryBroker

from taskiq_pg._internal.result_backend import BasePostgresResultBackend


@pytest.fixture(scope="session", autouse=True)
def clean_database():
    dsn = os.getenv(
        "TASKIQ_PG_DSN",
        "postgres://taskiq_postgres:look_in_vault@localhost:5432/taskiq_postgres",
    )
    temporary_prefix = uuid.uuid4().hex
    dsn_for_sqlalchemy = dsn.replace("postgres://", "postgresql://") + f"{temporary_prefix}"
    if not database_exists(dsn_for_sqlalchemy):
        create_database(dsn_for_sqlalchemy)
    try:
        yield dsn + temporary_prefix
    finally:
        engine = create_engine(dsn.replace("postgres://", "postgresql://"))
        dbname = "taskiq_postgres" + temporary_prefix
        with engine.connect() as conn:
            # DROP DATABASE can only be executed outside of a transaction
            conn = conn.execution_options(isolation_level="AUTOCOMMIT")  # noqa: PLW2901
            conn.execute(
                text(
                    "SELECT pg_terminate_backend(pid) "
                    "FROM pg_stat_activity "
                    "WHERE datname = :dbname AND pid <> pg_backend_pid()"
                ),
                {"dbname": dbname},
            )
            conn.execute(text(f'DROP DATABASE "{dbname}"'))
        engine.dispose()


@pytest.fixture
async def pg_dsn(clean_database: str):
    connection = await asyncpg.connect(dsn=clean_database)
    transaction = connection.transaction()
    try:
        await transaction.start()
        yield clean_database
    finally:
        await transaction.rollback()
        await connection.close()


@pytest.fixture
async def connection(pg_dsn: str) -> tp.AsyncGenerator[asyncpg.Connection, None]:
    connection = await asyncpg.connect(dsn=pg_dsn)
    try:
        yield connection
    finally:
        await connection.close()


@pytest.fixture
async def broker_with_backend(
    pg_dsn: str,
    request: pytest.FixtureRequest,
) -> tuple[InMemoryBroker, BasePostgresResultBackend]:
    table_name: str = f"taskiq_results_{uuid.uuid4().hex}"
    result_backend = request.param(
        dsn=pg_dsn,
        table_name=table_name,
    )
    broker = InMemoryBroker().with_result_backend(result_backend)
    await result_backend.startup()
    return broker, result_backend
