import uuid

import asyncpg
import psqlpy
import pytest
from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from taskiq_pg.asyncpg import AsyncpgBroker, AsyncpgResultBackend, AsyncpgScheduleSource
from taskiq_pg.psqlpy import PSQLPyBroker, PSQLPyResultBackend, PSQLPyScheduleSource
from taskiq_pg.psycopg import PsycopgBroker, PsycopgResultBackend, PsycopgScheduleSource


@pytest.mark.integration
async def test_asyncpg_shared_pool_not_closed_after_shutdown(pg_dsn: str) -> None:
    """Pool stays usable after broker, result_backend and schedule_source shut down."""
    pool: asyncpg.Pool = await asyncpg.create_pool(dsn=pg_dsn)

    broker = AsyncpgBroker(write_pool=pool, read_connection=await asyncpg.connect(dsn=pg_dsn))
    result_backend = AsyncpgResultBackend(
        dsn=pg_dsn,
        table_name=f"taskiq_results_{uuid.uuid4().hex}",
        pool=pool,
    )
    schedule_source = AsyncpgScheduleSource(
        broker=broker,
        dsn=pg_dsn,
        table_name=f"taskiq_schedules_{uuid.uuid4().hex}",
        pool=pool,
    )

    await broker.startup()
    await result_backend.startup()
    await schedule_source.startup()

    await schedule_source.shutdown()
    await result_backend.shutdown()
    await broker.shutdown()

    # Pool must still be alive — execute a simple query on it
    result = await pool.fetchval("SELECT 1")
    assert result == 1

    await pool.close()


@pytest.mark.integration
async def test_asyncpg_broker_uses_provided_write_pool(pg_dsn: str) -> None:
    """Broker created with only write_pool (no read_connection) creates its own read conn."""
    pool: asyncpg.Pool = await asyncpg.create_pool(dsn=pg_dsn)

    broker = AsyncpgBroker(dsn=pg_dsn, write_pool=pool)
    await broker.startup()
    await broker.shutdown()

    # Pool must still be alive after broker shutdown
    result = await pool.fetchval("SELECT 1")
    assert result == 1

    await pool.close()


@pytest.mark.integration
async def test_asyncpg_broker_uses_provided_read_connection(pg_dsn: str) -> None:
    """Broker created with only read_connection (no write_pool) creates its own pool."""
    read_conn = await asyncpg.connect(dsn=pg_dsn)

    broker = AsyncpgBroker(dsn=pg_dsn, read_connection=read_conn)
    await broker.startup()
    await broker.shutdown()

    # Connection must still be alive after broker shutdown
    result = await read_conn.fetchval("SELECT 1")
    assert result == 1

    await read_conn.close()


@pytest.mark.integration
async def test_psycopg_shared_pool_not_closed_after_shutdown(pg_dsn: str) -> None:
    """Pool stays usable after broker, result_backend and schedule_source shut down."""
    pool = AsyncConnectionPool(conninfo=pg_dsn, open=False)
    await pool.open()

    read_conn = await AsyncConnection.connect(conninfo=pg_dsn, autocommit=True)
    broker = PsycopgBroker(write_pool=pool, read_connection=read_conn)
    result_backend = PsycopgResultBackend(
        dsn=pg_dsn,
        table_name=f"taskiq_results_{uuid.uuid4().hex}",
        pool=pool,
    )
    schedule_source = PsycopgScheduleSource(
        broker=broker,
        dsn=pg_dsn,
        table_name=f"taskiq_schedules_{uuid.uuid4().hex}",
        pool=pool,
    )

    await broker.startup()
    await result_backend.startup()
    await schedule_source.startup()

    await schedule_source.shutdown()
    await result_backend.shutdown()
    await broker.shutdown()

    # Pool must still be alive
    async with pool.connection() as conn:
        result = await conn.execute("SELECT 1")
        row = await result.fetchone()
    assert row is not None
    assert row[0] == 1

    await pool.close()


@pytest.mark.integration
async def test_psycopg_broker_uses_provided_write_pool(pg_dsn: str) -> None:
    """Broker created with only write_pool creates its own read connection."""
    pool = AsyncConnectionPool(conninfo=pg_dsn, open=False)
    await pool.open()

    broker = PsycopgBroker(dsn=pg_dsn, write_pool=pool)
    await broker.startup()
    await broker.shutdown()

    async with pool.connection() as conn:
        result = await conn.execute("SELECT 1")
        row = await result.fetchone()
    assert row is not None
    assert row[0] == 1

    await pool.close()


@pytest.mark.integration
async def test_psycopg_broker_uses_provided_read_connection(pg_dsn: str) -> None:
    """Broker created with only read_connection creates its own pool."""
    read_conn = await AsyncConnection.connect(conninfo=pg_dsn, autocommit=True)

    broker = PsycopgBroker(dsn=pg_dsn, read_connection=read_conn)
    await broker.startup()
    await broker.shutdown()

    # Connection must still be alive
    result = await read_conn.execute("SELECT 1")
    row = await result.fetchone()
    assert row is not None
    assert row[0] == 1

    await read_conn.close()


@pytest.mark.integration
async def test_psqlpy_shared_pool_not_closed_after_shutdown(pg_dsn: str) -> None:
    """Pool stays usable after broker, result_backend and schedule_source shut down."""
    pool = psqlpy.ConnectionPool(dsn=pg_dsn)
    read_conn = await psqlpy.connect(dsn=pg_dsn)

    broker = PSQLPyBroker(write_pool=pool, read_connection=read_conn)
    result_backend = PSQLPyResultBackend(
        dsn=pg_dsn,
        table_name=f"taskiq_results_{uuid.uuid4().hex}",
        pool=pool,
    )
    schedule_source = PSQLPyScheduleSource(
        broker=broker,
        dsn=pg_dsn,
        table_name=f"taskiq_schedules_{uuid.uuid4().hex}",
        pool=pool,
    )

    await broker.startup()
    await result_backend.startup()
    await schedule_source.startup()

    await schedule_source.shutdown()
    await result_backend.shutdown()
    await broker.shutdown()

    # Pool must still be alive
    async with pool.acquire() as conn:
        result = await conn.fetch("SELECT 1 AS val")
    assert result.result()[0]["val"] == 1

    pool.close()


@pytest.mark.integration
async def test_psqlpy_broker_uses_provided_write_pool(pg_dsn: str) -> None:
    """Broker created with only write_pool creates its own read connection."""
    pool = psqlpy.ConnectionPool(dsn=pg_dsn)

    broker = PSQLPyBroker(dsn=pg_dsn, write_pool=pool)
    await broker.startup()
    await broker.shutdown()

    async with pool.acquire() as conn:
        result = await conn.fetch("SELECT 1 AS val")
    assert result.result()[0]["val"] == 1

    pool.close()


@pytest.mark.integration
async def test_psqlpy_broker_uses_provided_read_connection(pg_dsn: str) -> None:
    """Broker created with only read_connection creates its own pool."""
    read_conn = await psqlpy.connect(dsn=pg_dsn)

    broker = PSQLPyBroker(dsn=pg_dsn, read_connection=read_conn)
    await broker.startup()
    await broker.shutdown()

    # Connection must still be alive — run a query on it
    result = await read_conn.fetch("SELECT 1 AS val")
    assert result.result()[0]["val"] == 1
