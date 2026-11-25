import asyncio
import typing as tp
import uuid
from urllib.parse import urlsplit, urlunsplit

import asyncpg
import pytest
from taskiq import BrokerMessage

from taskiq_pg.aiopg import AiopgBroker
from taskiq_pg.asyncpg import AsyncpgBroker
from taskiq_pg.asyncpg.queries import (
    ADD_DELIVERY_COLUMNS_QUERY,
    CREATE_MESSAGE_TABLE_QUERY,
    CREATE_PENDING_INDEX_QUERY,
    CREATE_PROCESSING_INDEX_QUERY,
)
from taskiq_pg.psqlpy import PSQLPyBroker
from taskiq_pg.psycopg import PsycopgBroker


@pytest.fixture
async def dml_only_dsn(pg_dsn: str, connection: asyncpg.Connection) -> tp.AsyncGenerator[tuple[str, str], None]:
    """Prepare a messages table as its owner and return a DSN for a role that may only read and write it."""
    role = f"dml_only_{uuid.uuid4().hex[:8]}"
    table = f"prepared_{uuid.uuid4().hex[:8]}"
    await connection.execute(CREATE_MESSAGE_TABLE_QUERY.format(table))
    await connection.execute(ADD_DELIVERY_COLUMNS_QUERY.format(table=table))
    await connection.execute(CREATE_PENDING_INDEX_QUERY.format(index=f"{table}_pending_idx", table=table))
    await connection.execute(CREATE_PROCESSING_INDEX_QUERY.format(index=f"{table}_processing_idx", table=table))
    await connection.execute(f"CREATE ROLE {role} LOGIN PASSWORD 'dml_only'")
    await connection.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON {table} TO {role}")
    await connection.execute(f"GRANT USAGE ON SEQUENCE {table}_id_seq TO {role}")
    await connection.execute(f"REVOKE CREATE ON SCHEMA public FROM {role}")
    parts = urlsplit(pg_dsn)
    dsn = urlunsplit(parts._replace(netloc=f"{role}:dml_only@{parts.hostname}:{parts.port}"))
    try:
        yield dsn, table
    finally:
        await connection.execute(f"DROP TABLE {table}")
        await connection.execute(f"DROP OWNED BY {role}")
        await connection.execute(f"DROP ROLE {role}")


@pytest.mark.integration
@pytest.mark.parametrize("broker_class", [AsyncpgBroker, PsycopgBroker, PSQLPyBroker, AiopgBroker])
async def test_when_table_is_prepared_in_advance__then_role_without_ddl_privileges_can_use_it(
    dml_only_dsn: tuple[str, str],
    broker_class: type[tp.Any],
) -> None:
    # given
    dsn, table = dml_only_dsn
    broker = broker_class(dsn=dsn, table_name=table, channel_name=table, processing_timeout=60)

    # when
    await broker.startup()
    listener = broker.listen()
    try:
        message = BrokerMessage(task_id=uuid.uuid4().hex, task_name="example:dml", message=b'{"ok":true}', labels={})
        await broker.kick(message)
        received = await asyncio.wait_for(anext(listener), timeout=5)

        # then
        assert received.data == message.message
    finally:
        await listener.aclose()
        await broker.shutdown()
