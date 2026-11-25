import asyncio
import typing as tp
import uuid

import pytest

from taskiq_pg.aiopg import AiopgBroker
from taskiq_pg.asyncpg import AsyncpgBroker
from taskiq_pg.psqlpy import PSQLPyBroker
from taskiq_pg.psycopg import PsycopgBroker


# Small pools, so that eight brokers stay well below max_connections (asyncpg opens 10 connections by default).
SMALL_POOLS: list[tp.Any] = [
    pytest.param(AsyncpgBroker, {"min_size": 1, "max_size": 2}, id="AsyncpgBroker"),
    pytest.param(PsycopgBroker, {"min_size": 1, "max_size": 2}, id="PsycopgBroker"),
    pytest.param(PSQLPyBroker, {"max_db_pool_size": 2}, id="PSQLPyBroker"),
    pytest.param(AiopgBroker, {"minsize": 1, "maxsize": 2}, id="AiopgBroker"),
]


@pytest.mark.integration
@pytest.mark.parametrize(("broker_class", "write_kwargs"), SMALL_POOLS)
async def test_when_many_brokers_start_at_once_on_a_new_table__then_all_of_them_start(
    pg_dsn: str,
    broker_class: type[tp.Any],
    write_kwargs: dict[str, tp.Any],
) -> None:
    """Workers of one deployment start together; concurrent schema setup must not make any of them fail."""
    # given
    names = {"table_name": f"setup_{uuid.uuid4().hex[:12]}", "channel_name": f"setup_{uuid.uuid4().hex[:12]}"}
    brokers = [broker_class(dsn=pg_dsn, write_kwargs=write_kwargs, **names) for _ in range(8)]

    # when
    results = await asyncio.gather(*(broker.startup() for broker in brokers), return_exceptions=True)

    # then
    try:
        assert [r for r in results if isinstance(r, BaseException)] == []
    finally:
        await asyncio.gather(*(broker.shutdown() for broker in brokers), return_exceptions=True)
