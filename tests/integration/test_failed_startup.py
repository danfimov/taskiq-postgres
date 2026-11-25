import typing as tp
import uuid

import pytest

from taskiq_pg.aiopg import AiopgBroker
from taskiq_pg.asyncpg import AsyncpgBroker
from taskiq_pg.psqlpy import PSQLPyBroker
from taskiq_pg.psycopg import PsycopgBroker


@pytest.mark.integration
@pytest.mark.parametrize(
    ("broker_class", "listener_setup"),
    [
        (AsyncpgBroker, "_connect_listener"),
        (PsycopgBroker, "_connect_listener"),
        (PSQLPyBroker, "_start_listener"),
        (AiopgBroker, "_connect_listener"),
    ],
)
async def test_when_startup_failed__then_shutdown_cleans_up_without_errors(
    pg_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    broker_class: type[tp.Any],
    listener_setup: str,
) -> None:
    # given: startup fails after the pool is created but before LISTEN is set up
    broker = broker_class(dsn=pg_dsn, table_name=f"failed_{uuid.uuid4().hex[:12]}")

    async def unavailable(*_args: object) -> tp.NoReturn:
        msg = "database is unavailable"
        raise OSError(msg)

    monkeypatch.setattr(broker, listener_setup, unavailable)
    with pytest.raises(OSError, match="unavailable"):
        await broker.startup()

    # when / then
    await broker.shutdown()
