import typing as tp
import uuid

import asyncpg
import pytest
from taskiq import (
    InMemoryBroker,
    TaskiqDepends,
)
from taskiq.depends.progress_tracker import ProgressTracker, TaskState

from taskiq_pg._internal.result_backend import BasePostgresResultBackend
from taskiq_pg.aiopg import AiopgResultBackend
from taskiq_pg.asyncpg import AsyncpgResultBackend
from taskiq_pg.psqlpy import PSQLPyResultBackend
from taskiq_pg.psycopg import PsycopgResultBackend


@pytest.mark.integration
@pytest.mark.parametrize(
    "result_backend_class",
    [
        AsyncpgResultBackend,
        AiopgResultBackend,
        PSQLPyResultBackend,
        PsycopgResultBackend,
    ],
)
@pytest.mark.parametrize(
    ("field_type", "expected_pg_type"),
    [
        ("VarChar", "character varying"),
        ("Text", "text"),
        ("Uuid", "uuid"),
    ],
)
async def test_when_startup_called__then_table_is_created(
    pg_dsn: str,
    result_backend_class: type[AsyncpgResultBackend | AiopgResultBackend | PSQLPyResultBackend | PsycopgResultBackend],
    field_type: tp.Literal["VarChar", "Text", "Uuid"],
    expected_pg_type: str,
) -> None:
    # Given: уникальное имя таблицы, backend и подключение к БД
    table_name: str = f"taskiq_results_{uuid.uuid4().hex}"
    backend = result_backend_class(
        dsn=pg_dsn,
        table_name=table_name,
        field_for_task_id=field_type,
    )
    conn = await asyncpg.connect(dsn=pg_dsn)

    try:
        # When: запускаем backend.startup()
        await backend.startup()

        # Then: таблица существует
        table_exists: bool = tp.cast(
            "bool",
            await conn.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                      AND table_name = $1
                )
                """,
                table_name,
            ),
        )
        assert table_exists is True, "Ожидалась созданная таблица в public schema"

        # And: колонки имеют ожидаемые типы
        rows = await conn.fetch(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = $1
            ORDER BY ordinal_position
            """,
            table_name,
        )
        columns: dict[str, str] = {tp.cast("str", r["column_name"]): tp.cast("str", r["data_type"]) for r in rows}
        assert columns["task_id"] == expected_pg_type
        assert columns["result"] == "bytea"

        # And: на task_id есть UNIQUE констрейнт
        unique_on_task_id: bool = tp.cast(
            "bool",
            await conn.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                      ON tc.constraint_name = kcu.constraint_name
                     AND tc.table_schema = kcu.table_schema
                     AND tc.table_name = kcu.table_name
                    WHERE tc.table_schema = 'public'
                      AND tc.table_name = $1
                      AND tc.constraint_type = 'UNIQUE'
                      AND kcu.column_name = 'task_id'
                )
                """,
                table_name,
            ),
        )
        assert unique_on_task_id is True, "Ожидался UNIQUE констрейнт на колонке task_id"

        # And: создан индекс {table}_task_id_idx
        index_name: str = f"{table_name}_task_id_idx"
        index_exists: bool = tp.cast(
            "bool",
            await conn.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_indexes
                    WHERE schemaname = 'public'
                      AND tablename = $1
                      AND indexname = $2
                )
                """,
                table_name,
                index_name,
            ),
        )
        assert index_exists is True, "Ожидалось наличие индекса на task_id"
    finally:
        # Cleanup: закрываем backend и дропаем таблицу
        await backend.shutdown()
        try:
            await conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        finally:
            await conn.close()


@pytest.mark.parametrize(
    "broker_with_backend",
    [
        AsyncpgResultBackend,
        AiopgResultBackend,
        PSQLPyResultBackend,
        PsycopgResultBackend,
    ],
    indirect=True,
)
@pytest.mark.parametrize(
    ("state", "meta"),
    [
        (TaskState.STARTED, "hello world!"),
        ("retry", "retry error!"),
        ("custom state", {"Complex": "Value"}),
    ],
)
async def test_when_task_tracker_set_progress__then_it_is_stored(
    broker_with_backend: tuple[InMemoryBroker, BasePostgresResultBackend],
    state: TaskState | str,
    meta: tp.Any,
) -> None:
    # given
    broker, _result_backend = broker_with_backend

    @broker.task
    async def test_func(tes_val: ProgressTracker[tp.Any] = TaskiqDepends()) -> None:  # noqa: B008
        await tes_val.set_progress(state, meta)

    # when
    kicker = await test_func.kiq()
    result = await kicker.wait_result()

    # then
    assert not result.is_err
    progress = await broker.result_backend.get_progress(kicker.task_id)
    assert progress is not None
    assert progress.meta == meta
    assert progress.state == state


@pytest.mark.parametrize(
    "broker_with_backend",
    [
        AsyncpgResultBackend,
        AiopgResultBackend,
        PSQLPyResultBackend,
        PsycopgResultBackend,
    ],
    indirect=True,
)
async def test_when_task_progress_is_not_set__get_progress_should_return_none(
    broker_with_backend: tuple[InMemoryBroker, BasePostgresResultBackend],
) -> None:
    # given
    broker, _result_backend = broker_with_backend

    @broker.task
    async def test_func() -> None:
        pass

    # when
    kicker = await test_func.kiq()
    result = await kicker.wait_result()

    # then
    assert not result.is_err
    progress = await broker.result_backend.get_progress(kicker.task_id)
    assert progress is None
