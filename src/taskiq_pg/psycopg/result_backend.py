from __future__ import annotations

import typing as tp

from psycopg import sql
from psycopg_pool import AsyncConnectionPool
from taskiq import TaskiqResult
from taskiq.compat import model_dump, model_validate

from taskiq_pg._internal.result_backend import BasePostgresResultBackend, ReturnType
from taskiq_pg.exceptions import ResultIsMissingError
from taskiq_pg.psycopg.queries import (
    CREATE_INDEX_QUERY,
    CREATE_TABLE_QUERY,
    DELETE_RESULT_QUERY,
    INSERT_RESULT_QUERY,
    IS_RESULT_EXISTS_QUERY,
    SELECT_RESULT_QUERY,
)


class PsycopgResultBackend(BasePostgresResultBackend):
    """Result backend for TaskIQ based on psycopg."""

    _database_pool: AsyncConnectionPool

    async def startup(self) -> None:
        """
        Initialize the result backend.

        Construct new connection pool
        and create new table for results if not exists.
        """
        self._database_pool = AsyncConnectionPool(
            conninfo=self.dsn if self.dsn is not None else "",
            open=False,
            **self.connect_kwargs,
        )
        await self._database_pool.open()
        async with self._database_pool.connection() as connection, connection.cursor() as cursor:
            await cursor.execute(
                query=sql.SQL(CREATE_TABLE_QUERY).format(
                    sql.Identifier(self.table_name),
                    sql.SQL(self.field_for_task_id),
                ),
            )
            await cursor.execute(
                query=sql.SQL(CREATE_INDEX_QUERY).format(
                    sql.Identifier(self.table_name + "_task_id_idx"),
                    sql.Identifier(self.table_name),
                ),
            )

    async def shutdown(self) -> None:
        """Close the connection pool."""
        if getattr(self, "_database_pool", None) is not None:
            await self._database_pool.close()

    async def set_result(
        self,
        task_id: str,
        result: TaskiqResult[ReturnType],
    ) -> None:
        """
        Set result to the PostgreSQL table.

        :param task_id: ID of the task.
        :param result: result of the task.
        """
        async with self._database_pool.connection() as connection, connection.cursor() as cursor:
            await cursor.execute(
                query=sql.SQL(INSERT_RESULT_QUERY).format(
                    sql.Identifier(self.table_name),
                ),
                params=[
                    task_id,
                    self.serializer.dumpb(model_dump(result)),
                ],
            )

    async def is_result_ready(self, task_id: str) -> bool:
        """
        Returns whether the result is ready.

        :param task_id: ID of the task.

        :returns: True if the result is ready else False.
        """
        async with self._database_pool.connection() as connection, connection.cursor() as cursor:
            execute_result = await cursor.execute(
                query=sql.SQL(IS_RESULT_EXISTS_QUERY).format(
                    sql.Identifier(self.table_name),
                ),
                params=[task_id],
            )
            row = await execute_result.fetchone()
            return bool(row and row[0])

    async def get_result(
        self,
        task_id: str,
        with_logs: bool = False,
    ) -> TaskiqResult[ReturnType]:
        """
        Retrieve result from the task.

        :param task_id: task's id.
        :param with_logs: if True it will download task's logs.
        :raises ResultIsMissingError: if there is no result when trying to get it.
        :return: TaskiqResult.
        """
        async with self._database_pool.connection() as connection, connection.cursor() as cursor:
            execute_result = await cursor.execute(
                query=sql.SQL(SELECT_RESULT_QUERY).format(
                    sql.Identifier(self.table_name),
                ),
                params=[task_id],
            )
            result = await execute_result.fetchone()
            if result is None:
                msg = f"Cannot find record with task_id = {task_id} in PostgreSQL"
                raise ResultIsMissingError(msg)
            result_in_bytes: tp.Final = result[0]

            if not self.keep_results:
                await cursor.execute(
                    query=sql.SQL(DELETE_RESULT_QUERY).format(
                        sql.Identifier(self.table_name),
                    ),
                    params=[task_id],
                )

            taskiq_result: tp.Final = model_validate(
                TaskiqResult[ReturnType],
                self.serializer.loadb(result_in_bytes),
            )

            if not with_logs:
                taskiq_result.log = None

            return taskiq_result
