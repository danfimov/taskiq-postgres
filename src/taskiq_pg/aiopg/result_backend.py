from __future__ import annotations

import typing as tp

from aiopg import Pool, create_pool

from taskiq_pg import exceptions
from taskiq_pg._internal.result_backend import BasePostgresResultBackend, ReturnType
from taskiq_pg.aiopg import queries


if tp.TYPE_CHECKING:
    from taskiq import TaskiqResult


class AiopgResultBackend(BasePostgresResultBackend):
    """Result backend for TaskIQ based on Aiopg."""

    _database_pool: Pool

    async def startup(self) -> None:
        """
        Initialize the result backend.

        Construct new connection pool
        and create new table for results if not exists.
        """
        try:
            self._database_pool = await create_pool(
                self.dsn,
                **self.connect_kwargs,
            )

            async with self._database_pool.acquire() as connection, connection.cursor() as cursor:
                await cursor.execute(
                    queries.CREATE_TABLE_QUERY.format(
                        self.table_name,
                        self.field_for_task_id,
                    ),
                )
                await cursor.execute(
                    queries.CREATE_INDEX_QUERY.format(
                        self.table_name,
                        self.table_name,
                    ),
                )
        except Exception as error:
            raise exceptions.DatabaseConnectionError(str(error)) from error

    async def shutdown(self) -> None:
        """Close the connection pool."""
        if getattr(self, "_database_pool", None) is not None:
            self._database_pool.close()

    async def set_result(
        self,
        task_id: tp.Any,
        result: TaskiqResult[ReturnType],
    ) -> None:
        """
        Set result to the PostgreSQL table.

        Args:
            task_id (Any): ID of the task.
            result (TaskiqResult[_ReturnType]):  result of the task.

        """
        dumped_result = self.serializer.dumpb(result)
        async with self._database_pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                queries.INSERT_RESULT_QUERY.format(
                    self.table_name,
                ),
                (
                    task_id,
                    dumped_result,
                    dumped_result,
                ),
            )

    async def is_result_ready(
        self,
        task_id: tp.Any,
    ) -> bool:
        """
        Return whether the result is ready.

        Args:
            task_id (Any): ID of the task.

        Returns:
            bool: True if the result is ready else False.

        """
        async with self._database_pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                queries.IS_RESULT_EXISTS_QUERY.format(
                    self.table_name,
                ),
                (task_id,),
            )
            result = await cursor.fetchone()
            return bool(result[0]) if result else False

    async def get_result(
        self,
        task_id: tp.Any,
        with_logs: bool = False,
    ) -> TaskiqResult[ReturnType]:
        """
        Retrieve result from the task.

        :param task_id: task's id.
        :param with_logs: if True it will download task's logs.
        :raises ResultIsMissingError: if there is no result when trying to get it.
        :return: TaskiqResult.
        """
        async with self._database_pool.acquire() as connection, connection.cursor() as cursor:
            await cursor.execute(
                queries.SELECT_RESULT_QUERY.format(
                    self.table_name,
                ),
                (task_id,),
            )
            result = await cursor.fetchone()

            if not result:
                msg = f"Cannot find record with task_id = {task_id} in PostgreSQL"
                raise exceptions.ResultIsMissingError(
                    msg,
                )

            result_in_bytes: bytes = result[0]

            if not self.keep_results:
                await cursor.execute(
                    queries.DELETE_RESULT_QUERY.format(
                        self.table_name,
                    ),
                    (task_id,),
                )

            taskiq_result: TaskiqResult[ReturnType] = self.serializer.loadb(
                result_in_bytes,
            )

            if not with_logs:
                taskiq_result.log = None

            return taskiq_result
