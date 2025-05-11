from __future__ import annotations

import typing as tp

from aiopg import Pool, create_pool
from taskiq import AsyncResultBackend, TaskiqResult
from taskiq.serializers import PickleSerializer

from taskiq_pg import exceptions
from taskiq_pg.aiopg import queries


if tp.TYPE_CHECKING:
    from taskiq.abc.serializer import TaskiqSerializer


_ReturnType = tp.TypeVar("_ReturnType")


class AiopgResultBackend(AsyncResultBackend[_ReturnType]):
    """Result backend for TaskIQ based on Aiopg."""

    def __init__(
        self,
        dsn: str | None = "postgres://postgres:postgres@localhost:5432/postgres",
        keep_results: bool = True,
        table_name: str = "taskiq_results",
        field_for_task_id: tp.Literal["VarChar", "Text", "Uuid"] = "Uuid",
        serializer: TaskiqSerializer | None = None,
        **connect_kwargs: tp.Any,
    ) -> None:
        """
        Construct new result backend.

        :param dsn: connection string to PostgreSQL.
        :param keep_results: flag to not remove results from Redis after reading.
        :param connect_kwargs: additional arguments for nats `ConnectionPool` class.
        """
        self.dsn: tp.Final = dsn
        self.keep_results: tp.Final = keep_results
        self.table_name: tp.Final = table_name
        self.field_for_task_id: tp.Final = field_for_task_id
        self.serializer: tp.Final = serializer or PickleSerializer()
        self.connect_kwargs: tp.Final = connect_kwargs

        self._database_pool: Pool

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
        async with self._database_pool.acquire() as connection:
            await connection.close()

    async def set_result(
        self,
        task_id: tp.Any,
        result: TaskiqResult[_ReturnType],
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
    ) -> TaskiqResult[_ReturnType]:
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

            taskiq_result: TaskiqResult[_ReturnType] = self.serializer.loadb(
                result_in_bytes,
            )

            if not with_logs:
                taskiq_result.log = None

            return taskiq_result
