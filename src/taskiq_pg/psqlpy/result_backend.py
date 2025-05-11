from __future__ import annotations

import typing as tp

from psqlpy import ConnectionPool  # type: ignore[import-not-found]
from psqlpy.exceptions import RustPSQLDriverPyBaseError  # type: ignore[import-not-found]
from taskiq import AsyncResultBackend, TaskiqResult
from taskiq.compat import model_dump, model_validate
from taskiq.serializers import PickleSerializer

from taskiq_pg.exceptions import ResultIsMissingError
from taskiq_pg.psqlpy.queries import (
    CREATE_INDEX_QUERY,
    CREATE_TABLE_QUERY,
    DELETE_RESULT_QUERY,
    INSERT_RESULT_QUERY,
    IS_RESULT_EXISTS_QUERY,
    SELECT_RESULT_QUERY,
)


if tp.TYPE_CHECKING:
    from taskiq.abc.serializer import TaskiqSerializer


_ReturnType = tp.TypeVar("_ReturnType")


class PSQLPyResultBackend(AsyncResultBackend[_ReturnType]):
    """Result backend for TaskIQ based on PSQLPy."""

    def __init__(
        self,
        dsn: str | None = "postgres://postgres:postgres@localhost:5432/postgres",
        keep_results: bool = True,
        table_name: str = "taskiq_results",
        field_for_task_id: tp.Literal["VarChar", "Text"] = "VarChar",
        serializer: TaskiqSerializer | None = None,
        **connect_kwargs: tp.Any,
    ) -> None:
        """
        Construct new result backend.

        :param dsn: connection string to PostgreSQL.
        :param keep_results: flag to not remove results from Redis after reading.
        :param table_name: name of the table to store results.
        :param field_for_task_id: type of the field to store task_id.
        :param serializer: serializer class to serialize/deserialize result from task.
        :param connect_kwargs: additional arguments for nats `ConnectionPool` class.
        """
        self.dsn: tp.Final = dsn
        self.keep_results: tp.Final = keep_results
        self.table_name: tp.Final = table_name
        self.field_for_task_id: tp.Final = field_for_task_id
        self.connect_kwargs: tp.Final = connect_kwargs
        self.serializer = serializer or PickleSerializer()

        self._database_pool: ConnectionPool

    async def startup(self) -> None:
        """
        Initialize the result backend.

        Construct new connection pool
        and create new table for results if not exists.
        """
        self._database_pool = ConnectionPool(
            dsn=self.dsn,
            **self.connect_kwargs,
        )
        connection = await self._database_pool.connection()
        await connection.execute(
            querystring=CREATE_TABLE_QUERY.format(
                self.table_name,
                self.field_for_task_id,
            ),
        )
        await connection.execute(
            querystring=CREATE_INDEX_QUERY.format(
                self.table_name,
                self.table_name,
            ),
        )

    async def shutdown(self) -> None:
        """Close the connection pool."""
        self._database_pool.close()

    async def set_result(
        self,
        task_id: str,
        result: TaskiqResult[_ReturnType],
    ) -> None:
        """
        Set result to the PostgreSQL table.

        :param task_id: ID of the task.
        :param result: result of the task.
        """
        connection = await self._database_pool.connection()
        await connection.execute(
            querystring=INSERT_RESULT_QUERY.format(
                self.table_name,
            ),
            parameters=[
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
        connection: tp.Final = await self._database_pool.connection()
        return tp.cast(
            "bool",
            await connection.fetch_val(
                querystring=IS_RESULT_EXISTS_QUERY.format(
                    self.table_name,
                ),
                parameters=[task_id],
            ),
        )

    async def get_result(
        self,
        task_id: str,
        with_logs: bool = False,
    ) -> TaskiqResult[_ReturnType]:
        """
        Retrieve result from the task.

        :param task_id: task's id.
        :param with_logs: if True it will download task's logs.
        :raises ResultIsMissingError: if there is no result when trying to get it.
        :return: TaskiqResult.
        """
        connection: tp.Final = await self._database_pool.connection()
        try:
            result_in_bytes: tp.Final[bytes] = await connection.fetch_val(
                querystring=SELECT_RESULT_QUERY.format(
                    self.table_name,
                ),
                parameters=[task_id],
            )
        except RustPSQLDriverPyBaseError as exc:
            msg = f"Cannot find record with task_id = {task_id} in PostgreSQL"
            raise ResultIsMissingError(msg) from exc

        if not self.keep_results:
            await connection.execute(
                querystring=DELETE_RESULT_QUERY.format(
                    self.table_name,
                ),
                parameters=[task_id],
            )

        taskiq_result: tp.Final = model_validate(
            TaskiqResult[_ReturnType],
            self.serializer.loadb(result_in_bytes),
        )

        if not with_logs:
            taskiq_result.log = None

        return taskiq_result
