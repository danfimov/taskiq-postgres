from __future__ import annotations

import typing as tp

import asyncpg
from taskiq import AsyncResultBackend, TaskiqResult
from taskiq.compat import model_dump, model_validate
from taskiq.serializers import PickleSerializer
from typing_extensions import override

from taskiq_pg.asyncpg.queries import (
    CREATE_INDEX_QUERY,
    CREATE_TABLE_QUERY,
    DELETE_RESULT_QUERY,
    INSERT_RESULT_QUERY,
    IS_RESULT_EXISTS_QUERY,
    SELECT_RESULT_QUERY,
)
from taskiq_pg.exceptions import ResultIsMissingError


if tp.TYPE_CHECKING:
    from taskiq.abc.serializer import TaskiqSerializer


_ReturnType = tp.TypeVar("_ReturnType")


class AsyncpgResultBackend(AsyncResultBackend[_ReturnType]):
    """Result backend for TaskIQ based on asyncpg."""

    def __init__(
        self,
        dsn: tp.Callable[[], str] | str | None = "postgres://postgres:postgres@localhost:5432/postgres",
        keep_results: bool = True,
        table_name: str = "taskiq_results",
        field_for_task_id: tp.Literal["VarChar", "Text"] = "VarChar",
        serializer: TaskiqSerializer | None = None,
        **connect_kwargs: tp.Any,
    ) -> None:
        """
        Construct new result backend.

        :param dsn: connection string to PostgreSQL, or callable returning one.
        :param keep_results: flag to not remove results from the database after reading.
        :param table_name: name of the table to store results.
        :param field_for_task_id: type of the field to store task_id.
        :param serializer: serializer class to serialize/deserialize result from task.
        :param connect_kwargs: additional arguments for asyncpg `create_pool` function.
        """
        self._dsn: tp.Final = dsn
        self.keep_results: tp.Final = keep_results
        self.table_name: tp.Final = table_name
        self.field_for_task_id: tp.Final = field_for_task_id
        self.connect_kwargs: tp.Final = connect_kwargs
        self.serializer = serializer or PickleSerializer()
        self._database_pool: asyncpg.Pool[tp.Any]

    @property
    def dsn(self) -> str | None:
        """
        Get the DSN string.

        Returns the DSN string or None if not set.
        """
        if callable(self._dsn):
            return self._dsn()
        return self._dsn

    @override
    async def startup(self) -> None:
        """
        Initialize the result backend.

        Construct new connection pool and create new table for results if not exists.
        """
        _database_pool = await asyncpg.create_pool(
            dsn=self.dsn,
            **self.connect_kwargs,
        )
        if _database_pool is None:
            msg = "Database pool not initialized"
            raise RuntimeError(msg)
        self._database_pool = _database_pool

        _ = await self._database_pool.execute(
            CREATE_TABLE_QUERY.format(
                self.table_name,
                self.field_for_task_id,
            ),
        )
        _ = await self._database_pool.execute(
            CREATE_INDEX_QUERY.format(
                self.table_name,
                self.table_name,
            ),
        )

    @override
    async def shutdown(self) -> None:
        """Close the connection pool."""
        await self._database_pool.close()

    @override
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
        _ = await self._database_pool.execute(
            INSERT_RESULT_QUERY.format(
                self.table_name,
            ),
            task_id,
            self.serializer.dumpb(model_dump(result)),
        )

    @override
    async def is_result_ready(self, task_id: str) -> bool:
        """
        Returns whether the result is ready.

        :param task_id: ID of the task.
        :returns: True if the result is ready else False.
        """
        return tp.cast(
            "bool",
            await self._database_pool.fetchval(
                IS_RESULT_EXISTS_QUERY.format(
                    self.table_name,
                ),
                task_id,
            ),
        )

    @override
    async def get_result(
        self,
        task_id: str,
        with_logs: bool = False,
    ) -> TaskiqResult[_ReturnType]:
        """
        Retrieve result from the task.

        :param task_id: task's id.
        :param with_logs: if True it will download task's logs. (deprecated in taskiq)
        :raises ResultIsMissingError: if there is no result when trying to get it.
        :return: TaskiqResult.
        """
        result_in_bytes = tp.cast(
            "bytes",
            await self._database_pool.fetchval(
                SELECT_RESULT_QUERY.format(
                    self.table_name,
                ),
                task_id,
            ),
        )
        if result_in_bytes is None:
            msg = f"Cannot find record with task_id = {task_id} in PostgreSQL"
            raise ResultIsMissingError(msg)
        if not self.keep_results:
            _ = await self._database_pool.execute(
                DELETE_RESULT_QUERY.format(
                    self.table_name,
                ),
                task_id,
            )
        taskiq_result: tp.Final = model_validate(
            TaskiqResult[_ReturnType],
            self.serializer.loadb(result_in_bytes),
        )
        if not with_logs:
            taskiq_result.log = None
        return taskiq_result
