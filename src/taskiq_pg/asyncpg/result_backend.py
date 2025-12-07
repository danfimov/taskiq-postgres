import typing as tp

import asyncpg
from taskiq import TaskiqResult
from taskiq.compat import model_dump, model_validate
from taskiq.depends.progress_tracker import TaskProgress

from taskiq_pg._internal.result_backend import BasePostgresResultBackend, ReturnType
from taskiq_pg.asyncpg import queries


class AsyncpgResultBackend(BasePostgresResultBackend):
    """Result backend for TaskIQ based on asyncpg."""

    _database_pool: "asyncpg.Pool[tp.Any]"

    async def startup(self) -> None:
        """
        Initialize the result backend.

        Construct new connection pool and create new table for results if not exists.
        """
        _database_pool = await asyncpg.create_pool(
            dsn=self.dsn,
            **self.connect_kwargs,
        )
        self._database_pool = _database_pool

        await self._database_pool.execute(
            queries.CREATE_TABLE_QUERY.format(
                self.table_name,
                self.field_for_task_id,
            ),
        )
        await self._database_pool.execute(
            queries.ADD_PROGRESS_COLUMN_QUERY.format(
                self.table_name,
            ),
        )
        await self._database_pool.execute(
            queries.CREATE_INDEX_QUERY.format(
                self.table_name,
                self.table_name,
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
        _ = await self._database_pool.execute(
            queries.INSERT_RESULT_QUERY.format(
                self.table_name,
            ),
            task_id,
            self.serializer.dumpb(model_dump(result)),
        )

    async def is_result_ready(self, task_id: str) -> bool:
        """
        Returns whether the result is ready.

        :param task_id: ID of the task.
        :returns: True if the result is ready else False.
        """
        return tp.cast(
            "bool",
            await self._database_pool.fetchval(
                queries.IS_RESULT_EXISTS_QUERY.format(
                    self.table_name,
                ),
                task_id,
            ),
        )

    async def get_result(
        self,
        task_id: str,
        with_logs: bool = False,
    ) -> TaskiqResult[ReturnType]:
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
                queries.SELECT_RESULT_QUERY.format(
                    self.table_name,
                ),
                task_id,
            ),
        )
        if not self.keep_results:
            await self._database_pool.execute(
                queries.DELETE_RESULT_QUERY.format(
                    self.table_name,
                ),
                task_id,
            )
        taskiq_result: tp.Final = model_validate(
            TaskiqResult[ReturnType],
            self.serializer.loadb(result_in_bytes),
        )
        if not with_logs:
            taskiq_result.log = None
        return taskiq_result

    async def set_progress(
        self,
        task_id: str,
        progress: TaskProgress[tp.Any],
    ) -> None:
        """
        Saves progress.

        :param task_id: task's id.
        :param progress: progress of execution.
        """
        await self._database_pool.execute(
            queries.INSERT_PROGRESS_QUERY.format(
                self.table_name,
            ),
            task_id,
            self.serializer.dumpb(model_dump(progress)),
        )

    async def get_progress(
        self,
        task_id: str,
    ) -> TaskProgress[tp.Any] | None:
        """
        Gets progress.

        :param task_id: task's id.
        """
        progress_in_bytes = await self._database_pool.fetchval(
            queries.SELECT_PROGRESS_QUERY.format(
                self.table_name,
            ),
            task_id,
        )
        if progress_in_bytes is None:
            return None
        return model_validate(
            TaskProgress[tp.Any],
            self.serializer.loadb(progress_in_bytes),
        )
