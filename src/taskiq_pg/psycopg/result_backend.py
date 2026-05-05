import typing as tp

from psycopg import sql
from psycopg_pool import AsyncConnectionPool
from taskiq import TaskiqResult
from taskiq.abc.serializer import TaskiqSerializer
from taskiq.compat import model_dump, model_validate
from taskiq.depends.progress_tracker import TaskProgress

from taskiq_pg._internal.result_backend import BasePostgresResultBackend, ReturnType
from taskiq_pg.exceptions import ResultIsMissingError
from taskiq_pg.psycopg import queries


class PsycopgResultBackend(BasePostgresResultBackend):
    """Result backend for TaskIQ based on psycopg."""

    _database_pool: AsyncConnectionPool
    _owns_pool: bool

    @tp.overload
    def __init__(
        self,
        dsn: tp.Callable[[], str] | str | None = ...,
        keep_results: bool = ...,
        table_name: str = ...,
        field_for_task_id: tp.Literal["VarChar", "Text", "Uuid"] = ...,
        serializer: TaskiqSerializer | None = ...,
        *,
        pool: None = ...,
        **connect_kwargs: tp.Any,
    ) -> None: ...

    @tp.overload
    def __init__(
        self,
        dsn: tp.Callable[[], str] | str | None = ...,
        keep_results: bool = ...,
        table_name: str = ...,
        field_for_task_id: tp.Literal["VarChar", "Text", "Uuid"] = ...,
        serializer: TaskiqSerializer | None = ...,
        *,
        pool: AsyncConnectionPool,
    ) -> None: ...

    def __init__(
        self,
        dsn: tp.Callable[[], str] | str | None = "postgres://postgres:postgres@localhost:5432/postgres",
        keep_results: bool = True,
        table_name: str = "taskiq_results",
        field_for_task_id: tp.Literal["VarChar", "Text", "Uuid"] = "VarChar",
        serializer: TaskiqSerializer | None = None,
        *,
        pool: AsyncConnectionPool | None = None,
        **connect_kwargs: tp.Any,
    ) -> None:
        """
        Construct a new PsycopgResultBackend.

        Args:
            dsn: PostgreSQL connection string or callable. Must be ``None`` in pool mode.
            keep_results: Whether to keep results after reading.
            table_name: Table to store results in.
            field_for_task_id: Column type for task_id.
            serializer: Serializer for task results.
            pool: An existing connection pool to reuse.
            **connect_kwargs: Extra kwargs for connection pool creation.
        """
        self._owns_pool = True
        if pool is not None:
            self._owns_pool = False
            self._database_pool = pool

        super().__init__(
            dsn=dsn,
            keep_results=keep_results,
            table_name=table_name,
            field_for_task_id=field_for_task_id,
            serializer=serializer,
            **connect_kwargs,
        )

    async def startup(self) -> None:
        """
        Initialize the result backend.

        Construct new connection pool (if not provided externally) and create new table
        for results if not exists.
        """
        if self._owns_pool:
            self._database_pool = AsyncConnectionPool(
                conninfo=self.dsn if self.dsn is not None else "",
                open=False,
                **self.connect_kwargs,
            )
            await self._database_pool.open()

        async with self._database_pool.connection() as connection, connection.cursor() as cursor:
            await cursor.execute(
                query=sql.SQL(queries.CREATE_TABLE_QUERY).format(
                    sql.Identifier(self.table_name),
                    sql.SQL(self.field_for_task_id),
                ),
            )
            await cursor.execute(
                query=sql.SQL(queries.ADD_PROGRESS_COLUMN_QUERY).format(
                    sql.Identifier(self.table_name),
                ),
            )
            await cursor.execute(
                query=sql.SQL(queries.CREATE_INDEX_QUERY).format(
                    sql.Identifier(self.table_name + "_task_id_idx"),
                    sql.Identifier(self.table_name),
                ),
            )

    async def shutdown(self) -> None:
        """Close the connection pool (only if owned by this backend)."""
        if self._owns_pool and getattr(self, "_database_pool", None) is not None:
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
                query=sql.SQL(queries.INSERT_RESULT_QUERY).format(
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
                query=sql.SQL(queries.IS_RESULT_EXISTS_QUERY).format(
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
                query=sql.SQL(queries.SELECT_RESULT_QUERY).format(
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
                    query=sql.SQL(queries.DELETE_RESULT_QUERY).format(
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
        async with self._database_pool.connection() as connection, connection.cursor() as cursor:
            await cursor.execute(
                query=sql.SQL(queries.INSERT_PROGRESS_QUERY).format(
                    sql.Identifier(self.table_name),
                ),
                params=[
                    task_id,
                    self.serializer.dumpb(model_dump(progress)),
                    self.serializer.dumpb(model_dump(progress)),
                ],
            )

    async def get_progress(
        self,
        task_id: str,
    ) -> TaskProgress[tp.Any] | None:
        """
        Gets progress.

        :param task_id: task's id.
        """
        async with self._database_pool.connection() as connection, connection.cursor() as cursor:
            execute_result = await cursor.execute(
                query=sql.SQL(queries.SELECT_PROGRESS_QUERY).format(
                    sql.Identifier(self.table_name),
                ),
                params=[task_id],
            )
            progress_in_bytes = await execute_result.fetchone()
            if progress_in_bytes is None or progress_in_bytes[0] is None:
                return None
            return model_validate(
                TaskProgress[tp.Any],
                self.serializer.loadb(progress_in_bytes[0]),
            )
