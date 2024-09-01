from typing import Type, Callable, AsyncGenerator, ParamSpec, TypeVar, Any, Coroutine
from typing import Concatenate, overload
from aiomysql import Connection

from .._common import WrapperBase, ParamsAutoEnum
from .._db_api_2 import PrepareFuncResult, req_sql_n_params
from ..registry import MetricsCollector

F_Spec = ParamSpec("F_Spec")
TR = TypeVar("TR")


def sql_fetch_all(row_type: Type[TR], sql: str | None = None):
    """
    Use this decorator to make a "fetchall" SQL statement executor out of
    the function that prepares parameters for the query.

    :param row_type: type of expected result. Usually some dataclass or named tuple
    :param sql: SQL statement to execute. If None, the SQL statement must be provided
    by decorated function.

    IMPORTANT: decorated function must not be async, but after decoration it
    becomes async.

    More info in the noorm.aiomysql docstring.
    """

    def decorator(
        func: Callable[F_Spec, PrepareFuncResult | ParamsAutoEnum | None]
    ) -> Callable[Concatenate[Connection, F_Spec], Coroutine[Any, Any, list[TR]]]:
        class wrapper(WrapperBase):
            async def __call__(
                self, conn: Connection, *args: F_Spec.args, **kwargs: F_Spec.kwargs
            ) -> list[TR]:
                with MetricsCollector(self._func) as mc:
                    if sql_and_params := req_sql_n_params(
                        self._func, args, kwargs, sql
                    ):
                        async with conn.cursor() as cur:
                            await cur.execute(*sql_and_params)
                            col_names = tuple(el[0] for el in cur.description)
                            res: list[TR] = [
                                row_type(**{n: v for n, v in zip(col_names, r)})
                                async for r in cur
                            ]
                            mc.tuples = len(res)
                            return res
                return []

        return wrapper(func)

    return decorator


def sql_iterate(row_type: Type[TR], sql: str | None = None):
    """
    Use this decorator to make a query and iterate through results. Be careful with
    this feature and, if possible, use `sql_fetch_all` instead, because
    `sql_fetch_all` gives you less possibilites to shoot your leg.

    :param row_type: type of expected result. Usually some dataclass or named tuple
    :param sql: SQL statement to execute. If None, the SQL statement must be provided
    by decorated function.

    IMPORTANT: decorated function must not be async, but after decoration it
    becomes async.

    More info in the noorm.aiomysql docstring.
    """

    def decorator(
        func: Callable[F_Spec, PrepareFuncResult | ParamsAutoEnum | None]
    ) -> Callable[Concatenate[Connection, F_Spec], AsyncGenerator[TR, None]]:
        class wrapper(WrapperBase):
            async def __call__(
                self, conn: Connection, *args: F_Spec.args, **kwargs: F_Spec.kwargs
            ) -> AsyncGenerator[TR, None]:
                with MetricsCollector(self._func) as mc:
                    if sql_and_params := req_sql_n_params(
                        self._func, args, kwargs, sql
                    ):
                        async with conn.cursor() as cur:
                            await cur.execute(*sql_and_params)
                            col_names = tuple(el[0] for el in cur.description)
                            is_first_row = True
                            async for r in cur:
                                if is_first_row:
                                    mc.finish(None)
                                    is_first_row = False
                                yield row_type(**{n: v for n, v in zip(col_names, r)})

        return wrapper(func)

    return decorator


def sql_one_or_none(row_type: Type[TR], sql: str | None = None):
    """
    Use this decorator to make a "one or none" SQL statement executor out of
    the function that prepares parameters for the query.

    :param row_type: type of expected result. Usually some dataclass or named tuple
    :param sql: SQL statement to execute. If None, the SQL statement must be provided
    by decorated function.

    IMPORTANT: decorated function must not be async, but after decoration it
    becomes async.

    More info in the noorm.aiomysql docstring.
    """

    def decorator(
        func: Callable[F_Spec, PrepareFuncResult | ParamsAutoEnum | None]
    ) -> Callable[Concatenate[Connection, F_Spec], Coroutine[Any, Any, TR | None]]:
        class wrapper(WrapperBase):
            async def __call__(
                self, conn: Connection, *args: F_Spec.args, **kwargs: F_Spec.kwargs
            ) -> TR | None:
                with MetricsCollector(self._func) as mc:
                    if sql_and_params := req_sql_n_params(
                        self._func, args, kwargs, sql
                    ):
                        async with conn.cursor() as cur:
                            await cur.execute(*sql_and_params)
                            col_names = tuple(el[0] for el in cur.description)
                            row = await cur.fetchone()
                            if row is not None:
                                mc.tuples = 1
                                return row_type(
                                    **{n: v for n, v in zip(col_names, row)}
                                )
                    return None

        return wrapper(func)

    return decorator


def sql_scalar_or_none(res_type: Type[TR], sql: str | None = None):
    """
    Use this decorator to make a "scalar or none" SQL statement executor out of
    the function that prepares parameters for the query.

    :param row_type: type of expected result. For scalar queries it is usually `int`,
    `str`, `bool`, `datetime`, or whatever can be produced by scalar query.
    :param sql: SQL statement to execute. If None, the SQL statement must be provided
    by decorated function.

    IMPORTANT: decorated function must not be async, but after decoration it
    becomes async.

    More info in the noorm.aiomysql docstring.
    """

    def decorator(
        func: Callable[F_Spec, PrepareFuncResult | ParamsAutoEnum | None]
    ) -> Callable[Concatenate[Connection, F_Spec], Coroutine[Any, Any, TR | None]]:
        class wrapper(WrapperBase):
            async def __call__(
                self, conn: Connection, *args: F_Spec.args, **kwargs: F_Spec.kwargs
            ) -> TR | None:
                with MetricsCollector(self._func) as mc:
                    if sql_and_params := req_sql_n_params(
                        self._func, args, kwargs, sql
                    ):
                        async with conn.cursor() as cur:
                            await cur.execute(*sql_and_params)
                            row = await cur.fetchone()
                            if row is not None:
                                mc.tuples = 1
                                return row[0]
                    return None

        return wrapper(func)

    return decorator


def sql_fetch_scalars(res_type: Type[TR], sql: str | None = None):
    """
    Use this decorator to make a "fetch scalars" SQL statement executor out of
    the function that prepares parameters for the query.

    :param res_type: type of expected result. For scalar queries it is usually `int`,
    `str`, `bool`, `datetime`, or whatever can be produced in a single-column query
    result.
    :param sql: SQL statement to execute. If None, the SQL statement must be provided
    by decorated function.

    IMPORTANT: decorated function must not be async, but after decoration it
    becomes async.

    More info in the noorm.aiomysql docstring.
    """

    def decorator(
        func: Callable[F_Spec, PrepareFuncResult | ParamsAutoEnum | None]
    ) -> Callable[Concatenate[Connection, F_Spec], Coroutine[Any, Any, list[TR]]]:
        class wrapper(WrapperBase):
            async def __call__(
                self, conn: Connection, *args: F_Spec.args, **kwargs: F_Spec.kwargs
            ) -> list[TR]:
                with MetricsCollector(self._func) as mc:
                    if sql_and_params := req_sql_n_params(
                        self._func, args, kwargs, sql
                    ):
                        async with conn.cursor() as cur:
                            await cur.execute(*sql_and_params)
                            res = [row[0] async for row in cur]
                            mc.tuples = len(res)
                            return res
                    return []

        return wrapper(func)

    return decorator


def sql_iterate_scalars(res_type: Type[TR], sql: str | None = None):
    """
    Use this decorator to make a query and iterate through scalar results. Be careful
    with this feature and, if possible, use `sql_fetch_scalars` instead, because
    `sql_fetch_scalars` gives you less possibilites to shoot your leg.

    :param res_type: type of expected result. For scalar queries it is usually `int`,
    `str`, `bool`, `datetime`, or whatever can be produced in a single-column query
    result.
    :param sql: SQL statement to execute. If None, the SQL statement must be provided
    by decorated function.

    IMPORTANT: decorated function must not be async, but after decoration it
    becomes async.

    More info in the noorm.aiomysql docstring.
    """

    def decorator(
        func: Callable[F_Spec, PrepareFuncResult | ParamsAutoEnum | None]
    ) -> Callable[Concatenate[Connection, F_Spec], AsyncGenerator[TR, None]]:
        class wrapper(WrapperBase):
            async def __call__(
                self, conn: Connection, *args: F_Spec.args, **kwargs: F_Spec.kwargs
            ) -> AsyncGenerator[TR, None]:
                with MetricsCollector(self._func) as mc:
                    if sql_and_params := req_sql_n_params(
                        self._func, args, kwargs, sql
                    ):
                        async with conn.cursor() as cur:
                            await cur.execute(*sql_and_params)
                            is_first_row = True
                            async for r in cur:
                                if is_first_row:
                                    mc.finish(None)
                                    is_first_row = False
                                yield r[0]

        return wrapper(func)

    return decorator


@overload
def sql_execute(
    func: Callable[F_Spec, PrepareFuncResult | ParamsAutoEnum | None]
) -> Callable[Concatenate[Connection, F_Spec], Coroutine[Any, Any, None]]:
    pass  # pragma: no cover


@overload
def sql_execute(
    sql: str | None = None,
) -> Callable[
    [Callable[F_Spec, Coroutine[Any, Any, None]]],
    Callable[Concatenate[Connection, F_Spec], Coroutine[Any, Any, None]],
]:
    pass  # pragma: no cover


def sql_execute(  # type: ignore
    sql: Callable[F_Spec, PrepareFuncResult | ParamsAutoEnum | None] | str | None = None
):
    """
    Use this decorator to execute a statement without responding a result.

    :param sql: SQL statement to execute. If None, the SQL statement must be provided
    by decorated function.

    IMPORTANT: decorated function must not be async, but after decoration it
    becomes async.

    More info in the noorm.aiomysql docstring.
    """
    if callable(sql):
        the_func = sql
        the_sql = None
    else:
        the_func = None
        the_sql = sql

    def decorator_wrapper(sql: str | None):
        def decorator(
            func: Callable[F_Spec, PrepareFuncResult | ParamsAutoEnum | None]
        ) -> Callable[Concatenate[Connection, F_Spec], Coroutine[Any, Any, None]]:
            class wrapper(WrapperBase):
                async def __call__(
                    self, conn: Connection, *args: F_Spec.args, **kwargs: F_Spec.kwargs
                ) -> None:
                    with MetricsCollector(self._func):
                        if sql_and_params := req_sql_n_params(
                            self._func, args, kwargs, sql
                        ):
                            async with conn.cursor() as cur:
                                await cur.execute(*sql_and_params)

            return wrapper(func)

        return decorator

    wrap_decorator = decorator_wrapper(the_sql)
    if callable(sql):
        return wrap_decorator(the_func)
    else:
        return wrap_decorator
