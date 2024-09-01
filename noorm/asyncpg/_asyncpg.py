from typing import Type, Callable, Coroutine, ParamSpec, TypeVar, Any, Concatenate
from typing import overload

import asyncpg

from .._common import WrapperBase, ParamsAutoEnum
from .._db_api_2_args_only import PrepareFuncResult, req_sql_n_params
from ..registry import MetricsCollector

F_Spec = ParamSpec("F_Spec")
TR = TypeVar("TR")


def sql_fetch_all(row_type: Type[TR], sql: str | None = None):
    """
    Use this decorator to make a "fetch" SQL statement executor out of
    the function that prepares parameters for the query.

    :param row_type: type of expected result. Usually some dataclass or named tuple
    :param sql: SQL statement to execute. If None, the SQL statement must be provided
    by decorated function.

    IMPORTANT: decorated function must not be async, but after decoration it
    becomes async.

    More info in the noorm.asyncpg docstring.
    """

    def decorator(
        func: Callable[F_Spec, PrepareFuncResult | ParamsAutoEnum | None]
    ) -> Callable[
        Concatenate[asyncpg.Connection, F_Spec], Coroutine[Any, Any, list[TR]]
    ]:
        class wrapper(WrapperBase):
            async def __call__(
                self,
                conn: asyncpg.Connection,
                *args: F_Spec.args,
                **kwargs: F_Spec.kwargs,
            ) -> list[TR]:
                with MetricsCollector(self._func) as mc:
                    if sql_and_params := req_sql_n_params(
                        self._func, args, kwargs, sql
                    ):
                        q_res = await conn.fetch(sql_and_params[0], *sql_and_params[1])
                        res: list[TR] = [
                            row_type(**{n: v for n, v in r.items()}) for r in q_res
                        ]
                        mc.tuples = len(res)
                        return res
                    return []

        return wrapper(func)

    return decorator


def sql_one_or_none(row_type: Type[TR], sql: str | None = None):
    """
    Use this decorator to make an "fetchrow" SQL statement executor out of
    the function that prepares parameters for the query.

    :param row_type: type of expected result. Usually some dataclass or named tuple
    :param sql: SQL statement to execute. If None, the SQL statement must be provided
    by decorated function.

    IMPORTANT: decorated function must not be async, but after decoration it
    becomes async.

    More info in the noorm.asyncpg docstring.
    """

    def decorator(
        func: Callable[F_Spec, PrepareFuncResult | ParamsAutoEnum | None],
    ) -> Callable[
        Concatenate[asyncpg.Connection, F_Spec], Coroutine[Any, Any, TR | None]
    ]:
        class wrapper(WrapperBase):
            async def __call__(
                self,
                conn: asyncpg.Connection,
                *args: F_Spec.args,
                **kwargs: F_Spec.kwargs,
            ) -> TR | None:
                with MetricsCollector(self._func) as mc:
                    if sql_and_params := req_sql_n_params(
                        self._func, args, kwargs, sql
                    ):
                        q_res = await conn.fetchrow(
                            sql_and_params[0], *sql_and_params[1]
                        )
                    else:
                        return None
                    if q_res is None:
                        return None
                    mc.tuples = 1
                    return row_type(**{n: v for n, v in q_res.items()})

        return wrapper(func)

    return decorator


def sql_scalar_or_none(res_type: Type[TR], sql: str | None = None):
    """
    Use this decorator to make a "scalar" SQL statement executor out of
    the function that prepares parameters for the query.

    :param res_type: type of expected result. For scalar queries it is usually `int`,
    `str`, `bool`, `datetime`, or whatever can be produced by scalar query.
    :param sql: SQL statement to execute. If None, the SQL statement must be provided
    by decorated function.

    IMPORTANT: decorated function must not be async, but after decoration it
    becomes async.

    More info in the noorm.asyncpg docstring.
    """

    def decorator(
        func: Callable[F_Spec, PrepareFuncResult | ParamsAutoEnum | None],
    ) -> Callable[
        Concatenate[asyncpg.Connection, F_Spec], Coroutine[Any, Any, TR | None]
    ]:
        class wrapper(WrapperBase):
            async def __call__(
                self,
                conn: asyncpg.Connection,
                *args: F_Spec.args,
                **kwargs: F_Spec.kwargs,
            ) -> TR | None:
                with MetricsCollector(self._func) as mc:
                    if sql_and_params := req_sql_n_params(
                        self._func, args, kwargs, sql
                    ):
                        q_res = await conn.fetchval(
                            sql_and_params[0], *sql_and_params[1]
                        )
                        if q_res is not None:
                            mc.tuples = 1
                    else:
                        return None
                    return q_res

        return wrapper(func)

    return decorator


def sql_fetch_scalars(res_type: Type[TR], sql: str | None = None):
    """
    Use this decorator to make a "fetch scalars" SQL statement executor out of
    the function that prepares parameters for the query.

    :param row_type: type of expected result. For scalar queries it is usually `int`,
    `str`, `bool`, `datetime`, or whatever can be produced in a single-column query
    result.
    :param sql: SQL statement to execute. If None, the SQL statement must be provided
    by decorated function.

    IMPORTANT: decorated function must not be async, but after decoration it
    becomes async.

    More info in the noorm.asyncpg docstring.
    """

    def decorator(
        func: Callable[F_Spec, PrepareFuncResult | ParamsAutoEnum | None]
    ) -> Callable[
        Concatenate[asyncpg.Connection, F_Spec], Coroutine[Any, Any, list[TR]]
    ]:
        class wrapper(WrapperBase):
            async def __call__(
                self,
                conn: asyncpg.Connection,
                *args: F_Spec.args,
                **kwargs: F_Spec.kwargs,
            ) -> list[TR]:
                with MetricsCollector(self._func) as mc:
                    if sql_and_params := req_sql_n_params(
                        self._func, args, kwargs, sql
                    ):
                        q_res = await conn.fetch(sql_and_params[0], *sql_and_params[1])
                        res: list[TR] = [r[0] for r in q_res]
                        mc.tuples = len(res)
                        return res
                    return []

        return wrapper(func)

    return decorator


@overload
def sql_execute(
    func: Callable[F_Spec, PrepareFuncResult | ParamsAutoEnum | None]
) -> Callable[Concatenate[asyncpg.Connection, F_Spec], Coroutine[Any, Any, None]]:
    pass  # pragma: no cover


@overload
def sql_execute(
    sql: str | None = None,
) -> Callable[
    [Callable[F_Spec, Coroutine[Any, Any, None]]],
    Callable[Concatenate[asyncpg.Connection, F_Spec], Coroutine[Any, Any, None]],
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

    More info in the noorm.asyncpg docstring.
    """

    if callable(sql):
        the_func = sql
        the_sql = None
    else:
        the_func = None
        the_sql = sql

    def decorator_wrapper(sql: str | None):
        def decorator(
            func: Callable[F_Spec, PrepareFuncResult | ParamsAutoEnum | None],
        ) -> Callable[
            Concatenate[asyncpg.Connection, F_Spec], Coroutine[Any, Any, None]
        ]:
            class wrapper(WrapperBase):
                async def __call__(
                    self,
                    conn: asyncpg.Connection,
                    *args: F_Spec.args,
                    **kwargs: F_Spec.kwargs,
                ) -> None:
                    with MetricsCollector(self._func):
                        if sql_and_params := req_sql_n_params(
                            self._func, args, kwargs, sql
                        ):
                            await conn.execute(sql_and_params[0], *sql_and_params[1])

            return wrapper(func)

        return decorator

    wrap_decorator = decorator_wrapper(the_sql)
    if callable(sql):
        return wrap_decorator(the_func)
    else:
        return wrap_decorator
