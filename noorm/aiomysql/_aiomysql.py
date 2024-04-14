from typing import Type, Callable, ParamSpec, TypeVar, Any, Coroutine, Concatenate
from typing import overload
from aiomysql import Connection

from noorm._db_api_2 import _PrepareFuncResult

F_Spec = ParamSpec("F_Spec")
F_Return = TypeVar("F_Return")
TR = TypeVar("TR")


def _req_sql_n_params(
    func, f_args, f_kwargs, default_sql: str | None
) -> tuple[str, dict | tuple]:
    sql_n_params: _PrepareFuncResult | None = func(*f_args, **f_kwargs)
    if sql_n_params is not None:
        if not isinstance(sql_n_params, _PrepareFuncResult):
            raise TypeError(
                f"Function {func.__name__} returned {type(sql_n_params).__name__} "
                "(expected None or result of 'params', 'query_and_params', or "
                "'query_only' functions)"
            )
        sql = sql_n_params.sql
        params = sql_n_params.params
        if sql is None:
            sql = default_sql
        if params is None:
            params = tuple()
    else:
        sql = default_sql
        params = tuple()
    if sql is None:
        raise RuntimeError(
            f"Function {func.__name__} did not return an SQL statement "
            "in its result. When SQL statement is not provided in decorator params, "
            "it should be returned by the function through the 'query_only' or "
            "'query_and_params' function."
        )
    return sql, params


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
        func: Callable[F_Spec, _PrepareFuncResult | None]
    ) -> Callable[Concatenate[Connection, F_Spec], Coroutine[Any, Any, list[TR]]]:
        async def wrapper(
            conn: Connection, *args: F_Spec.args, **kwargs: F_Spec.kwargs
        ) -> list[TR]:
            sql_text, params = _req_sql_n_params(func, args, kwargs, sql)
            async with conn.cursor() as cur:
                await cur.execute(sql_text, params)
                col_names = tuple(el[0] for el in cur.description)
                res: list[row_type] = [  # type: ignore
                    row_type(**{n: v for n, v in zip(col_names, r)}) async for r in cur
                ]
                return res

        return wrapper

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
        func: Callable[F_Spec, _PrepareFuncResult | None]
    ) -> Callable[Concatenate[Connection, F_Spec], Coroutine[Any, Any, TR | None]]:
        async def wrapper(
            conn: Connection, *args: F_Spec.args, **kwargs: F_Spec.kwargs
        ) -> TR | None:
            sql_text, params = _req_sql_n_params(func, args, kwargs, sql)
            async with conn.cursor() as cur:
                await cur.execute(sql_text, params)
                col_names = tuple(el[0] for el in cur.description)
                row = await cur.fetchone()
                if row is not None:
                    return row_type(**{n: v for n, v in zip(col_names, row)})
                return None

        return wrapper

    return decorator


def sql_scalar_or_none(res_type: Type[TR], sql: str | None = None):
    """
    Use this decorator to make a "scalar or none" SQL statement executor out of
    the function that prepares parameters for the query.

    :param row_type: type of expected result. For scalar queries it is usually `int`,
    `str`, `bool`, `datetime`, or whatever can be produced by scalar query. Use Any to
    return a value without conversion.
    :param sql: SQL statement to execute. If None, the SQL statement must be provided
    by decorated function.

    IMPORTANT: decorated function must not be async, but after decoration it
    becomes async.

    More info in the noorm.aiomysql docstring.
    """

    def decorator(
        func: Callable[F_Spec, _PrepareFuncResult | None]
    ) -> Callable[Concatenate[Connection, F_Spec], Coroutine[Any, Any, TR | None]]:
        async def wrapper(
            conn: Connection, *args: F_Spec.args, **kwargs: F_Spec.kwargs
        ) -> TR | None:
            sql_text, params = _req_sql_n_params(func, args, kwargs, sql)
            async with conn.cursor() as cur:
                await cur.execute(sql_text, params)
                row = await cur.fetchone()
                if row is not None:
                    return row[0]
                return None

        return wrapper

    return decorator


def sql_fetch_scalars(res_type: Type[TR], sql: str | None = None):
    """
    Use this decorator to make a "fetch scalars" SQL statement executor out of
    the function that prepares parameters for the query.

    :param row_type: type of expected result. For scalar queries it is usually `int`,
    `str`, `bool`, `datetime`, or whatever can be produced in a single-column query
    result. Use Any to return a value without conversion.
    :param sql: SQL statement to execute. If None, the SQL statement must be provided
    by decorated function.

    IMPORTANT: decorated function must not be async, but after decoration it
    becomes async.

    More info in the noorm.aiomysql docstring.
    """

    def decorator(
        func: Callable[F_Spec, _PrepareFuncResult | None]
    ) -> Callable[Concatenate[Connection, F_Spec], Coroutine[Any, Any, list[TR]]]:
        async def wrapper(
            conn: Connection, *args: F_Spec.args, **kwargs: F_Spec.kwargs
        ) -> list[TR]:
            sql_text, params = _req_sql_n_params(func, args, kwargs, sql)
            async with conn.cursor() as cur:
                await cur.execute(sql_text, params)
                return [row[0] async for row in cur]

        return wrapper

    return decorator


@overload
def sql_execute(
    func: Callable[F_Spec, _PrepareFuncResult | None]
) -> Callable[Concatenate[Connection, F_Spec], Coroutine[Any, Any, None]]:
    ...  # pragma: no cover


@overload
def sql_execute(
    sql: str | None = None,
) -> Callable[
    [Callable[F_Spec, Coroutine[Any, Any, None]]],
    Callable[Concatenate[Connection, F_Spec], Coroutine[Any, Any, None]],
]:
    ...  # pragma: no cover


def sql_execute(  # type: ignore
    sql: Callable[F_Spec, _PrepareFuncResult | None] | str | None = None
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
            func: Callable[F_Spec, _PrepareFuncResult | None]
        ) -> Callable[Concatenate[Connection, F_Spec], Coroutine[Any, Any, None]]:
            async def wrapper(
                conn: Connection, *args: F_Spec.args, **kwargs: F_Spec.kwargs
            ) -> None:
                sql_text, params = _req_sql_n_params(func, args, kwargs, sql)
                async with conn.cursor() as cur:
                    await cur.execute(sql_text, params)

            return wrapper

        return decorator

    wrap_decorator = decorator_wrapper(the_sql)
    if callable(sql):
        return wrap_decorator(the_func)
    else:
        return wrap_decorator
