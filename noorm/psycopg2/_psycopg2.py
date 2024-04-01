"""
NoORM (Not Only ORM) helpers for psycopg2
"""

from typing import Type, Callable, ParamSpec, TypeVar, Concatenate, Any, overload

from .._db_api_2 import _PrepareFuncResult

F_Spec = ParamSpec("F_Spec")
F_Return = TypeVar("F_Return")
TR = TypeVar("TR")
Psycopg2Connection = Any


def _req_sql_n_params(
    func, f_args, f_kwargs, default_sql: str | None
) -> tuple[str, tuple | dict]:
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
    Use this decorator to make a "fetch" SQL statement executor out of
    the function that prepares parameters for the query

    :param row_type: type of expected result. Usually some dataclass or named tuple
    :param sql: SQL statement to execute. If None, the SQL statement must be provided
    by decorated function.

    More info in the noorm.psycopg2 docstring.
    """

    def decorator(
        func: Callable[F_Spec, _PrepareFuncResult | None]
    ) -> Callable[Concatenate[Psycopg2Connection, F_Spec], list[TR]]:
        def wrapper(conn, *args: F_Spec.args, **kwargs: F_Spec.kwargs) -> list[TR]:
            sql_text, params = _req_sql_n_params(func, args, kwargs, sql)
            with conn.cursor() as cur:
                cur.execute(sql_text, params)
                col_names = tuple(el[0] for el in cur.description)
                res: list[row_type] = [  # type: ignore
                    row_type(**{n: v for n, v in zip(col_names, r)}) for r in cur
                ]
                return res

        return wrapper

    return decorator


def sql_one_or_none(row_type: Type[TR], sql: str | None = None):
    """
    Use this decorator to make an "fetchrow" SQL statement executor out of
    the function that prepares parameters for the query

    :param row_type: type of expected result. Usually some dataclass or named tuple
    :param sql: SQL statement to execute. If None, the SQL statement must be provided
    by decorated function.

    More info in the noorm.psycopg2 docstring.
    """

    def decorator(
        func: Callable[F_Spec, _PrepareFuncResult | None],
    ) -> Callable[Concatenate[Psycopg2Connection, F_Spec], TR | None]:
        def wrapper(conn, *args: F_Spec.args, **kwargs: F_Spec.kwargs) -> TR | None:
            sql_text, params = _req_sql_n_params(func, args, kwargs, sql)
            with conn.cursor() as cur:
                cur.execute(sql_text, params)
                col_names = tuple(el[0] for el in cur.description)
                for row in cur:
                    return row_type(**{n: v for n, v in zip(col_names, row)})
                return None

        return wrapper

    return decorator


def sql_scalar_or_none(res_type: Type[TR], sql: str | None = None):
    """
    Use this decorator to make a "scalar" SQL statement executor out of
    the function that prepares parameters for the query

    :param res_type: type of expected result. For scalar queries it is usually `int`,
    `str`, `bool`, `datetime`, or whatever can be produced by scalar query.
    :param sql: SQL statement to execute. If None, the SQL statement must be provided
    by decorated function.

    More info in the noorm.psycopg2 docstring.
    """

    def decorator(
        func: Callable[F_Spec, _PrepareFuncResult | None],
    ) -> Callable[Concatenate[Psycopg2Connection, F_Spec], TR | None]:
        def wrapper(conn, *args: F_Spec.args, **kwargs: F_Spec.kwargs) -> TR | None:
            sql_text, params = _req_sql_n_params(func, args, kwargs, sql)
            with conn.cursor() as cur:
                cur.execute(sql_text, params)
                for row in cur:
                    return row[0]
                return None

        return wrapper

    return decorator


def sql_fetch_scalars(res_type: Type[TR], sql: str | None = None):
    """
    Use this decorator to make a "fetch scalars" SQL statement executor out of
    the function that prepares parameters for the query

    :param row_type: type of expected result. For scalar queries it is usually `int`,
    `str`, `bool`, `datetime`, or whatever can be produced in a single-column query
    result.
    :param sql: SQL statement to execute. If None, the SQL statement must be provided
    by decorated function.

    More info in the noorm.psycopg2 docstring.
    """

    def decorator(
        func: Callable[F_Spec, _PrepareFuncResult | None]
    ) -> Callable[Concatenate[Psycopg2Connection, F_Spec], list[TR]]:
        def wrapper(conn, *args: F_Spec.args, **kwargs: F_Spec.kwargs) -> list[TR]:
            sql_text, params = _req_sql_n_params(func, args, kwargs, sql)
            with conn.cursor() as cur:
                cur.execute(sql_text, params)
                res: list[res_type] = [r[0] for r in cur]  # type: ignore
                return res

        return wrapper

    return decorator


@overload
def sql_execute(
    func: Callable[F_Spec, _PrepareFuncResult | None]
) -> Callable[Concatenate[Psycopg2Connection, F_Spec], None]:
    ...  # pragma: no cover


@overload
def sql_execute(
    sql: str | None = None,
) -> Callable[
    [Callable[F_Spec, None]], Callable[Concatenate[Psycopg2Connection, F_Spec], None]
]:
    ...  # pragma: no cover


def sql_execute(  # type: ignore
    sql: Callable[F_Spec, _PrepareFuncResult | None] | str | None = None,
):
    """
    Use this decorator to execute a statement without responding a result

    :param sql: SQL statement to execute. If None, the SQL statement must be provided
    by decorated function.

    More info in the noorm.psycopg2 docstring.
    """

    if callable(sql):
        the_func = sql
        the_sql = None
    else:
        the_func = None
        the_sql = sql

    def decorator_wrapper(sql: str | None):
        def decorator(
            func: Callable[F_Spec, _PrepareFuncResult | None],
        ) -> Callable[Concatenate[Psycopg2Connection, F_Spec], None]:
            def wrapper(conn, *args: F_Spec.args, **kwargs: F_Spec.kwargs) -> None:
                sql_text, params = _req_sql_n_params(func, args, kwargs, sql)
                with conn.cursor() as cur:
                    cur.execute(sql_text, params)

            return wrapper

        return decorator

    wrap_decorator = decorator_wrapper(the_sql)
    if callable(sql):
        return wrap_decorator(the_func)
    else:
        return wrap_decorator