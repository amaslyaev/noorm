from typing import Type, Callable, Generator, ParamSpec, TypeVar, Concatenate, overload
import sqlite3

from .._common import WrapperBase
from .._sqlite_common import (
    make_decoder as _make_decoder,
    make_scalar_decoder as _make_scalar_decoder,
)
from .._db_api_2 import PrepareFuncResult, req_sql_n_params
from ..registry import MetricsCollector

F_Spec = ParamSpec("F_Spec")
F_Return = TypeVar("F_Return")
TR = TypeVar("TR")
ConnectionOrCursor = sqlite3.Connection | sqlite3.Cursor


def sql_fetch_all(row_type: Type[TR], sql: str | None = None):
    """
    Use this decorator to make a "fetch all" SQL statement executor out of
    the function that prepares parameters for the query.

    :param row_type: type of expected result. Usually some dataclass or named tuple
    :param sql: SQL statement to execute. If None, the SQL statement must be provided
    by decorated function.

    More info in the noorm.sqlite3 docstring.
    """

    def decorator(
        func: Callable[F_Spec, PrepareFuncResult | None]
    ) -> Callable[Concatenate[ConnectionOrCursor, F_Spec], list[TR]]:
        decoder = _make_decoder(row_type)

        class wrapper(WrapperBase):
            def __call__(
                self,
                conn: ConnectionOrCursor,
                *args: F_Spec.args,
                **kwargs: F_Spec.kwargs,
            ) -> list[TR]:
                with MetricsCollector(self._func) as mc:
                    if sql_and_params := req_sql_n_params(
                        self._func, args, kwargs, sql
                    ):
                        q_res = conn.execute(*sql_and_params)
                        col_names = tuple(el[0] for el in q_res.description)
                        res: list[TR] = [
                            row_type(**decoder({n: v for n, v in zip(col_names, r)}))
                            for r in q_res
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

    More info in the noorm.sqlite3 docstring.
    """

    def decorator(
        func: Callable[F_Spec, PrepareFuncResult | None]
    ) -> Callable[Concatenate[sqlite3.Connection, F_Spec], Generator[TR, None, None]]:
        decoder = _make_decoder(row_type)

        class wrapper(WrapperBase):
            def __call__(
                self,
                conn: sqlite3.Connection,
                *args: F_Spec.args,
                **kwargs: F_Spec.kwargs,
            ) -> Generator[TR, None, None]:
                with MetricsCollector(self._func) as mc:
                    if sql_and_params := req_sql_n_params(
                        self._func, args, kwargs, sql
                    ):
                        cur = conn.cursor()
                        q_res = cur.execute(*sql_and_params)
                        col_names = tuple(el[0] for el in q_res.description)
                        is_first_row = True
                        for r in q_res:
                            if is_first_row:
                                mc.finish(None)
                                is_first_row = False
                            yield row_type(
                                **decoder({n: v for n, v in zip(col_names, r)})
                            )

        return wrapper(func)

    return decorator


def sql_one_or_none(row_type: Type[TR], sql: str | None = None):
    """
    Use this decorator to make a "one or none" SQL statement executor out of
    the function that prepares parameters for the query.

    :param row_type: type of expected result. Usually some dataclass or named tuple
    :param sql: SQL statement to execute. If None, the SQL statement must be provided
    by decorated function.

    More info in the noorm.sqlite3 docstring.
    """

    def decorator(
        func: Callable[F_Spec, PrepareFuncResult | None]
    ) -> Callable[Concatenate[ConnectionOrCursor, F_Spec], TR | None]:
        decoder = _make_decoder(row_type)

        class wrapper(WrapperBase):
            def __call__(
                self,
                conn: ConnectionOrCursor,
                *args: F_Spec.args,
                **kwargs: F_Spec.kwargs,
            ) -> TR | None:
                with MetricsCollector(self._func) as mc:
                    if sql_and_params := req_sql_n_params(
                        self._func, args, kwargs, sql
                    ):
                        q_res = conn.execute(*sql_and_params)
                        col_names = tuple(el[0] for el in q_res.description)
                        for row in q_res:
                            mc.tuples = 1
                            return row_type(
                                **decoder({n: v for n, v in zip(col_names, row)})
                            )
                    return None

        return wrapper(func)

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

    More info in the noorm.sqlite3 docstring.
    """

    def decorator(
        func: Callable[F_Spec, PrepareFuncResult | None]
    ) -> Callable[Concatenate[ConnectionOrCursor, F_Spec], TR | None]:
        decoder = _make_scalar_decoder(res_type)

        class wrapper(WrapperBase):
            def __call__(
                self,
                conn: ConnectionOrCursor,
                *args: F_Spec.args,
                **kwargs: F_Spec.kwargs,
            ) -> TR | None:
                with MetricsCollector(self._func) as mc:
                    if sql_and_params := req_sql_n_params(
                        self._func, args, kwargs, sql
                    ):
                        q_res = conn.execute(*sql_and_params)
                        for row in q_res:
                            mc.tuples = 1
                            return decoder(row[0])
                    return None

        return wrapper(func)

    return decorator


def sql_fetch_scalars(res_type: Type[TR], sql: str | None = None):
    """
    Use this decorator to make a "fetch scalars" SQL statement executor out of
    the function that prepares parameters for the query.

    :param res_type: type of expected result. For scalar queries it is usually `int`,
    `str`, `bool`, `datetime`, or whatever can be produced in a single-column query
    result. Use Any to return values without conversion.
    :param sql: SQL statement to execute. If None, the SQL statement must be provided
    by decorated function.

    More info in the noorm.sqlite3 docstring.
    """

    def decorator(
        func: Callable[F_Spec, PrepareFuncResult | None]
    ) -> Callable[Concatenate[ConnectionOrCursor, F_Spec], list[TR]]:
        decoder = _make_scalar_decoder(res_type)

        class wrapper(WrapperBase):
            def __call__(
                self,
                conn: ConnectionOrCursor,
                *args: F_Spec.args,
                **kwargs: F_Spec.kwargs,
            ) -> list[TR]:
                with MetricsCollector(self._func) as mc:
                    if sql_and_params := req_sql_n_params(
                        self._func, args, kwargs, sql
                    ):
                        q_res = conn.execute(*sql_and_params)
                        res = [decoder(row[0]) for row in q_res]
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
    result. Use Any to return values without conversion.
    :param sql: SQL statement to execute. If None, the SQL statement must be provided
    by decorated function.

    More info in the noorm.sqlite3 docstring.
    """

    def decorator(
        func: Callable[F_Spec, PrepareFuncResult | None]
    ) -> Callable[Concatenate[sqlite3.Connection, F_Spec], Generator[TR, None, None]]:
        decoder = _make_scalar_decoder(res_type)

        class wrapper(WrapperBase):
            def __call__(
                self,
                conn: sqlite3.Connection,
                *args: F_Spec.args,
                **kwargs: F_Spec.kwargs,
            ) -> Generator[TR, None, None]:
                with MetricsCollector(self._func) as mc:
                    if sql_and_params := req_sql_n_params(
                        self._func, args, kwargs, sql
                    ):
                        cur = conn.cursor()
                        q_res = cur.execute(*sql_and_params)
                        is_first_row = True
                        for r in q_res:
                            if is_first_row:
                                mc.finish(None)
                                is_first_row = False
                            yield decoder(r[0])

        return wrapper(func)

    return decorator


@overload
def sql_execute(
    func: Callable[F_Spec, PrepareFuncResult | None]
) -> Callable[Concatenate[ConnectionOrCursor, F_Spec], None]:
    pass  # pragma: no cover


@overload
def sql_execute(
    sql: str | None = None,
) -> Callable[
    [Callable[F_Spec, None]], Callable[Concatenate[ConnectionOrCursor, F_Spec], None]
]:
    pass  # pragma: no cover


def sql_execute(  # type: ignore
    sql: Callable[F_Spec, PrepareFuncResult | None] | str | None = None,
):
    """
    Use this decorator to execute a statement without responding a result.

    :param sql: SQL statement to execute. If None, the SQL statement must be provided
    by decorated function.

    More info in the noorm.sqlite3 docstring.
    """

    if callable(sql):
        the_func = sql
        the_sql = None
    else:
        the_func = None
        the_sql = sql

    def decorator_wrapper(sql: str | None):
        def decorator(
            func: Callable[F_Spec, PrepareFuncResult | None]
        ) -> Callable[Concatenate[ConnectionOrCursor, F_Spec], None]:
            class wrapper(WrapperBase):
                def __call__(
                    self,
                    conn: ConnectionOrCursor,
                    *args: F_Spec.args,
                    **kwargs: F_Spec.kwargs,
                ) -> None:
                    with MetricsCollector(self._func):
                        if sql_and_params := req_sql_n_params(
                            self._func, args, kwargs, sql
                        ):
                            conn.execute(*sql_and_params)

            return wrapper(func)

        return decorator

    wrap_decorator = decorator_wrapper(the_sql)
    if callable(sql):
        return wrap_decorator(the_func)
    else:
        return wrap_decorator
