"""
NoORM (Not Only ORM) helpers for asynchronous sqlalchemy
"""

from typing import Type, Callable, Coroutine, ParamSpec, TypeVar, Any, overload
from typing import Concatenate

from sqlalchemy.sql import Executable, Select
from sqlalchemy.ext.asyncio import AsyncSession

F_Spec = ParamSpec("F_Spec")
F_Return = TypeVar("F_Return")
TR = TypeVar("TR")


def _req_sql_n_params(func, f_args, f_kwargs) -> Executable:
    sql: Executable = func(*f_args, **f_kwargs)
    if isinstance(sql, Executable):
        return sql
    raise TypeError(
        f"Function {func.__name__} returned {type(sql).__name__} (expected Executable)"
    )


async def _commit_if_needed(
    session: AsyncSession, sql_stmt: Executable, no_commit: bool
):
    if not isinstance(sql_stmt, Select) and not no_commit:
        await session.commit()


def sql_fetch_all(row_type: Type[TR], no_commit: bool = False):
    """
    Use this decorator to make `.all()` queries.

    :param row_type: type of expected result. Usually some dataclass or named tuple
    :param no_commit: set to False to prevent commit after the DML execution.

    IMPORTANT: decorated function must not be async, but after decoration it
    becomes async.

    More info in the noorm.sqlalchemy_async docstring.
    """

    def decorator(
        func: Callable[F_Spec, Executable]
    ) -> Callable[Concatenate[AsyncSession, F_Spec], Coroutine[Any, Any, list[TR]]]:
        async def wrapper(
            session: AsyncSession, *args: F_Spec.args, **kwargs: F_Spec.kwargs
        ) -> list[TR]:
            sql_stmt = _req_sql_n_params(func, args, kwargs)
            q_res = (await session.execute(sql_stmt)).all()
            await _commit_if_needed(session, sql_stmt, no_commit)
            res: list[row_type] = [  # type: ignore
                row_type(**{n: v for n, v in r._asdict().items()}) for r in q_res
            ]
            return res

        return wrapper

    return decorator


def sql_one_or_none(row_type: Type[TR], no_commit: bool = False):
    """
    Use this decorator to make `.one_or_none()` queries.

    :param row_type: type of expected result. Usually some dataclass or named tuple
    :param no_commit: set to False to prevent commit after the DML execution.

    IMPORTANT: decorated function must not be async, but after decoration it
    becomes async.

    More info in the noorm.sqlalchemy_async docstring.
    """

    def decorator(
        func: Callable[F_Spec, Executable],
    ) -> Callable[Concatenate[AsyncSession, F_Spec], Coroutine[Any, Any, TR | None]]:
        async def wrapper(
            session: AsyncSession, *args: F_Spec.args, **kwargs: F_Spec.kwargs
        ) -> TR | None:
            sql_stmt = _req_sql_n_params(func, args, kwargs)
            q_res = (await session.execute(sql_stmt)).one_or_none()
            await _commit_if_needed(session, sql_stmt, no_commit)
            if q_res is None:
                return None
            return row_type(**{n: v for n, v in q_res._asdict().items()})

        return wrapper

    return decorator


def sql_scalar_or_none(res_type: Type[TR], no_commit: bool = False):
    """
    Use this decorator to make a "scalar" SQL statement executor out of
    the function that prepares parameters for the query

    :param res_type: type of expected result. For scalar queries it is usually `int`,
    `str`, `bool`, `datetime`, or whatever can be produced by scalar query.
    :param no_commit: set to False to prevent commit after the DML execution.

    IMPORTANT: decorated function must not be async, but after decoration it
    becomes async.

    More info in the noorm.sqlalchemy_async docstring.
    """

    def decorator(
        func: Callable[F_Spec, Executable],
    ) -> Callable[Concatenate[AsyncSession, F_Spec], Coroutine[Any, Any, TR | None]]:
        async def wrapper(
            session: AsyncSession, *args: F_Spec.args, **kwargs: F_Spec.kwargs
        ) -> TR | None:
            sql_stmt = _req_sql_n_params(func, args, kwargs)
            q_res = (await session.execute(sql_stmt)).scalar_one_or_none()
            await _commit_if_needed(session, sql_stmt, no_commit)
            if q_res is None:
                return None
            if res_type is Any:
                return q_res
            return q_res

        return wrapper

    return decorator


def sql_fetch_scalars(res_type: Type[TR], no_commit: bool = False):
    """
    Use this decorator to make a "scalars" SQL statement executor out of
    the function that prepares parameters for the query

    :param res_type: type of expected result. For scalar queries it is usually `int`,
    `str`, `bool`, `datetime`, or whatever can be produced by scalar query.
    :param no_commit: set to False to prevent commit after the DML execution.

    IMPORTANT: decorated function must not be async, but after decoration it
    becomes async.

    More info in the noorm.sqlalchemy_async docstring.
    """

    def decorator(
        func: Callable[F_Spec, Executable],
    ) -> Callable[Concatenate[AsyncSession, F_Spec], Coroutine[Any, Any, list[TR]]]:
        async def wrapper(
            session: AsyncSession, *args: F_Spec.args, **kwargs: F_Spec.kwargs
        ) -> list[TR]:
            sql_stmt = _req_sql_n_params(func, args, kwargs)
            q_res = (await session.execute(sql_stmt)).scalars()
            await _commit_if_needed(session, sql_stmt, no_commit)
            if res_type is Any:
                return [el for el in q_res]
            return [el for el in q_res]

        return wrapper

    return decorator


@overload
def sql_execute(
    func: Callable[F_Spec, Executable]
) -> Callable[Concatenate[AsyncSession, F_Spec], Coroutine[Any, Any, None]]:
    ...  # pragma: no cover


@overload
def sql_execute(
    no_commit: bool = False,
) -> Callable[
    [Callable[F_Spec, Coroutine[Any, Any, None]]],
    Callable[Concatenate[AsyncSession, F_Spec], Coroutine[Any, Any, None]],
]:
    ...  # pragma: no cover


def sql_execute(  # type: ignore
    func: Callable[F_Spec, Executable] | None = None,
    no_commit: bool = False,
):
    """
    Use this decorator to execute a statement without responding a result.

    :param no_commit: set to False to prevent commit after the DML execution.

    IMPORTANT: decorated function must not be async, but after decoration it
    becomes async.

    More info in the noorm.sqlalchemy_async docstring.
    """

    if callable(func):
        the_func = func
    else:
        the_func = None

    def decorator_wrapper():
        def decorator(
            func: Callable[F_Spec, Executable],
        ) -> Callable[Concatenate[AsyncSession, F_Spec], Coroutine[Any, Any, None]]:
            async def wrapper(
                session: AsyncSession, *args: F_Spec.args, **kwargs: F_Spec.kwargs
            ) -> None:
                sql_stmt = _req_sql_n_params(func, args, kwargs)
                await session.execute(sql_stmt)
                await _commit_if_needed(session, sql_stmt, no_commit)

            return wrapper

        return decorator

    wrap_decorator = decorator_wrapper()
    if callable(func):
        return wrap_decorator(the_func)
    else:
        return wrap_decorator
