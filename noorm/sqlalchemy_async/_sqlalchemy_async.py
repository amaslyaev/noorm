"""
NoORM (Not Only ORM) helpers for asynchronous sqlalchemy
"""

from typing import Type, Callable, Coroutine, ParamSpec, TypeVar, Any, overload
from typing import Concatenate

from sqlalchemy.sql import Executable, Select
from sqlalchemy.ext.asyncio import AsyncSession

from .._sqlalchemy_common import req_sql_n_params
from .._common import WrapperBase

F_Spec = ParamSpec("F_Spec")
F_Return = TypeVar("F_Return")
TR = TypeVar("TR")


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
        class wrapper(WrapperBase):
            async def __call__(
                self, session: AsyncSession, *args: F_Spec.args, **kwargs: F_Spec.kwargs
            ) -> list[TR]:
                if (sql_stmt := req_sql_n_params(self._func, args, kwargs)) is not None:
                    q_res = (await session.execute(sql_stmt)).all()
                    await _commit_if_needed(session, sql_stmt, no_commit)
                    res: list[TR] = [
                        row_type(**{n: v for n, v in r._asdict().items()})
                        for r in q_res
                    ]
                    return res
                return []

        return wrapper(func)

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
        class wrapper(WrapperBase):
            async def __call__(
                self, session: AsyncSession, *args: F_Spec.args, **kwargs: F_Spec.kwargs
            ) -> TR | None:
                if (sql_stmt := req_sql_n_params(self._func, args, kwargs)) is not None:
                    q_res = (await session.execute(sql_stmt)).one_or_none()
                    await _commit_if_needed(session, sql_stmt, no_commit)
                    if q_res is None:
                        return None
                    return row_type(**{n: v for n, v in q_res._asdict().items()})
                return None

        return wrapper(func)

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
        class wrapper(WrapperBase):
            async def __call__(
                self, session: AsyncSession, *args: F_Spec.args, **kwargs: F_Spec.kwargs
            ) -> TR | None:
                if (sql_stmt := req_sql_n_params(self._func, args, kwargs)) is not None:
                    q_res = (await session.execute(sql_stmt)).scalar_one_or_none()
                    await _commit_if_needed(session, sql_stmt, no_commit)
                    return q_res
                return None

        return wrapper(func)

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
        class wrapper(WrapperBase):
            async def __call__(
                self, session: AsyncSession, *args: F_Spec.args, **kwargs: F_Spec.kwargs
            ) -> list[TR]:
                if (sql_stmt := req_sql_n_params(self._func, args, kwargs)) is not None:
                    q_res = (await session.execute(sql_stmt)).scalars()
                    await _commit_if_needed(session, sql_stmt, no_commit)
                    return [el for el in q_res]
                return []

        return wrapper(func)

    return decorator


@overload
def sql_execute(
    func: Callable[F_Spec, Executable]
) -> Callable[Concatenate[AsyncSession, F_Spec], Coroutine[Any, Any, None]]:
    pass  # pragma: no cover


@overload
def sql_execute(
    no_commit: bool = False,
) -> Callable[
    [Callable[F_Spec, Coroutine[Any, Any, None]]],
    Callable[Concatenate[AsyncSession, F_Spec], Coroutine[Any, Any, None]],
]:
    pass  # pragma: no cover


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
            class wrapper(WrapperBase):
                async def __call__(
                    self,
                    session: AsyncSession,
                    *args: F_Spec.args,
                    **kwargs: F_Spec.kwargs,
                ) -> None:
                    if (
                        sql_stmt := req_sql_n_params(self._func, args, kwargs)
                    ) is not None:
                        await session.execute(sql_stmt)
                        await _commit_if_needed(session, sql_stmt, no_commit)

            return wrapper(func)

        return decorator

    wrap_decorator = decorator_wrapper()
    if callable(func):
        return wrap_decorator(the_func)
    else:
        return wrap_decorator
