"""
NoORM (Not Only ORM) helpers for synchronous sqlalchemy
"""

from typing import Type, Callable, ParamSpec, TypeVar, overload, Concatenate

from sqlalchemy.sql import Executable, Select
from sqlalchemy.orm import Session as OrmSession, scoped_session

from .._sqlalchemy_common import req_sql_n_params

F_Spec = ParamSpec("F_Spec")
F_Return = TypeVar("F_Return")
TR = TypeVar("TR")
Session = OrmSession | scoped_session


def _commit_if_needed(session: Session, sql_stmt: Executable, no_commit: bool):
    if not isinstance(sql_stmt, Select) and not no_commit:
        session.commit()


def sql_fetch_all(row_type: Type[TR], no_commit: bool = False):
    """
    Use this decorator to make `.all()` queries.

    :param row_type: type of expected result. Usually some dataclass or named tuple
    :param no_commit: set to False to prevent commit after the DML execution.

    More info in the noorm.sqlalchemy_sync docstring.
    """

    def decorator(
        func: Callable[F_Spec, Executable]
    ) -> Callable[Concatenate[Session, F_Spec], list[TR]]:
        def wrapper(
            session: Session, *args: F_Spec.args, **kwargs: F_Spec.kwargs
        ) -> list[TR]:
            if (sql_stmt := req_sql_n_params(func, args, kwargs)) is not None:
                q_res = session.execute(sql_stmt).all()
                _commit_if_needed(session, sql_stmt, no_commit)
                res: list[TR] = [
                    row_type(**{n: v for n, v in r._asdict().items()}) for r in q_res
                ]
                return res
            return []

        return wrapper

    return decorator


def sql_one_or_none(row_type: Type[TR], no_commit: bool = False):
    """
    Use this decorator to make `.one_or_none()` queries.

    :param row_type: type of expected result. Usually some dataclass or named tuple
    :param no_commit: set to False to prevent commit after the DML execution.

    More info in the noorm.sqlalchemy_sync docstring.
    """

    def decorator(
        func: Callable[F_Spec, Executable],
    ) -> Callable[Concatenate[Session, F_Spec], TR | None]:
        def wrapper(
            session: Session, *args: F_Spec.args, **kwargs: F_Spec.kwargs
        ) -> TR | None:
            if (sql_stmt := req_sql_n_params(func, args, kwargs)) is not None:
                q_res = session.execute(sql_stmt).one_or_none()
                _commit_if_needed(session, sql_stmt, no_commit)
                if q_res is None:
                    return None
                return row_type(**{n: v for n, v in q_res._asdict().items()})
            return None

        return wrapper

    return decorator


def sql_scalar_or_none(res_type: Type[TR], no_commit: bool = False):
    """
    Use this decorator to make a "scalar" SQL statement executor out of
    the function that prepares parameters for the query

    :param res_type: type of expected result. For scalar queries it is usually `int`,
    `str`, `bool`, `datetime`, or whatever can be produced by scalar query.
    :param no_commit: set to False to prevent commit after the DML execution.

    More info in the noorm.sqlalchemy_sync docstring.
    """

    def decorator(
        func: Callable[F_Spec, Executable],
    ) -> Callable[Concatenate[Session, F_Spec], TR | None]:
        def wrapper(
            session: Session, *args: F_Spec.args, **kwargs: F_Spec.kwargs
        ) -> TR | None:
            if (sql_stmt := req_sql_n_params(func, args, kwargs)) is not None:
                q_res = session.execute(sql_stmt).scalar_one_or_none()
                _commit_if_needed(session, sql_stmt, no_commit)
                return q_res
            return None

        return wrapper

    return decorator


def sql_fetch_scalars(res_type: Type[TR], no_commit: bool = False):
    """
    Use this decorator to make a "scalars" SQL statement executor out of
    the function that prepares parameters for the query

    :param res_type: type of expected result. For scalar queries it is usually `int`,
    `str`, `bool`, `datetime`, or whatever can be produced by scalar query.
    :param no_commit: set to False to prevent commit after the DML execution.

    More info in the noorm.sqlalchemy_sync docstring.
    """

    def decorator(
        func: Callable[F_Spec, Executable],
    ) -> Callable[Concatenate[Session, F_Spec], list[TR]]:
        def wrapper(
            session: Session, *args: F_Spec.args, **kwargs: F_Spec.kwargs
        ) -> list[TR]:
            if (sql_stmt := req_sql_n_params(func, args, kwargs)) is not None:
                q_res = session.execute(sql_stmt).scalars()
                _commit_if_needed(session, sql_stmt, no_commit)
                return [el for el in q_res]
            return []

        return wrapper

    return decorator


@overload
def sql_execute(
    func: Callable[F_Spec, Executable]
) -> Callable[Concatenate[Session, F_Spec], None]:
    pass  # pragma: no cover


@overload
def sql_execute(
    no_commit: bool = False,
) -> Callable[[Callable[F_Spec, None]], Callable[Concatenate[Session, F_Spec], None]]:
    pass  # pragma: no cover


def sql_execute(  # type: ignore
    func: Callable[F_Spec, Executable] | None = None,
    no_commit: bool = False,
):
    """
    Use this decorator to execute a statement without responding a result.

    :param no_commit: set to False to prevent commit after the DML execution.

    More info in the noorm.sqlalchemy_sync docstring.
    """

    if callable(func):
        the_func = func
    else:
        the_func = None

    def decorator_wrapper():
        def decorator(
            func: Callable[F_Spec, Executable],
        ) -> Callable[Concatenate[Session, F_Spec], None]:
            def wrapper(
                session: Session, *args: F_Spec.args, **kwargs: F_Spec.kwargs
            ) -> None:
                if (sql_stmt := req_sql_n_params(func, args, kwargs)) is not None:
                    session.execute(sql_stmt)
                    _commit_if_needed(session, sql_stmt, no_commit)

            return wrapper

        return decorator

    wrap_decorator = decorator_wrapper()
    if callable(func):
        return wrap_decorator(the_func)
    else:
        return wrap_decorator
