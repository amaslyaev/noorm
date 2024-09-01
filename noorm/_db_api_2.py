"""
Implements DB_API_2-compliant 'params', 'query_and_params', and 'query_only'
functions.
"""

from ._common import CancelExecException, args_as_tuple, args_as_dict
from ._common import ParamsAutoEnum, PARAMS_APPLY_POSITIONAL, PARAMS_APPLY_NAMED


class PrepareFuncResult:
    def __init__(self, sql: str | None, params: tuple | dict | None) -> None:
        self.sql = sql
        self.params = params


def _bake_params(args: tuple, kwargs: dict) -> tuple | dict | None:
    if args and kwargs:
        raise ValueError(
            "Only positional OR keyword arguments are allowed here, not both"
        )
    if kwargs:
        return kwargs
    elif args:
        return args
    return None


def params(*args, **kwargs):
    return PrepareFuncResult(None, _bake_params(args, kwargs))


def query_and_params(sql: str, *args, **kwargs):
    return PrepareFuncResult(sql, _bake_params(args, kwargs))


def query_only(sql: str):
    return PrepareFuncResult(sql, None)


def req_sql_n_params(
    func, f_args, f_kwargs, default_sql: str | None
) -> tuple[str, dict | tuple] | None:
    try:
        sql_n_params: PrepareFuncResult | ParamsAutoEnum | None = func(
            *f_args, **f_kwargs
        )
    except CancelExecException:
        return None

    params: dict | tuple | None
    if sql_n_params is not None:
        if sql_n_params == PARAMS_APPLY_POSITIONAL:
            sql = default_sql
            params = args_as_tuple(func, f_args, f_kwargs)
        elif sql_n_params == PARAMS_APPLY_NAMED:
            sql = default_sql
            params = args_as_dict(func, f_args, f_kwargs)
        elif isinstance(sql_n_params, PrepareFuncResult):
            sql = sql_n_params.sql
            params = sql_n_params.params
            if sql is None:
                sql = default_sql
            if params is None:
                params = tuple()
        else:
            raise TypeError(
                f"Function {func.__name__} returned {type(sql_n_params).__name__} "
                "(expected None or result of 'params', 'query_and_params', or "
                "'query_only' functions)"
            )
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
