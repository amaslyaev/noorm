from ._common import CancelExecException


class PrepareFuncResult:
    def __init__(self, sql: str | None, params: tuple | None) -> None:
        self.sql = sql
        self.params = params


def params(*args):
    return PrepareFuncResult(None, args)


def query_and_params(sql: str, *args):
    return PrepareFuncResult(sql, args)


def query_only(sql: str):
    return PrepareFuncResult(sql, None)


def req_sql_n_params(
    func, f_args, f_kwargs, default_sql: str | None
) -> tuple[str, tuple] | None:
    try:
        sql_n_params: PrepareFuncResult | None = func(*f_args, **f_kwargs)
    except CancelExecException:
        return None
    if sql_n_params is not None:
        if not isinstance(sql_n_params, PrepareFuncResult):
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
