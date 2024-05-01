from sqlalchemy.sql import Executable

from ._common import CancelExecException


def req_sql_n_params(func, f_args, f_kwargs) -> Executable | None:
    try:
        sql: Executable = func(*f_args, **f_kwargs)
    except CancelExecException:
        return None
    if isinstance(sql, Executable):
        return sql
    raise TypeError(
        f"Function {func.__name__} returned {type(sql).__name__} (expected Executable)"
    )
