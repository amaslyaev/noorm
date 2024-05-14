from sqlalchemy.sql import Executable, Select

from ._common import CancelExecException


def req_sql_n_params(
    func, f_args, f_kwargs, sync_session: bool | str | None
) -> Executable | None:
    try:
        sql: Executable = func(*f_args, **f_kwargs)
    except CancelExecException:
        return None
    if isinstance(sql, Executable):
        if not isinstance(sql, Select) and sync_session is not None:
            sql = sql.execution_options(synchronize_session=sync_session)
        return sql
    raise TypeError(
        f"Function {func.__name__} returned {type(sql).__name__} (expected Executable)"
    )
