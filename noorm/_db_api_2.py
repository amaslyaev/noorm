"""
Implements DB_API_2-compliant 'params', 'query_and_params', and 'query_only'
functions.
"""


class _PrepareFuncResult:
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
    return _PrepareFuncResult(None, _bake_params(args, kwargs))


def query_and_params(sql: str, *args, **kwargs):
    return _PrepareFuncResult(sql, _bake_params(args, kwargs))


def query_only(sql: str):
    return _PrepareFuncResult(sql, None)
