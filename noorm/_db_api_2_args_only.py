class _PrepareFuncResult:
    def __init__(self, sql: str | None, params: tuple | None) -> None:
        self.sql = sql
        self.params = params


def params(*args):
    return _PrepareFuncResult(None, args)


def query_and_params(sql: str, *args):
    return _PrepareFuncResult(sql, args)


def query_only(sql: str):
    return _PrepareFuncResult(sql, None)
