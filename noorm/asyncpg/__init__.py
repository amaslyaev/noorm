"""
NoORM adapter for asynchronous access to Postgres via asyncpg.

Decorators:
- `sql_fetch_all(row_type: type, sql: str)` to fetch records as a list
- `sql_one_or_none(res_type: type, sql: str)` to fetch one record
- `sql_scalar_or_none(res_type: type, sql: str)` to fetch a scalar
- `sql_fetch_scalars(res_type: type, sql: str)` to fetch a list of scalars
- `sql_execute(sql: str)` to execute a statement

Decorator parameters are:
- (except `sql_execute`) Expected result type. For `sql_fetch_all` and `sql_one_or_none`
  is usually a dataclass or namedtuple; for `sql_scalar_or_none` and `sql_fetch_scalars`
  is usually `int`, `str`, `bool`, `datetime`, or whatever can be produced by scalar
  query.
- SQL statement as string. If None, must be provided from the decorated function.
  Use `$1`, `$2`, etc. as parameters placeholders.

After decoration the decorated function receives an open AsyncPg DB connection as a
first positional argument.

IMPORTANT: decorated function must not be async, but after decoration it becomes async.

Decorated function should return:
- `None` if no changes to original sql statement is needed,
  and no params to be applied
- Use `nm.params`, `nm.query_only`, or `nm.query_and_params` functions to return
  accordingly parameters values, adjusted statement, or both these things together.
  - `params(*args)` to pass parameters.
  - `query_only(sql: str)` - no query parameters, but another query instead of original.
  - `query_and_params(sql: str, *args)` to provide both - a new query and parameters.
- `nm.PARAMS_APPLY_POSITIONAL` in case you want to simply pass function parameters
  to the query as positional parameters.

Examples:
```
from collections import namedtuple
import asyncpg

import noorm.asyncpg as nm

# Fetch all, no parameters
@nm.sql_fetch_all(
    namedtuple("DbAllUsersResult", "id, username"),
    "select id, username from users order by id;",
)
def get_all_users():  # Consider no "async"
    pass  # no parameters for this query

# Fetch one by id
@nm.sql_one_or_none(
    namedtuple("DbOneUserResult", "username, email"),
    "select username, email from users where id = $1;"
)
def get_one_user(id_: int):
    return nm.params(id_)

# Fetch scalar
@nm.sql_scalar_or_none(int, "select count(*) from users where username ilike $1;")
def get_num_users(search: str):
    return nm.params(f"%{search}%")

# Insert a new record
@nm.sql_execute("insert into users(username, email) values($1, $2)")
def ins_user(username: str | None = None, email: str | None = None):
    return nm.PARAMS_APPLY_POSITIONAL

# Usage:
conn = await asyncpg.connect('postgresql://postgres@localhost/test')
for user in await get_all_users(conn):  # Consider "await"
    print(user)
print(f"User with id=1: {await get_one_user(conn, 1)}")
print(f"Search results by 'John': {await get_num_users(conn, 'John')}")

await ins_user(conn, "Jane", "jane@example.com")
# NOTE: No need to commit because asyncpg uses auto-commit.
# Explicit commit is needed only in transactions.

await conn.close()
```
"""

from ._asyncpg import (
    sql_fetch_all,
    sql_one_or_none,
    sql_scalar_or_none,
    sql_fetch_scalars,
    sql_execute,
)
from noorm._db_api_2_args_only import params, query_and_params, query_only
from noorm._common import CancelExecException
from noorm._common import PARAMS_APPLY_POSITIONAL

__all__ = [
    "sql_fetch_all",
    "sql_one_or_none",
    "sql_scalar_or_none",
    "sql_fetch_scalars",
    "sql_execute",
    "params",
    "query_and_params",
    "query_only",
    "CancelExecException",
    "PARAMS_APPLY_POSITIONAL",
]
