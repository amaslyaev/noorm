"""
NoORM adapter for asynchronous access to SQLite via aiosqlite.

Decorators:
- `sql_fetch_all(row_type: type, sql: str)` to fetch records as a list
- `sql_one_or_none(res_type: type, sql: str)` to fetch one record
- `sql_scalar_or_none(res_type: type, sql: str)` to fetch a scalar
- `sql_fetch_scalars(res_type: type, sql: str)` to fetch a list of scalars
- `sql_execute(sql: str)` to execute a statement
- `sql_iterate(row_type: type, sql: str)` and
  `sql_iterate_scalars(row_type: type, sql: str)` to make a query and iterate
  through results, objects or scalars respectively.

Decorator parameters are:
- (except `sql_execute`) Expected result type. For `sql_fetch_all` and `sql_one_or_none`
  is usually a dataclass or namedtuple; for `sql_scalar_or_none` and `sql_fetch_scalars`
  is usually `int`, `str`, `bool`, `datetime`, or whatever can be produced by scalar
  query.
- SQL statement as string. If None, must be provided from the decorated function.
  Query parameters placeholders are `?` for positional and `:name` for named arguments:
  - `select rowid, username from users where username like ? and birthdate >= ?`
  - `select rowid, username from users where username like :name and birthdate >= :bdate`

After decoration the decorated function receives an open aiosqlite.Connection or an
aiosqlite.Cursor as a first positional argument.

IMPORTANT: decorated function must NOT be async, but after decoration it becomes async.

Decorated function should return:
- `None` if no changes to original sql statement is needed,
  and no params to be applied
- Use `nm.params`, `nm.query_only`, or `nm.query_and_params` functions to return
  accordingly parameters values, SQL statement, or both together.
  - `params(*args, **kwargs)` to pass positional arguments for `?`-style placeholders
    or keyword arguments for named query parameter placeholders. SQL statement is
    original from decorator parameter.
  - `query_only(sql: str)` - only SQL query and no query parameters.
  - `query_and_params(sql: str, *args, **kwargs)` to provide both - a query and its
    parameters.
- `nm.PARAMS_APPLY_POSITIONAL` or `nm.PARAMS_APPLY_NAMED` in case you want to simply
  pass function parameters to the query as accordingly positional or named parameters.

SQLite-specific features:
1. You can use `date`, `datetime`, `bool`, `Decimal` fields in query result dataclasses.
   Conversion will be done automatically.
2. You can also pass values of these types as parameters. They will be converted to
   SQLite-acceptable types automatically. Date and datetime become TEXT in ISO format,
   booleans become INTEGER, Decimal becomes TEXT.
3. Although SQLite engine does not accept collections as parameter values, here you can
   do it. That is especially useful in `WHERE ... IN (?)` conditions. Such parameter
   can be any kind if plain collection: tuple, list, set, etc., but not dict. See
   the `get_several_users` example below.

Examples:
```
from collections import namedtuple
import aiosqlite

import noorm.aiosqlite as nm

# Fetch all, no parameters
@nm.sql_fetch_all(
    namedtuple("DbAllUsersResult", "id, username"),
    "select rowid as id, username from users order by rowid;",
)
def get_all_users():  # Consider no "async"
    pass  # no parameters for this query

# Fetch one, the '?' style parameter
@nm.sql_one_or_none(
    namedtuple("DbOneUserResult", "username, email"),
    "select username, email from users where rowid = ?;",  # Placeholder is "?"
)
def get_one_user(rowid: int):
    return nm.params(rowid)

# Fetch scalar using named parameter
@nm.sql_scalar_or_none(
    int,
    "select count(*) from users where username like :search;"  # Named placeholder
)
def get_num_users(search: str):
    return nm.params(search=f"%{search}%")

# Fetch several users
@nm.sql_fetch_all(
    namedtuple("DbUsersResult", "id, username, email"),
    "select rowid as id, username, email from users where rowid in (:ids);",
)
def get_several_users(ids: list[int]):
    return nm.PARAMS_APPLY_NAMED

# Insert a new record
@nm.sql_execute("insert into users(username, email) values(?, ?)")
def ins_user(username: str | None = None, email: str | None = None):
    return nm.PARAMS_APPLY_POSITIONAL

# Usage:
async with aiosqlite.connect("test.sqlite") as conn:
    for user in await get_all_users(conn):  # Consider "await"
        print(user)
    print(f"User with id=1: {await get_one_user(conn, 1)}")
    print(f"Search results by 'John': {await get_num_users(conn, 'John')}")

    await ins_user(conn, "Jane", "jane@example.com")
    await conn.commit()  # Do not forget to commit the data manipulation!!!

```
"""  # noqa: E501

from ._aiosqlite import (
    sql_fetch_all,
    sql_iterate,
    sql_one_or_none,
    sql_scalar_or_none,
    sql_fetch_scalars,
    sql_iterate_scalars,
    sql_execute,
)
from noorm._db_api_2 import params, query_and_params, query_only
from noorm._common import CancelExecException
from noorm._common import PARAMS_APPLY_POSITIONAL, PARAMS_APPLY_NAMED

__all__ = [
    "sql_fetch_all",
    "sql_iterate",
    "sql_one_or_none",
    "sql_scalar_or_none",
    "sql_fetch_scalars",
    "sql_iterate_scalars",
    "sql_execute",
    "params",
    "query_and_params",
    "query_only",
    "CancelExecException",
    "PARAMS_APPLY_POSITIONAL",
    "PARAMS_APPLY_NAMED",
]
