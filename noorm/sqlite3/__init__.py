"""
NoORM adapter for synchronous access to SQLite via Standard Python library.

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

After decoration the decorated function receives an open SQLite connection or a Cursor
as a first positional argument.

Decorated function should return:
- `None` if no changes to original sql statement is needed,
  and no params to be applied
- Use `params`, `query_only`, or `query_and_params` functions to return accordingly
  parameters values, SQL statement, or both together.
  - `params(*args, **kwargs)` to pass positional arguments for `?`-style placeholders
    or keyword arguments for named query parameter placeholders. SQL statement is
    original from decorator parameter.
  - `query_only(sql: str)` - only SQL query and no query parameters.
  - `query_and_params(sql: str, *args, **kwargs)` to provide both - a query and its
    parameters.

Examples:
```
from collections import namedtuple
import sqlite3

import noorm.sqlite3 as nm

# Fetch all, no parameters
@nm.sql_fetch_all(
    namedtuple("DbAllUsersResult", "id, username"),
    "select rowid as id, username from users order by rowid;",
)
def get_all_users():
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

# Insert a new record
@nm.sql_execute("insert into users(username, email) values(?, ?)")
def ins_user(username: str | None = None, email: str | None = None):
    return nm.params(username, email)

# Usage:
with sqlite3.connect("test.sqlite") as conn:  # "test.sqlite" is a DB file name
    for user in get_all_users(conn):
        print(user)
    print(f"User with id=1: {get_one_user(conn, 1)}")
    print(f"Search results by 'John': {get_num_users(conn, 'John')}")

    ins_user(conn, "Jane", "jane@example.com")
    conn.commit()  # Do not forget to commit the data manipulation!!!

```
"""  # noqa: E501

from ._sqlite3 import (
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
]
