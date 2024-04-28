"""
NoORM adapter for synchronous SQLAlchemy.

Decorators:
- `sql_fetch_all(row_type: type, no_commit: bool)` to fetch records as a list
- `sql_one_or_none(res_type: type, no_commit: bool)` to fetch one record
- `sql_scalar_or_none(res_type: type, no_commit: bool)` to fetch a scalar
- `sql_fetch_scalars(res_type: type, no_commit: bool)` to fetch a list of scalars
- `sql_execute(no_commit: bool)` to execute a statement

Decorator parameters are:
- (except `sql_execute`) Expected result type. For `sql_fetch_all` and `sql_one_or_none`
  is usually a dataclass or namedtuple; for `sql_scalar_or_none` and `sql_fetch_scalars`
  is usually `int`, `str`, `bool`, `datetime`, or whatever can be produced by scalar
  query.
- "No commit" flag. Default is False, so if executed statement is data manipulation
  (insert, update, or delete), commit will be automatically invoked. If you need
  to prevent it for some reason, set no_commit=True.

After decoration the decorated function receives an open Session as a first positional
argument.

Decorated function should return an executable statement (select, insert, update,
delete).

Examples:
```
from collections import namedtuple

import sqlalchemy as sa
import noorm.sqlalchemy_sync as nm

from db import User  # Model class for users table

# Fetch all
@nm.sql_fetch_all(namedtuple("DbAllUsersResult", "id, username, email"))
def get_all_users():
    return sa.select(User.id, User.username, User.email).order_by(User.id))

# Fetch one
@nm.sql_one_or_none(namedtuple("DbOneUserResult", "username, email"))
def get_one_user(id_: int):
    return sa.select(User.username, User.email).where(User.id == id_)

# Insert a new record
@nm.sql_execute
def ins_user(username: str | None = None, email: str | None = None):
    return sa.insert(User).values(username=username, email=email)

# Usage:
    for user in get_all_users(session):
        print(user)
    print(f"User with id=1: {get_one_user(session, 1)}")

    ins_user(session, "Jane", "jane@example.com") # Commit is done automatically
```
"""

from ._sqlalchemy_sync import (
    sql_fetch_all,
    sql_one_or_none,
    sql_scalar_or_none,
    sql_fetch_scalars,
    sql_execute,
)

__all__ = [
    "sql_fetch_all",
    "sql_one_or_none",
    "sql_scalar_or_none",
    "sql_fetch_scalars",
    "sql_execute",
]
