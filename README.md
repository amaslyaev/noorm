[![PyPI pyversions](https://img.shields.io/pypi/pyversions/true-noorm.svg)](https://pypi.python.org/pypi/true-noorm/)
[![PyPI status](https://img.shields.io/pypi/status/true-noorm.svg)](https://pypi.python.org/pypi/true-noorm/)
[![GitHub license](https://img.shields.io/github/license/Naereen/StrapDown.js.svg)](https://github.com/amaslyaev/noorm/blob/master/LICENSE)
[![codecov](https://codecov.io/gh/amaslyaev/noorm/graph/badge.svg?token=31YWXNHPMM)](https://codecov.io/gh/amaslyaev/noorm)
[![Code style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black/)

## noorm

NoORM (Not only ORM) - make your database operations convenient and natural

## Install

**noorm** requires Python 3.10 or newer. Install it from PyPI:
```shell
$ pip install true-noorm
```
Please note that the correct name is "**true**-noorm".

## NoORM principles

1. It is not a holy war against ORM but "in addition to".
2. It is not one more "finally perfect" ORM. It is not an ORM at all. **No persistent objects, no "ideal" entities.**
3. It should be good for for medium-sized and big project.
4. Focus on developer experience.
5. Not only a set of helpers to write less code, but first of all, an approach that guides to more understandable, performant, scalable, robust, and maintainable solutions.

## Usage

Impotring `noorm` depends on DB you use in your project. Available options:

- `import noorm.sqlite3 as nm` – for SQLite
- `import noorm.aiosqlite as nm` – for asynchronous SQLite via **aiosqlite**
- `import noorm.psycopg2 as nm` – for synchronous Postgres via **psycopg2**
- `import noorm.asyncpg as nm` – for asynchronous Postgres via **asyncpg**
- `import noorm.pymysql as nm` – for synchronous MySQL/MariaDB via **PyMySQL**
- `import noorm.aiomysql as nm` – for asynchronous MySQL/MariaDB via **aiomysql**
- `import noorm.sqlalchemy_sync as nm` – for synchronous **SqlAlchemy**
- `import noorm.sqlalchemy_async as nm` – for asynchronous **SqlAlchemy**

Yes, using the SqlAlchemy ORM through NoORM is a nice idea.

After importing "nm" you use `@nm.sql_...` decorators to create functions that perform database operations. All other your application code uses these functions as a so-called "DB API layer". The decorators are:
- **@nm.sql_fetch_all** – to make a query and produce a list of objects of specified type. The query is usually SELECT, but it is also useful with data manipulations RETURNING data.
- **@nm.sql_one_or_none** – to make a query and produce one object or None if nothing is found.
- **@nm.sql_scalar_or_none** – to get a scalar or None if nothing is found.
- **@nm.sql_fetch_scalars** – to get a list of scalars.
- **@nm.sql_execute** – to execute something, usually INSERT, UPDATE, or DELETE.
- **@nm.sql_iterate** and **@nm.sql_iterate_scalars** – to make a query and iterate through results – objects or scalars respectively. Be careful with this features and, if possible, use `sql_fetch_all` and `sql_fetch_scalars` instead, because they give you less possibilites to shoot your leg. These functions are not implemented for **asyncpg**.

Usage of these decorators in different submodules and underlying databases might have own peculiarities, so check docstring documentation of the chosen "nm".

#### Example for SQLite through the sqlite3 standard library

Assume we have a **users** table with fields:
- Integer `id` (`rowid` in SQLite)
- String `username`
- String `email`

And an **orders** table:
- Integer `id` (`rowid` in SQLite)
- Integer `user_id` references to user id
- Datetime `order_date` (TEXT in SQLite)
- Decimal `amount` (TEXT in SQLite)

```python
from dataclasses import dataclass
from decimal import Decimal
from datetime import datetime
import sqlite3
import noorm.sqlite3 as nm

@dataclass
class DbUser:  # When we need only basic info. Not a "model"! Just a dataclass!
    id: int
    username: str
    email: str

@nm.sql_fetch_all(DbUser, "SELECT rowid AS id, username, email FROM users")
def get_all_users():
    pass  # no parameters, so just "pass"

@nm.sql_one_or_none(
    DbUser, "SELECT rowid AS id, username, email FROM users WHERE rowid = :id"
)
def get_user_by_id(id_: int):
    return nm.params(id=id_)

@dataclass
class DbUserWithOrdersSummary:  # With additional info from `orders` table
    id: int
    username: str
    sum_orders: Decimal | None  # SQLite noorm can make decimal out of TEXT
    first_order: datetime | None  # and datatime too.
    last_order: datetime | None

@nm.sql_fetch_all(
    DbUserWithOrdersSummary,
    """SELECT
        u.rowid AS id, u.username,
        SUM(o.amount) AS sum_orders,
        MIN(o.order_date) AS first_order, MAX(o.order_date) AS last_order
    FROM users u
        LEFT OUTER JOIN orders o ON o.user_id = u.rowid
    GROUP BY u.rowid, u.username ORDER BY u.rowid
    """,
)
def get_users_with_order_summary():
    pass

def main():
    with sqlite3.connect("data.sqlite") as conn:
        # will use our DB API functions
        for usr in get_all_users(conn):
            print(usr)

        print(f"{get_user_by_id(conn, 1)=}")

        for usr_summary in get_users_with_order_summary(conn):
            print(usr_summary)
```

#### Example for SQLite through SqlAlchemy

Will use asynchronous version of SqlAlchemy "nm".
```python
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
import noorm.sqlalchemy_async as nm
...

@nm.sql_fetch_all(DbUser)
def get_all_users():  # Notice no "async"
    return sa.select(User.id, User.username, User.email)

@nm.sql_one_or_none(DbUser)
def get_user_by_id(id_: int):
    return sa.select(User.id, User.username, User.email).where(User.id == id_)

@nm.sql_fetch_all(DbUserWithOrdersSummary)
def get_users_with_order_summary():
    return (
        sa.select(
            User.id,
            User.username,
            sa.func.sum(Order.amount).label("sum_orders"),
            sa.func.min(Order.order_date).label("first_order"),
            sa.func.max(Order.order_date).label("last_order"),
        )
        .select_from(User)  # `User` and `Order` are ORM model classes
        .outerjoin(Order, Order.user_id == User.id)
        .group_by(User.id)
        .order_by(User.id)
    )

async def main():
    engine = create_async_engine("sqlite+aiosqlite:///data.sqlite")
    async with AsyncSession(engine) as session:
        for usr in await get_all_users(session):  # Notice "await"
            print(usr)

        print(f"user_1={await get_user_by_id(session, 1)}")

        for usr_summary in await get_users_with_order_summary(session):
            print(usr_summary)
```

## Suggestions

1. Code structure:
   - Avoid scattering DB API functions throughout the codebase. It's preferable to consolidate them in dedicated locations.
   - For small applications, consider housing all DB API functions in a single module, which becomes the DB API layer.
   - In larger applications, dividing the DB API layer into multiple modules is advisable. For example, organize user management functions in `db_api/users.py` and order processing functions in `db_api/orders.py`.
   - For systems with distinct, independent subsystems, consider placing common functions in a shared location, such as the `db/db_api/` folder, and specific functions in subsystem folders like `external_api/db_api/`.
   - Declare the "Db..." dataclasses immediately preceding the functions that produce them.
2. Naming:
   - Prefix classes returned from DB API functions with "Db". For instance, use "DbUsers", "DbOrders", "DbOrdersWithDetails", "DbInvoicesReportLine".
   - Prefix functions that SELECT data from the DB with "get_". For example, "get_user_by_id", "get_orders_by_user", etc.
   - Use prefix "iter_" for DB API functions made using `@nm.sql_iterate` and `@nm.sql_iterate_scalars` decorators.
   - For data manipulation functions:
     - Use prefixes "ins_", "upd_", "del_" for INSERTs, UPDATEs, DELETEs respectively.
     - Employ "upsert_" for upsert operations.

## Advanced features

#### Cancelling operations

If, for any reason, you need to terminate execution and produce a default result in your function, raise the `nm.CancelExecException` exception. Example:
```python
@nm.sql_fetch_all(DbOrder)
def get_orders_by_ids(order_ids: list[int]):
    if not order_ids:
        raise nm.CancelExecException
    return select(Order.id, Order.date, Order.amount).where(
        Order.id.in_(order_ids)  # <<< empty list is not acceptable here
    )
```
The `nm.CancelExecException` triggers production of a default empty result without querying the DB:
- `@nm.sql_fetch_all` and `@nm.sql_fetch_scalars` return an empty list
- `@nm.sql_one_or_none` and `@nm.sql_scalar_or_none` return None
- `@nm.sql_execute` takes no action

#### Registry

Observability is crucial. This library facilitates collecting statistics on DB API function usage out of the box. Statistics include:
- **calls** – number of calls
- **duration** – total execution time
- **tuples** – total number of retrieved tuples
- **fails** – number of failed calls
- **fails_by_error** – dict[str, int] – detailed fails categorized by exception types
```python
from noorm.registry import get_registry
registry = get_registry()
stat = registry.stat_by_name["db.db_api.orders.get_orders_by_user"]
print(stat)  # Stat(calls=3, duration=0.0324, tuples=11, fails=0, fails_by_error={})
```
To collect statistics in a multiprocessing application, initialize this option in your MainProcess:
```python
# Example for uvicorn
import uvicorn
from noorm.registry import get_registry

async def app(scope, receive, send):
    ...

if __name__ == "__main__":
    get_registry().init_multiprocess_registry()  # <<< here
    uvicorn.run("main:app", port=5000, workers=3, log_level="info")
```
Consequently, all DB operations occurring in child processes will be aggregated in the MainProcess registry.

**Important**: statistics for `@nm.sql_iterate` and `@nm.sql_iterate_scalars` is not precise:
1. `stat.duration` is counted only for query execution and first row extraction.
2. `stat.tuples` is always zero.
3. `stat.fails` and `stat.fails_by_error` do not counter errors that might happen after successful first row extraction.

#### Executing unwrapped functions

To call an unwrapped version of a DB API function for evaluation, testing, or debugging purposes:
```python
orders_list = await get_orders_by_user(session, 1)  # A "normal" call
orders_list_q = get_orders_by_user.unwrapped(1)  # An "unwrapped" call
print(str(orders_list_q.compile()))  # Want to see a compiled SqlAlchemy "SELECT ..."
```

## Acknowledgements

Inspired and sponsored by [FRAMEN](https://www.framen.com/)
