from typing import Any
import aiosqlite
from collections import namedtuple
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from hashlib import sha1

import pytest
import noorm.aiosqlite as nm


@pytest.fixture
async def tst_conn():
    async with aiosqlite.connect(":memory:") as conn:
        await conn.execute(
            """
            create table users(
                username text,
                email text,
                birthdate text,  -- date
                salary text,  -- decimal
                last_seen text,  -- datetime
                password_hash blob
            );
            """
        )
        await conn.execute(
            """
            insert into users(
                username, email, birthdate, salary, last_seen, password_hash
            )
            values('John', 'john@doe.com', NULL, '1234.56',
                '2023-05-14T12:34:56+00:00', ?);
            """,
            (sha1(b"123456").digest(),),
        )
        await conn.execute(
            """
            insert into users(username, email, birthdate, salary, last_seen)
            values('Jane', 'jane@doe.com', '1982-04-27', NULL, NULL);
            """
        )
        await conn.commit()
        yield conn


# MARK: sql_fetch_all


@nm.sql_fetch_all(
    namedtuple("AllUsersResult", "id,username"),
    "select rowid as id, username from users order by rowid",
)
def get_all_users_namedtuple():
    pass


async def test_fetch_all(tst_conn: aiosqlite.Connection):
    got = await get_all_users_namedtuple(tst_conn)
    assert len(got) == 2
    rtype = type(got[0])
    assert got == [rtype(1, "John"), rtype(2, "Jane")]

    async with tst_conn.cursor() as cur:
        got = await get_all_users_namedtuple(cur)
        assert got == [rtype(1, "John"), rtype(2, "Jane")]


@nm.sql_fetch_all(
    namedtuple("AllUsersResult", "id,username"),
    "select rowid as id, username from users where rowid > :id order by rowid",
)
def get_all_users_wrong1(id_: int):
    return id_


@nm.sql_fetch_all(namedtuple("AllUsersResult", "id,username"))
def get_all_users_wrong2(id_: int):
    sql = "select rowid as id, username from users where rowid > :id order by rowid"
    return nm.query_and_params(sql, id_, id=id_)


@nm.sql_fetch_all(namedtuple("AllUsersResult", "id,username"))
def get_all_users_wrong3(id_: int):
    return nm.params(id=id_)


async def test_fetch_all_wrong(tst_conn: aiosqlite.Connection):
    with pytest.raises(TypeError):
        _ = await get_all_users_wrong1(tst_conn, 1)
    with pytest.raises(ValueError):
        _ = await get_all_users_wrong2(tst_conn, 1)
    with pytest.raises(RuntimeError):
        _ = await get_all_users_wrong3(tst_conn, 1)


@dataclass
class UInfo:
    id: int
    username: str
    email: str | None
    birthdate: date | None
    salary: Decimal | None
    last_seen: datetime | None


@nm.sql_fetch_all(
    UInfo,
    """select rowid as id, username, email, birthdate, salary, last_seen
    from users where username like :search or email like :search
    order by rowid""",
)
def find_users_by_text(search: str):
    if not search:
        raise nm.CancelExecException
    return nm.params(search=f"%{search}%")


async def test_fetch_find(tst_conn: aiosqlite.Connection):
    got = await find_users_by_text(tst_conn, "doe")
    assert got == [
        UInfo(
            id=1,
            username="John",
            email="john@doe.com",
            birthdate=None,
            salary=Decimal("1234.56"),
            last_seen=datetime(2023, 5, 14, 12, 34, 56, tzinfo=timezone.utc),
        ),
        UInfo(
            id=2,
            username="Jane",
            email="jane@doe.com",
            birthdate=date(1982, 4, 27),
            salary=None,
            last_seen=None,
        ),
    ]
    got = await find_users_by_text(tst_conn, None)
    assert got == []


# MARK: sql_iterate


@nm.sql_iterate(
    namedtuple("AllUsersResult", "id,username"),
    "select rowid as id, username from users order by rowid",
)
def iter_users():
    pass


async def test_iter_users(tst_conn: aiosqlite.Connection):
    gen = iter_users(tst_conn)
    first = await anext(gen)
    rtype = type(first)
    assert first == rtype(1, "John")
    assert await anext(gen) == rtype(2, "Jane")
    with pytest.raises(StopAsyncIteration):
        _ = await anext(gen)


# MARK: sql_one_or_none


@dataclass
class UData:
    id: int
    username: str


@nm.sql_one_or_none(UData, "select rowid as id, username from users where rowid=:id")
def get_user_by_id(id_: int):
    return nm.params(id=id_)


async def test_one_or_none(tst_conn: aiosqlite.Connection):
    user_info = await get_user_by_id(tst_conn, 1)
    assert user_info == UData(id=1, username="John")

    user_info = await get_user_by_id(tst_conn, 999)
    assert user_info is None


# MARK: sql_scalar_or_none


@nm.sql_scalar_or_none(int, "select count(*) from users;")
def get_users_count():
    return nm.query_only(None)


@nm.sql_scalar_or_none(date)
def get_users_min_birthday(birthdate_from: date | None = None):
    sql = "select min(birthdate) from users where true --where_cond--"
    if birthdate_from is None:
        return nm.query_only(sql)
    return nm.query_and_params(
        sql.replace("--where_cond--", "and birthdate >= ?"),
        birthdate_from.isoformat(),
    )


@nm.sql_scalar_or_none(int)
def get_random_user_id(id_from: int | None = None):
    sql = """
        select rowid from users
        where true --where_cond--
        order by random()
        limit 1
    """
    if id_from is None:
        return nm.query_and_params(sql)
    return nm.query_and_params(sql.replace("--where_cond--", "and rowid >= ?"), id_from)


async def test_scalar_or_none(tst_conn: aiosqlite.Connection):
    assert (await get_users_count(tst_conn)) == 2
    assert (await get_users_min_birthday(tst_conn)) == date(1982, 4, 27)
    assert (await get_users_min_birthday(tst_conn, date(1982, 4, 28))) is None
    assert (await get_random_user_id(tst_conn)) is not None
    assert (await get_random_user_id(tst_conn, 999)) is None


# MARK: nm.sql_fetch_scalars


@nm.sql_fetch_scalars(int, "select rowid from users;")
def get_user_ids():
    pass


@nm.sql_fetch_scalars(str, "select rowid from users;")
def get_user_ids_str():
    pass


@nm.sql_fetch_scalars(Any, "select rowid from users;")
def get_user_ids_any():
    pass


@nm.sql_fetch_scalars(int, "select rowid from users where username like :search;")
def get_user_ids_search(search: str):
    if not search:
        raise nm.CancelExecException
    return nm.params(search=f"%{search}%")


@nm.sql_fetch_scalars(bytes, "select password_hash from users;")
def get_user_ids_p_hashes():
    pass


async def test_fetch_scalars(tst_conn: aiosqlite.Connection):
    got = await get_user_ids(tst_conn)
    assert got == [1, 2]
    got = await get_user_ids_str(tst_conn)
    assert got == ["1", "2"]
    got = await get_user_ids_any(tst_conn)
    assert got == [1, 2]
    got = await get_user_ids_p_hashes(tst_conn)
    assert got == [sha1(b"123456").digest(), None]
    got = await get_user_ids_search(tst_conn, "j")
    assert got == [1, 2]
    got = await get_user_ids_search(tst_conn, "")
    assert got == []


# MARK: sql_iterate_scalars


@nm.sql_iterate_scalars(str, "select username from users order by rowid")
def iter_usernames():
    pass


async def test_iter_usernames(tst_conn: aiosqlite.Connection):
    gen = iter_usernames(tst_conn)
    assert await anext(gen) == "John"
    assert await anext(gen) == "Jane"
    with pytest.raises(StopAsyncIteration):
        _ = await anext(gen)

    got = []
    async for v in iter_usernames(tst_conn):
        got.append(v)
    assert got == ["John", "Jane"]


# MARK: sql_execute


@nm.sql_execute("insert into users(username, email) values(?, ?)")
def ins_user(username: str | None = None, email: str | None = None):
    return nm.params(username, email)


@nm.sql_execute
def del_user(email: str | None = None):
    return nm.query_and_params("delete from users where email = ?", email)


async def test_ins_user(tst_conn: aiosqlite.Connection):
    assert (await get_users_count(tst_conn)) == 2
    await ins_user(tst_conn, "New", "new@test.com")
    assert (await get_users_count(tst_conn)) == 3
    await del_user(tst_conn, "new@test.com")
    assert (await get_users_count(tst_conn)) == 2
