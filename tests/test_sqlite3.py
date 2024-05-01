from typing import Any
import sqlite3
from collections import namedtuple
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from hashlib import sha1

import pytest
import noorm.sqlite3 as nm


@pytest.fixture
def tst_conn():
    with sqlite3.connect(":memory:") as conn:
        conn.execute(
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
        conn.execute(
            """
            insert into users(
                username, email, birthdate, salary, last_seen, password_hash
            )
            values('John', 'john@doe.com', NULL, '1234.56',
                '2023-05-14T12:34:56+00:00', ?);
            """,
            (sha1(b"123456").digest(),),
        )
        conn.execute(
            """
            insert into users(username, email, birthdate, salary, last_seen)
            values('Jane', 'jane@doe.com', '1982-04-27', NULL, NULL);
            """
        )
        conn.commit()
        yield conn


# MARK: sql_fetch_all


@nm.sql_fetch_all(
    namedtuple("AllUsersResult", "id,username"),
    "select rowid as id, username from users order by rowid",
)
def get_all_users_namedtuple():
    pass


def test_fetch_all(tst_conn: sqlite3.Connection):
    got = get_all_users_namedtuple(tst_conn)
    assert len(got) == 2
    rtype = type(got[0])
    assert got == [rtype(1, "John"), rtype(2, "Jane")]

    cur = tst_conn.cursor()
    got = get_all_users_namedtuple(cur)
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


def test_fetch_all_wrong(tst_conn: sqlite3.Connection):
    with pytest.raises(TypeError):
        _ = get_all_users_wrong1(tst_conn, 1)
    with pytest.raises(ValueError):
        _ = get_all_users_wrong2(tst_conn, 1)
    with pytest.raises(RuntimeError):
        _ = get_all_users_wrong3(tst_conn, 1)


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


def test_fetch_find(tst_conn: sqlite3.Connection):
    got = find_users_by_text(tst_conn, "doe")
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
    got = find_users_by_text(tst_conn, None)
    assert got == []


# MARK: sql_one_or_none


@dataclass
class UData:
    id: int
    username: str


@nm.sql_one_or_none(UData, "select rowid as id, username from users where rowid=:id")
def get_user_by_id(id_: int):
    return nm.params(id=id_)


def test_one_or_none(tst_conn: sqlite3.Connection):
    user_info = get_user_by_id(tst_conn, 1)
    assert user_info == UData(id=1, username="John")

    user_info = get_user_by_id(tst_conn, 999)
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
        order by random() limit 1
    """
    if id_from is None:
        return nm.query_and_params(sql)
    return nm.query_and_params(sql.replace("--where_cond--", "and rowid >= ?"), id_from)


def test_scalar_or_none(tst_conn: sqlite3.Connection):
    assert get_users_count(tst_conn) == 2
    assert get_users_min_birthday(tst_conn) == date(1982, 4, 27)
    assert get_users_min_birthday(tst_conn, date(1982, 4, 28)) is None
    assert get_random_user_id(tst_conn) is not None
    assert get_random_user_id(tst_conn, 999) is None


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


def test_fetch_scalars(tst_conn: sqlite3.Connection):
    got = get_user_ids(tst_conn)
    assert got == [1, 2]
    got = get_user_ids_str(tst_conn)
    assert got == ["1", "2"]
    got = get_user_ids_any(tst_conn)
    assert got == [1, 2]
    got = get_user_ids_p_hashes(tst_conn)
    assert got == [sha1(b"123456").digest(), None]
    got = get_user_ids_search(tst_conn, "j")
    assert got == [1, 2]
    got = get_user_ids_search(tst_conn, "")
    assert got == []


# MARK: sql_execute


@nm.sql_execute("insert into users(username, email) values(?, ?)")
def ins_user(username: str | None = None, email: str | None = None):
    return nm.params(username, email)


@nm.sql_execute
def del_user(email: str | None = None):
    return nm.query_and_params("delete from users where email = ?", email)


def test_ins_user(tst_conn: sqlite3.Connection):
    assert get_users_count(tst_conn) == 2
    ins_user(tst_conn, "New", "new@test.com")
    assert get_users_count(tst_conn) == 3
    del_user(tst_conn, "new@test.com")
    assert get_users_count(tst_conn) == 2
