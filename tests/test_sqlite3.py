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


# MARK: sql_iterate


@nm.sql_iterate(
    namedtuple("AllUsersResult", "id,username"),
    "select rowid as id, username from users order by rowid",
)
def iter_users():
    pass


def test_iter_users(tst_conn: sqlite3.Connection):
    gen = iter_users(tst_conn)
    first = next(gen)
    rtype = type(first)
    assert first == rtype(1, "John")
    assert next(gen) == rtype(2, "Jane")
    with pytest.raises(StopIteration):
        _ = next(gen)


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


CollParamsRes = namedtuple("CollParamsRes", "s1,i1,s2")


@nm.sql_one_or_none(CollParamsRes)
def get_coll_params_res(sql, params):
    if isinstance(params, dict):
        return nm.query_and_params(sql, **params)
    else:
        return nm.query_and_params(sql, *params)


@pytest.mark.parametrize(
    "sql, params, expected",
    (
        (
            """select '"hi" :ids' as s1, max(rowid) as i1, :pstr as s2
            from users where rowid in (:ids)""",
            {"ids": [1, 2, 100], "pstr": "'; drop table users;"},
            CollParamsRes('"hi" :ids', 2, "'; drop table users;"),
        ),
        (
            """select '-- not a comment' as s1, max(rowid) as i1, '"' as s2
            from users where rowid in (:ids) and rowid <> :idskeep""",
            {"ids": [1, 2, 100], "idskeep": 2},
            CollParamsRes("-- not a comment", 1, '"'),
        ),
        (
            """select "'hi' ?" as s1, max(rowid) as i1, ? as s2 -- works?
            from users where rowid in (?) and ? = 'okey?'""",
            ["'; drop table users;", set([1, 2, 100]), "okey?"],
            CollParamsRes("'hi' ?", 2, "'; drop table users;"),
        ),
        ("select ? as s1, ? as i1, ? as s2", [1, Ellipsis, 1], TypeError),
        ("select ? as s1, ? as i1, ? as s2", [1, ([],), 1], TypeError),
        ("select :par1 as s1, 1 as i1, '-' as s2", {"par1": Ellipsis}, TypeError),
        (
            "select '-' as s1, 1 as i1, '-' as s2 where 1 in (:par1)",
            {"par1": [1, 2, 3]},
            CollParamsRes("-", 1, "-"),
        ),
        (
            "select '-' as s1, 1 as i1, '-' as s2 where 1 in (:par1)",
            {"par1": [1, (2,), 3]},
            TypeError,
        ),
        (
            "select ? as s1, ? as i1, ? as s2",
            (True, date(2024, 1, 2), datetime(2024, 1, 2, 3, 4, 5)),
            CollParamsRes(1, "2024-01-02", "2024-01-02 03:04:05"),
        ),
        (
            "select ? as s1, ? as i1, ? as s2",
            (False, Decimal("1.2345"), None),
            CollParamsRes(0, "1.2345", None),
        ),
    ),
)
def test_collection_params(tst_conn: sqlite3.Connection, sql, params, expected):
    if expected == TypeError:
        with pytest.raises(expected):
            _ = get_coll_params_res(tst_conn, sql, params)
    else:
        got = get_coll_params_res(tst_conn, sql, params)
        assert got == expected


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


# MARK: sql_fetch_scalars


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


# MARK: sql_iterate_scalars


@nm.sql_iterate_scalars(str, "select username from users order by rowid")
def iter_usernames():
    pass


def test_iter_usernames(tst_conn: sqlite3.Connection):
    gen = iter_usernames(tst_conn)
    assert next(gen) == "John"
    assert next(gen) == "Jane"
    with pytest.raises(StopIteration):
        _ = next(gen)
    assert list(iter_usernames(tst_conn)) == ["John", "Jane"]


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


# MARK: default_db


@nm.default_db
@nm.sql_scalar_or_none(int, "select count(*) from users where rowid <= ?")
def get_count_def_db(max_id: int):
    return nm.params(max_id)


def test_def_db(tst_conn: sqlite3.Connection):
    with pytest.raises(RuntimeError):
        a = get_count_def_db(2)
    with nm.set_default_db(tst_conn):
        a = get_count_def_db(2)
        assert a == 2
        with nm.set_default_db(None):
            with pytest.raises(RuntimeError):
                a = get_count_def_db(2)
        a = get_count_def_db(2)
        assert a == 2
    with pytest.raises(RuntimeError):
        a = get_count_def_db(2)


# Conversions


@dataclass
class Point:
    x: float
    y: float

    def __conform__(self, protocol):
        if protocol is sqlite3.PrepareProtocol:
            return f"{self.x};{self.y}"


def convert_point(s):
    coord = map(float, s.split(b";"))
    return Point(*coord)


@dataclass
class ConvRes:
    p: Point
    d1: date
    d2: datetime
    dt1: date
    dt2: datetime


@nm.sql_execute("INSERT INTO test(p, d1, d2, dt1, dt2) VALUES(?, ?, ?, ?, ?)")
def ins_converted(p, d1, d2, dt1, dt2):
    return nm.params(p, d1, d2, dt1, dt2)


@nm.sql_one_or_none(ConvRes, "select * from test limit 1")
def get_converted():
    pass


def test_conversions():
    sqlite3.register_converter("point", convert_point)
    conn = sqlite3.connect(":memory:", detect_types=sqlite3.PARSE_DECLTYPES)
    conn.execute(
        "CREATE TABLE test(p point, d1 date, d2 date, dt1 timestamp, dt2 timestamp)"
    )
    d1, d2 = date(2024, 1, 22), date(2024, 1, 23)
    dt1, dt2 = datetime(2024, 1, 24, 3, 44, 55), datetime(2024, 1, 25, 3, 44, 55)
    ins_converted(conn, Point(1.23, 3.45), d1, d2, dt1, dt2)
    conn.commit()

    got = get_converted(conn)
    assert got == ConvRes(
        p=Point(1.23, 3.45),
        d1=d1,
        d2=datetime(2024, 1, 23, 0, 0),
        dt1=date(2024, 1, 24),
        dt2=dt2,
    )

    with pytest.raises(TypeError):
        ins_converted(conn, UData(1, "."), d1, d2, dt1, dt2)
