from typing import Any
from collections import namedtuple
from dataclasses import dataclass
import json
from unittest.mock import Mock, call

import pytest

import noorm.psycopg2 as nm


class MockConn(Mock):
    _res = []
    _idx = 0
    description = []

    def cursor(self):
        return self

    def execute(self, res_json, *args, **kwargs):
        self._res = json.loads(res_json)
        if self._res and isinstance(self._res[0], dict):
            self.description = [(k,) for k in self._res[0].keys()]
        if args:
            self.execute_call(res_json, args[0])
        elif kwargs:
            self.execute_call(res_json, kwargs)
        else:
            self.execute_call(res_json, tuple())

    def __iter__(self):
        self._idx = 0
        return self

    def __next__(self):
        if not self._res or self._idx >= len(self._res):
            raise StopIteration
        res = self._res[self._idx]
        self._idx += 1
        return tuple(res.values())

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


@pytest.fixture
def tst_conn():
    return MockConn()


get_all_users_fake_sql = (
    '[{"id": 1, "username": "John"}, {"id": 2, "username": "Jane"}]'
)


# ---------------------- sql_fetch_all ----------------------


@nm.sql_fetch_all(namedtuple("AllUsersResult", "id,username"), get_all_users_fake_sql)
def get_all_users_namedtuple():
    pass


def test_fetch_all(tst_conn: Mock):
    got = get_all_users_namedtuple(tst_conn)
    assert len(got) == 2
    rtype = type(got[0])
    assert got == [rtype(1, "John"), rtype(2, "Jane")]
    assert tst_conn.execute_call.call_args == call(get_all_users_fake_sql, tuple())


@nm.sql_fetch_all(namedtuple("AllUsersResult", "id,username"), get_all_users_fake_sql)
def get_all_users_with_params(case: int):
    if case == 0:
        return "haha"
    elif case == 1:
        return nm.params()
    elif case == 2:
        return nm.params(11)
    elif case == 3:
        return nm.params(11, 22)
    elif case == 4:
        return nm.query_only(None)
    elif case == 5:
        return nm.query_only(get_all_users_fake_sql)
    elif case == 6:
        return nm.query_and_params(None, 11)
    elif case == 7:
        return nm.query_and_params(get_all_users_fake_sql, 11, 22)


def test_fetch_with_params(tst_conn: Mock):
    with pytest.raises(TypeError):
        _ = get_all_users_with_params(tst_conn, 0)

    get_all_users_with_params(tst_conn, 1)
    assert tst_conn.execute_call.call_args == call(get_all_users_fake_sql, tuple())
    get_all_users_with_params(tst_conn, 2)
    assert tst_conn.execute_call.call_args == call(get_all_users_fake_sql, (11,))
    get_all_users_with_params(tst_conn, 3)
    assert tst_conn.execute_call.call_args == call(get_all_users_fake_sql, (11, 22))
    get_all_users_with_params(tst_conn, 4)
    assert tst_conn.execute_call.call_args == call(get_all_users_fake_sql, tuple())
    get_all_users_with_params(tst_conn, 5)
    assert tst_conn.execute_call.call_args == call(get_all_users_fake_sql, tuple())
    get_all_users_with_params(tst_conn, 6)
    assert tst_conn.execute_call.call_args == call(get_all_users_fake_sql, (11,))
    get_all_users_with_params(tst_conn, 7)
    assert tst_conn.execute_call.call_args == call(get_all_users_fake_sql, (11, 22))


@nm.sql_fetch_all(namedtuple("AllUsersResult", "id,username"))
def get_all_users_wrong3(id_: int):
    return nm.params(id=id_)


def test_fetch_all_wrong(tst_conn: Mock):
    with pytest.raises(RuntimeError):
        _ = get_all_users_wrong3(tst_conn, 1)


# ---------------------- sql_one_or_none ----------------------


@dataclass
class UData:
    id: int
    username: str


get_one_user_fake_sql = '[{"id": 1, "username": "John"}]'


@nm.sql_one_or_none(UData, get_one_user_fake_sql)
def get_user_by_id(id_: int):
    return nm.params(id_)


@nm.sql_one_or_none(UData, "null")
def get_no_users_by_id(id_: int):
    return nm.params(id_)


def test_one_or_none(tst_conn: Mock):
    got = get_user_by_id(tst_conn, 1)
    assert got == UData(id=1, username="John")
    assert tst_conn.execute_call.call_args == call(get_one_user_fake_sql, (1,))

    got = get_no_users_by_id(tst_conn, 999)
    assert got is None
    assert tst_conn.execute_call.call_args == call("null", (999,))


# ---------------------- sql_scalar_or_none ----------------------


@nm.sql_scalar_or_none(int, "-")
def get_scalar(qresult_json: str):
    return nm.query_only(qresult_json)


@nm.sql_scalar_or_none(Any, "-")
def get_any_scalar(qresult_json: str):
    return nm.query_only(qresult_json)


def test_scalar_or_none(tst_conn: Mock):
    got = get_scalar(tst_conn, '[{"v": 123}]')
    assert got == 123
    got = get_scalar(tst_conn, "[]")
    assert got is None
    got = get_any_scalar(tst_conn, '[{"v": 123}]')
    assert got == 123
    got = get_any_scalar(tst_conn, "[]")
    assert got is None
    got = get_any_scalar(tst_conn, '[{"v": "do not fail here"}]')
    assert got == "do not fail here"


# ---------------------- sql_fetch_scalars ----------------------


@nm.sql_fetch_scalars(int, get_all_users_fake_sql)
def get_user_ids():
    pass


def test_fetch_scalars(tst_conn: Mock):
    got = get_user_ids(tst_conn)
    assert got == [1, 2]


# ---------------------- sql_execute ----------------------


@nm.sql_execute("null")
def do_something1():
    return nm.params(12, "34")


@nm.sql_execute
def do_something2():
    return nm.query_and_params("null", 23, "45")


def test_execute(tst_conn: Mock):
    do_something1(tst_conn)
    assert tst_conn.execute_call.call_args == call("null", (12, "34"))
    do_something2(tst_conn)
    assert tst_conn.execute_call.call_args == call("null", (23, "45"))
