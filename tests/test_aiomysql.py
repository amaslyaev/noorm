from collections import namedtuple
from dataclasses import dataclass
import json
from unittest.mock import Mock, call

import pytest

import noorm.aiomysql as nm


class MockConn:
    _res: list[dict] = []
    _idx = 0
    description: list[tuple[str, ...]] = []
    exec_call_args = None

    def __init__(self) -> None:
        self.mock_exec = Mock()

    def cursor(self):
        return self

    async def execute(self, res_json, *args, **kwargs):
        self._res = json.loads(res_json)
        if self._res and isinstance(self._res[0], dict):
            self.description = [(k,) for k in self._res[0].keys()]
        if args:
            self.mock_exec.execute(res_json, args[0])
        elif kwargs:
            self.mock_exec.execute(res_json, kwargs)
        else:
            self.mock_exec.execute(res_json)

    async def fetchone(self):
        return self._res and tuple(self._res[0].values())

    def __aiter__(self):
        self._idx = 0
        return self

    async def __anext__(self):
        if not self._res or self._idx >= len(self._res):
            raise StopAsyncIteration
        res = self._res[self._idx]
        self._idx += 1
        return tuple(res.values())

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


@pytest.fixture
async def tst_conn():
    return MockConn()


get_all_users_fake_sql = (
    '[{"id": 1, "username": "John"}, {"id": 2, "username": "Jane"}]'
)


# MARK: sql_fetch_all


@nm.sql_fetch_all(namedtuple("AllUsersResult", "id,username"), get_all_users_fake_sql)
def get_all_users_namedtuple(do_cancel: bool):
    if do_cancel:
        raise nm.CancelExecException


async def test_fetch_all(tst_conn: MockConn):
    got = await get_all_users_namedtuple(tst_conn, False)
    assert len(got) == 2
    rtype = type(got[0])
    assert got == [rtype(1, "John"), rtype(2, "Jane")]
    assert tst_conn.mock_exec.execute.call_args == call(get_all_users_fake_sql, tuple())


async def test_fetch_all_cancel(tst_conn: MockConn):
    got = await get_all_users_namedtuple(tst_conn, True)
    assert got == []
    assert tst_conn.mock_exec.execute.call_count == 0


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


async def test_fetch_with_params(tst_conn: MockConn):
    with pytest.raises(TypeError):
        _ = await get_all_users_with_params(tst_conn, 0)

    await get_all_users_with_params(tst_conn, 1)
    assert tst_conn.mock_exec.execute.call_args == call(get_all_users_fake_sql, tuple())
    await get_all_users_with_params(tst_conn, 2)
    assert tst_conn.mock_exec.execute.call_args == call(get_all_users_fake_sql, (11,))
    await get_all_users_with_params(tst_conn, 3)
    assert tst_conn.mock_exec.execute.call_args == call(
        get_all_users_fake_sql, (11, 22)
    )
    await get_all_users_with_params(tst_conn, 4)
    assert tst_conn.mock_exec.execute.call_args == call(get_all_users_fake_sql, tuple())
    await get_all_users_with_params(tst_conn, 5)
    assert tst_conn.mock_exec.execute.call_args == call(get_all_users_fake_sql, tuple())
    await get_all_users_with_params(tst_conn, 6)
    assert tst_conn.mock_exec.execute.call_args == call(get_all_users_fake_sql, (11,))
    await get_all_users_with_params(tst_conn, 7)
    assert tst_conn.mock_exec.execute.call_args == call(
        get_all_users_fake_sql, (11, 22)
    )


@nm.sql_fetch_all(namedtuple("AllUsersResult", "id,username"))
def get_all_users_wrong3(id_: int):
    return nm.params(id_)


async def test_fetch_all_wrong(tst_conn: MockConn):
    with pytest.raises(RuntimeError):
        _ = await get_all_users_wrong3(tst_conn, 1)


# MARK: sql_iterate


@nm.sql_iterate(namedtuple("AllUsersResult", "id,username"), get_all_users_fake_sql)
def iter_users():
    pass


async def test_iter_users(tst_conn: MockConn):
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


get_one_user_fake_sql = '[{"id": 1, "username": "John"}]'


@nm.sql_one_or_none(UData, get_one_user_fake_sql)
def get_user_by_id(id_: int):
    return nm.params(id_)


@nm.sql_one_or_none(UData, "null")
def get_no_users_by_id(id_: int):
    return nm.params(id_)


async def test_one_or_none(tst_conn: MockConn):
    got = await get_user_by_id(tst_conn, 1)
    assert got == UData(id=1, username="John")
    assert tst_conn.mock_exec.execute.call_args == call(get_one_user_fake_sql, (1,))

    got = await get_no_users_by_id(tst_conn, 999)
    assert got is None
    assert tst_conn.mock_exec.execute.call_args == call("null", (999,))


# MARK: sql_scalar_or_none


@nm.sql_scalar_or_none(int, "-")
def get_scalar(qresult_json: str):
    return nm.query_only(qresult_json)


async def test_scalar_or_none(tst_conn: MockConn):
    got = await get_scalar(tst_conn, '[{"a":123}]')
    assert got == 123
    got = await get_scalar(tst_conn, "null")
    assert got is None


# MARK: sql_fetch_scalars

get_all_users_fake_sql_tuples = '[{"i":1, "n":"John"}, {"i":2, "n":"Jane"}]'


@nm.sql_fetch_scalars(int, get_all_users_fake_sql_tuples)
def get_user_ids(do_cancel: bool):
    if do_cancel:
        raise nm.CancelExecException


async def test_fetch_scalars(tst_conn: MockConn):
    got = await get_user_ids(tst_conn, False)
    assert got == [1, 2]
    got = await get_user_ids(tst_conn, True)
    assert got == []


# MARK: sql_iterate_scalars


@nm.sql_iterate_scalars(str, get_all_users_fake_sql)
def iter_user_ids():
    pass


async def test_iter_usernames(tst_conn: MockConn):
    gen = iter_user_ids(tst_conn)
    assert await anext(gen) == 1
    assert await anext(gen) == 2
    with pytest.raises(StopAsyncIteration):
        _ = await anext(gen)

    got = []
    async for v in iter_user_ids(tst_conn):
        got.append(v)
    assert got == [1, 2]


# MARK: sql_execute


@nm.sql_execute("null")
def do_something():
    return nm.params(12, "34")


@nm.sql_execute
def do_something2():
    return nm.query_and_params("null", 23, "45")


async def test_execute(tst_conn: MockConn):
    await do_something(tst_conn)
    assert tst_conn.mock_exec.execute.call_args == call("null", (12, "34"))
    await do_something2(tst_conn)
    assert tst_conn.mock_exec.execute.call_args == call("null", (23, "45"))
