from collections import namedtuple
from dataclasses import dataclass
import json
from unittest.mock import AsyncMock, call

import pytest

import noorm.asyncpg as nm
from noorm._common import PARAMS_APPLY_NAMED


@pytest.fixture
async def tst_conn():
    res = AsyncMock()

    async def decode_json(res_json, *params):
        return json.loads(res_json)

    res.fetch.side_effect = decode_json
    res.fetchrow.side_effect = decode_json
    res.fetchval.side_effect = decode_json

    return res


get_all_users_fake_sql = (
    '[{"id": 1, "username": "John"}, {"id": 2, "username": "Jane"}]'
)


# MARK: sql_fetch_all


@nm.sql_fetch_all(namedtuple("AllUsersResult", "id,username"), get_all_users_fake_sql)
def get_all_users_namedtuple(do_cancel: bool):
    if do_cancel:
        raise nm.CancelExecException


async def test_fetch_all(tst_conn: AsyncMock):
    got = await get_all_users_namedtuple(tst_conn, False)
    assert len(got) == 2
    rtype = type(got[0])
    assert got == [rtype(1, "John"), rtype(2, "Jane")]
    assert tst_conn.fetch.call_args == call(get_all_users_fake_sql)


async def test_fetch_all_cancel(tst_conn: AsyncMock):
    got = await get_all_users_namedtuple(tst_conn, True)
    assert got == []
    assert tst_conn.fetch.call_count == 0


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


async def test_fetch_with_params(tst_conn: AsyncMock):
    with pytest.raises(TypeError):
        _ = await get_all_users_with_params(tst_conn, 0)

    await get_all_users_with_params(tst_conn, 1)
    assert tst_conn.fetch.call_args == call(get_all_users_fake_sql)
    await get_all_users_with_params(tst_conn, 2)
    assert tst_conn.fetch.call_args == call(get_all_users_fake_sql, 11)
    await get_all_users_with_params(tst_conn, 3)
    assert tst_conn.fetch.call_args == call(get_all_users_fake_sql, 11, 22)
    await get_all_users_with_params(tst_conn, 4)
    assert tst_conn.fetch.call_args == call(get_all_users_fake_sql)
    await get_all_users_with_params(tst_conn, 5)
    assert tst_conn.fetch.call_args == call(get_all_users_fake_sql)
    await get_all_users_with_params(tst_conn, 6)
    assert tst_conn.fetch.call_args == call(get_all_users_fake_sql, 11)
    await get_all_users_with_params(tst_conn, 7)
    assert tst_conn.fetch.call_args == call(get_all_users_fake_sql, 11, 22)


@nm.sql_fetch_all(namedtuple("AllUsersResult", "id,username"))
def get_all_users_wrong3(id_: int):
    return nm.params(id_)


async def test_fetch_all_wrong(tst_conn: AsyncMock):
    with pytest.raises(RuntimeError):
        _ = await get_all_users_wrong3(tst_conn, 1)


@nm.sql_fetch_all(namedtuple("AllUsersResult", "id,username"), get_all_users_fake_sql)
def get_user_by_named_params(username: str, min_salary: int = 0):
    return PARAMS_APPLY_NAMED


@nm.sql_fetch_all(namedtuple("AllUsersResult", "id,username"), get_all_users_fake_sql)
def get_user_by_pos_params(username: str, min_salary: int = 0):
    return nm.PARAMS_APPLY_POSITIONAL


async def test_fetch_apply_params(tst_conn: AsyncMock):
    with pytest.raises(ValueError):
        await get_user_by_named_params(tst_conn, "John", 100)

    await get_user_by_pos_params(tst_conn, min_salary=100, username="John")
    assert tst_conn.fetch.call_args == call(get_all_users_fake_sql, "John", 100)

    await get_user_by_pos_params(tst_conn, "John")
    assert tst_conn.fetch.call_args == call(get_all_users_fake_sql, "John", 0)

    await get_user_by_pos_params(tst_conn, "John", min_salary=100)
    assert tst_conn.fetch.call_args == call(get_all_users_fake_sql, "John", 100)


# MARK: sql_one_or_none


@dataclass
class UData:
    id: int
    username: str


get_one_user_fake_sql = '{"id": 1, "username": "John"}'


@nm.sql_one_or_none(UData, get_one_user_fake_sql)
def get_user_by_id(id_: int | None):
    if id_ is None:
        raise nm.CancelExecException
    return nm.params(id_)


@nm.sql_one_or_none(UData, "null")
def get_no_users_by_id(id_: int):
    return nm.params(id_)


async def test_one_or_none(tst_conn: AsyncMock):
    got = await get_user_by_id(tst_conn, 1)
    assert got == UData(id=1, username="John")
    assert tst_conn.fetchrow.call_args == call(get_one_user_fake_sql, 1)

    got = await get_no_users_by_id(tst_conn, 999)
    assert got is None
    assert tst_conn.fetchrow.call_args == call("null", 999)


async def test_one_or_none_cancel(tst_conn: AsyncMock):
    got = await get_user_by_id(tst_conn, None)
    assert got is None
    assert tst_conn.fetchrow.call_count == 0


# MARK: sql_scalar_or_none


@nm.sql_scalar_or_none(int, "-")
def get_scalar(qresult_json: str, do_cancel: bool):
    if do_cancel:
        raise nm.CancelExecException
    return nm.query_only(qresult_json)


async def test_scalar_or_none(tst_conn: AsyncMock):
    got = await get_scalar(tst_conn, "123", False)
    assert got == 123
    got = await get_scalar(tst_conn, "null", False)
    assert got is None
    got = await get_scalar(tst_conn, "123", True)
    assert got is None


# MARK: sql_fetch_scalars

get_all_users_fake_sql_tuples = '[[1, "John"], [2, "Jane"]]'


@nm.sql_fetch_scalars(int, get_all_users_fake_sql_tuples)
def get_user_ids(do_cancel: bool):
    if do_cancel:
        raise nm.CancelExecException


async def test_fetch_scalars(tst_conn: AsyncMock):
    got = await get_user_ids(tst_conn, False)
    assert got == [1, 2]
    got = await get_user_ids(tst_conn, True)
    assert got == []


# MARK: sql_execute


@nm.sql_execute("-")
def do_something():
    return nm.params(12, "34")


@nm.sql_execute
def do_something2():
    return nm.query_and_params("null", 23, "45")


async def test_execute(tst_conn: AsyncMock):
    await do_something(tst_conn)
    assert tst_conn.execute.call_args == call("-", 12, "34")
    await do_something2(tst_conn)
    assert tst_conn.execute.call_args == call("null", 23, "45")
