from typing import AsyncGenerator
from collections import namedtuple

import pytest
import sqlalchemy as sa
from sqlalchemy.orm import declarative_base
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, AsyncEngine

import noorm.sqlalchemy_async as nm


meta = sa.MetaData()
Base = declarative_base(metadata=meta)


class User(Base):
    __tablename__ = "users"

    id = sa.Column(sa.Integer, primary_key=True)
    username = sa.Column(sa.String)
    email = sa.Column(sa.String)


@pytest.fixture
async def engine():
    engine = create_async_engine("sqlite+aiosqlite://")
    async with engine.begin() as conn:
        await conn.run_sync(meta.create_all)

    async with AsyncSession(engine) as session:
        session.add_all(
            [
                User(id=1, username="John", email="john@doe.com"),
                User(id=2, username="Jane", email="jane@doe.com"),
            ]
        )
        await session.commit()
    return engine


@pytest.fixture
async def session(engine: AsyncEngine) -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSession(engine) as session:
        yield session


# MARK: sql_fetch_all


@nm.sql_fetch_all(namedtuple("AllUsersResult", "id,username"))
def get_all_users_namedtuple(do_cancel: bool):
    if do_cancel:
        raise nm.CancelExecException
    return sa.select(User.id, User.username).order_by(User.id)


async def test_fetch_all(session: AsyncSession):
    got = await get_all_users_namedtuple(session, False)
    assert len(got) == 2
    rtype = type(got[0])
    assert got == [rtype(1, "John"), rtype(2, "Jane")]

    got = await get_all_users_namedtuple(session, True)
    assert got == []


@nm.sql_fetch_all(namedtuple("AllUsersResult", "id,username"))
def get_all_users_wrong1(id_: int):
    return id_


@nm.sql_fetch_all(namedtuple("AllUsersResult", "id,username"))
def get_all_users_wrong2(id_: int):
    pass


async def test_fetch_all_wrong(session: AsyncSession):
    with pytest.raises(TypeError):
        _ = await get_all_users_wrong1(session, 1)
    with pytest.raises(TypeError):
        _ = await get_all_users_wrong2(session, 1)


# MARK: sql_iterate


@nm.sql_iterate(namedtuple("AllUsersResult", "id,username"))
def iter_users():
    return sa.select(User.id, User.username).order_by(User.id)


async def test_iter_users(session: AsyncSession):
    gen = iter_users(session)
    first = await anext(gen)
    rtype = type(first)
    assert first == rtype(1, "John")
    assert await anext(gen) == rtype(2, "Jane")
    with pytest.raises(StopAsyncIteration):
        _ = await anext(gen)


# MARK: sql_one_or_none


@nm.sql_one_or_none(namedtuple("UsersTabInfo", "cnt, max_id"))
def get_users_tab_info():
    return sa.select(sa.func.count().label("cnt"), sa.func.max(User.id).label("max_id"))


async def test_get_users_tab_info(session: AsyncSession):
    got = await get_users_tab_info(session)
    assert got is not None
    assert got.cnt == 2
    assert got.max_id == 2


@nm.sql_one_or_none(namedtuple("UserInfo", "id, username, email"))
def get_user_info(user_id: int | None):
    if user_id is None:
        raise nm.CancelExecException
    return sa.select(User.id, User.username, User.email).filter(User.id == user_id)


async def test_get_user_info(session: AsyncSession):
    got = await get_user_info(session, 1)
    assert got is not None
    assert got.id == 1
    assert got.username == "John"
    assert got.email == "john@doe.com"
    got = await get_user_info(session, 3)
    assert got is None
    got = await get_user_info(session, None)
    assert got is None


# MARK: sql_scalar_or_none


@nm.sql_scalar_or_none(int)
def get_users_count():
    return sa.select(sa.func.count()).select_from(User)


async def test_get_users_count(session: AsyncSession):
    got = await get_users_count(session)
    assert got == 2


@nm.sql_scalar_or_none(str)
def get_user_name(user_id: int | None):
    if user_id is None:
        raise nm.CancelExecException
    return sa.select(User.username).filter(User.id == user_id)


async def test_get_user_name(session: AsyncSession):
    got = await get_user_name(session, 2)
    assert got == "Jane"
    got = await get_user_name(session, None)
    assert got is None


# MARK: sql_fetch_scalars


@nm.sql_fetch_scalars(int)
def get_user_ids(do_cancel: bool):
    if do_cancel:
        raise nm.CancelExecException
    return sa.select(User.id)


async def test_fetch_scalars(session: AsyncSession):
    got = await get_user_ids(session, False)
    assert got == [1, 2]
    got = await get_user_ids(session, True)
    assert got == []


# MARK: sql_iterate_scalars


@nm.sql_iterate_scalars(str, "select username from users order by rowid")
def iter_usernames():
    return sa.select(User.username).order_by(User.id)


async def test_iter_usernames(session: AsyncSession):
    gen = iter_usernames(session)
    assert await anext(gen) == "John"
    assert await anext(gen) == "Jane"
    with pytest.raises(StopAsyncIteration):
        _ = await anext(gen)

    got = []
    async for v in iter_usernames(session):
        got.append(v)
    assert got == ["John", "Jane"]


# MARK: sql_execute


@nm.sql_execute  # decorator without parameters
def rename_user(user_id: int, new_username: str | None):
    return sa.update(User).values(username=new_username).where(User.id == user_id)


@nm.sql_execute()
def delete_user(user_id: int):
    return sa.delete(User).where(User.id == user_id)


@nm.sql_execute(no_commit=True)
def delete_user_no_commit(user_id: int):
    return sa.delete(User).where(User.id == user_id)


@nm.sql_execute(no_commit=True)
def delete_all_users():
    return sa.delete(User)


@pytest.mark.parametrize("no_commit", (False, True))
async def test_delete_user(session: AsyncSession, no_commit: bool):
    assert (await get_users_count(session)) == 2
    await rename_user(session, 1, "Mr. John")
    await session.rollback()  # no effect because already commited
    assert (await get_user_name(session, 1)) == "Mr. John"

    if no_commit:
        await delete_user_no_commit(session, 1)
    else:
        await delete_user(session, 1)
    assert (await get_users_count(session)) == 1  # John is gone in this session

    await session.rollback()  # if no_commit == True, John is back
    assert (await get_users_count(session)) == (2 if no_commit else 1)

    await delete_all_users(session)
    assert (await get_users_count(session)) == 0
