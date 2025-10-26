from typing import Generator
from collections import namedtuple

import pytest
import sqlalchemy as sa
from sqlalchemy.orm import declarative_base, Session

import noorm.sqlalchemy_sync as nm


meta = sa.MetaData()
Base = declarative_base(metadata=meta)


class User(Base):
    __tablename__ = "users"

    id = sa.Column(sa.Integer, primary_key=True)
    username = sa.Column(sa.String)
    email = sa.Column(sa.String)


@pytest.fixture
def engine():
    engine = sa.create_engine("sqlite://")
    with engine.begin() as conn:
        meta.create_all(conn)

    with Session(engine) as session:
        session.add_all(
            [
                User(id=1, username="John", email="john@doe.com"),
                User(id=2, username="Jane", email="jane@doe.com"),
            ]
        )
        session.commit()
    return engine


@pytest.fixture
def session(engine: sa.engine.Engine) -> Generator[Session, None, None]:
    with Session(engine) as session:
        yield session


# MARK: sql_fetch_all


@nm.sql_fetch_all(namedtuple("AllUsersResult", "id,username"))
def get_all_users_namedtuple(do_cancel: bool):
    if do_cancel:
        raise nm.CancelExecException
    return sa.select(User.id, User.username).order_by(User.id)


def test_fetch_all(session: Session):
    got = get_all_users_namedtuple(session, False)
    assert len(got) == 2
    q = get_all_users_namedtuple.unwrapped(False)
    assert str(q.compile()).startswith("SELECT users.id,")
    rtype = type(got[0])
    assert got == [rtype(1, "John"), rtype(2, "Jane")]

    got = get_all_users_namedtuple(session, True)
    assert got == []
    with pytest.raises(nm.CancelExecException):
        get_all_users_namedtuple.unwrapped(True)


@nm.sql_fetch_all(namedtuple("AllUsersResult", "id,username"))
def get_all_users_wrong1(id_: int):
    return id_


@nm.sql_fetch_all(namedtuple("AllUsersResult", "id,username"))
def get_all_users_wrong2(id_: int):
    pass


def test_fetch_all_wrong(session: Session):
    with pytest.raises(TypeError):
        _ = get_all_users_wrong1(session, 1)
    with pytest.raises(TypeError):
        _ = get_all_users_wrong2(session, 1)


# MARK: repo style


class Repo:
    def __init__(self, limit=None):
        self.limit = limit

    @nm.sql_fetch_all(namedtuple("AllUsersResult", "id,username"))
    def get_all_users_objmethod(self, do_cancel: bool):
        """Test for object method"""
        if do_cancel:
            raise nm.CancelExecException
        res = sa.select(User.id, User.username).order_by(User.id)
        if self.limit is not None:
            res = res.limit(self.limit)
        return res

    @nm.sql_fetch_all(namedtuple("AllUsersResult", "id,username"))
    @classmethod
    def get_all_users_classmethod(cls, do_cancel: bool):
        """Test for class method"""
        if do_cancel:
            raise nm.CancelExecException
        return sa.select(User.id, User.username).order_by(User.id)

    @nm.sql_fetch_all(namedtuple("AllUsersResult", "id,username"))
    @staticmethod
    def get_all_users_staticmethod(do_cancel: bool):
        """Test for static method"""
        if do_cancel:
            raise nm.CancelExecException
        return sa.select(User.id, User.username).order_by(User.id)


def test_repo_objmethod(session: Session):
    got2 = Repo().get_all_users_objmethod(session, False)
    assert len(got2) == 2
    got1 = Repo(limit=1).get_all_users_objmethod(session, False)
    assert len(got1) == 1
    got0 = Repo().get_all_users_objmethod(session, True)
    assert len(got0) == 0
    assert isinstance(Repo().get_all_users_objmethod.unwrapped(False), sa.sql.Select)
    with pytest.raises(nm.CancelExecException):
        Repo().get_all_users_objmethod.unwrapped(True)


def test_repo_classmethod(session: Session):
    got2 = Repo.get_all_users_classmethod(session, False)
    assert len(got2) == 2
    got0 = Repo.get_all_users_classmethod(session, True)
    assert len(got0) == 0
    assert isinstance(Repo().get_all_users_classmethod.unwrapped(False), sa.sql.Select)
    with pytest.raises(nm.CancelExecException):
        Repo().get_all_users_classmethod.unwrapped(True)


def test_repo_staticmethod(session: Session):
    got2 = Repo.get_all_users_staticmethod(session, False)
    assert len(got2) == 2
    got0 = Repo.get_all_users_staticmethod(session, True)
    assert len(got0) == 0
    assert isinstance(Repo().get_all_users_staticmethod.unwrapped(False), sa.sql.Select)
    with pytest.raises(nm.CancelExecException):
        Repo().get_all_users_staticmethod.unwrapped(True)


# MARK: sql_iterate


@nm.sql_iterate(namedtuple("AllUsersResult", "id,username"))
def iter_users():
    return sa.select(User.id, User.username).order_by(User.id)


def test_iter_users(session: Session):
    gen = iter_users(session)
    first = next(gen)
    rtype = type(first)
    assert first == rtype(1, "John")
    assert next(gen) == rtype(2, "Jane")
    with pytest.raises(StopIteration):
        _ = next(gen)


# MARK: sql_one_or_none


@nm.sql_one_or_none(namedtuple("UsersTabInfo", "cnt, max_id"))
def get_users_tab_info():
    return sa.select(sa.func.count().label("cnt"), sa.func.max(User.id).label("max_id"))


def test_get_users_tab_info(session: Session):
    got = get_users_tab_info(session)
    assert got is not None
    assert got.cnt == 2
    assert got.max_id == 2


@nm.sql_one_or_none(namedtuple("UserInfo", "id, username, email"))
def get_user_info(user_id: int | None):
    if user_id is None:
        raise nm.CancelExecException
    return sa.select(User.id, User.username, User.email).filter(User.id == user_id)


def test_get_user_info(session: Session):
    got = get_user_info(session, 1)
    assert got is not None
    assert got.id == 1
    assert got.username == "John"
    assert got.email == "john@doe.com"
    got = get_user_info(session, 3)
    assert got is None
    got = get_user_info(session, None)
    assert got is None


# MARK: sql_scalar_or_none


@nm.sql_scalar_or_none(int)
def get_users_count():
    return sa.select(sa.func.count()).select_from(User)


def test_get_users_count(session: Session):
    got = get_users_count(session)
    assert got == 2


@nm.sql_scalar_or_none(str)
def get_user_name(user_id: int | None):
    if user_id is None:
        raise nm.CancelExecException
    return sa.select(User.username).filter(User.id == user_id)


def test_get_user_name(session: Session):
    got = get_user_name(session, 2)
    assert got == "Jane"
    got = get_user_name(session, None)
    assert got is None


# MARK: sql_fetch_scalars


@nm.sql_fetch_scalars(int)
def get_user_ids(do_cancel: bool):
    if do_cancel:
        raise nm.CancelExecException
    return sa.select(User.id)


def test_fetch_scalars(session: Session):
    got = get_user_ids(session, False)
    assert got == [1, 2]
    got = get_user_ids(session, True)
    assert got == []


# MARK: sql_iterate_scalars


@nm.sql_iterate_scalars(str)
def iter_usernames():
    return sa.select(User.username).order_by(User.id)


def test_iter_usernames(session: Session):
    gen = iter_usernames(session)
    assert next(gen) == "John"
    assert next(gen) == "Jane"
    with pytest.raises(StopIteration):
        _ = next(gen)
    assert list(iter_usernames(session)) == ["John", "Jane"]


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


@nm.sql_fetch_scalars(int)
def get_nums():
    return sa.union(sa.select(sa.literal(1)), sa.select(sa.literal(2)))


@pytest.mark.parametrize("no_commit", (False, True))
def test_delete_user(session: Session, no_commit: bool):
    assert get_users_count(session) == 2
    rename_user(session, 1, "Mr. John")
    session.rollback()  # no effect because already commited
    assert get_user_name(session, 1) == "Mr. John"

    if no_commit:
        delete_user_no_commit(session, 1)
    else:
        delete_user(session, 1)
    assert get_users_count(session) == 1  # John is gone in this session

    # Call to get_nums() should not cause commit
    assert get_nums(session) == [1, 2]

    session.rollback()  # if no_commit == True, John is back
    assert get_users_count(session) == (2 if no_commit else 1)

    delete_all_users(session)
    assert get_users_count(session) == 0
