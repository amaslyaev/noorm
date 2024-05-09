import sqlalchemy as sa

import noorm.sqlalchemy_sync as nm


@nm.sql_scalar_or_none(bool)
def some_func():
    return sa.select(sa.literal(True))


class SomeClass:
    @nm.sql_scalar_or_none(str)
    def say_hello():
        return sa.select(sa.literal("Hello"))
