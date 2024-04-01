from typing import Type, Any, Callable
from dataclasses import is_dataclass, fields
from decimal import Decimal
from datetime import date, datetime
from functools import partial


def _decode_date(val: str | None) -> date | None:
    if val is None:
        return None
    return date.fromisoformat(val)


def _decode_datetime(val: str | None) -> datetime | None:
    if val is None:
        return None
    return datetime.fromisoformat(val)


def _as_is(val: Any) -> Any:
    return val


def _as_is_val(type_: Type, val: Any) -> Any:
    if val is None:
        return None
    return type_(val)


def make_scalar_decoder(type_: Type) -> Callable[[Any], Any]:
    if type_ is Any:
        return _as_is
    for ttype in (int, float, str, bool, Decimal):
        if issubclass(ttype, type_):
            return partial(_as_is_val, ttype)
    if issubclass(date, type_):
        return _decode_date
    if issubclass(datetime, type_):
        return _decode_datetime
    return _as_is


def make_decoder(row_type: type) -> Callable[[dict], dict]:
    if is_dataclass(row_type):
        fields_mapper: dict[str, Callable] = {
            fld.name: make_scalar_decoder(fld.type) for fld in fields(row_type)
        }

        def _dataclass_decoder(inp: dict) -> dict:
            return {k: fields_mapper[k](v) for k, v in inp.items()}

        return _dataclass_decoder

    return _as_is
