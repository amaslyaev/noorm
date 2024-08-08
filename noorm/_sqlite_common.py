from typing import Type, Any, Callable, Collection, Mapping
from dataclasses import is_dataclass, fields
from decimal import Decimal
from datetime import date, datetime
from functools import partial
import re

from ._db_api_2 import req_sql_n_params


def _decode_date(val: str | date | datetime | None) -> date | None:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    return date.fromisoformat(val)


def _decode_datetime(val: str | date | datetime | None) -> datetime | None:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    if isinstance(val, date):
        return datetime(val.year, val.month, val.day)
    return datetime.fromisoformat(val)


def _as_is(val: Any) -> Any:
    return val


def _as_is_val(type_: Type, val: Any) -> Any:
    if val is None:
        return None
    if isinstance(val, type_):
        return val
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


def _encode_val(val: Any) -> Any:
    if isinstance(val, bool):
        return int(val)
    elif isinstance(val, int | str | None | float | bytes | bytearray):
        return val
    elif isinstance(val, datetime):
        return val.isoformat(sep=" ")
    elif isinstance(val, date):
        return val.isoformat()
    elif isinstance(val, Decimal):
        return str(val)
    elif getattr(type(val), "__conform__", None) is not None:
        return val
    return ...


_SQL_SPLITTER = re.compile(r"'.*?'|\".*?\"|--.*?\n")


def _propagate_named_params(list_param_sizes: dict[str, int], code: str) -> str:
    for pname, size in list_param_sizes.items():
        code_parts: list[str] = []
        pname_len = len(pname)
        cleaned_pname = pname[1:]
        while True:
            pos = code.find(pname)
            if pos == -1:
                code = "".join(code_parts) + code
                break
            if pos > 0:
                code_parts.append(code[:pos])
                code = code[pos:]
            next_char = code[pname_len : pname_len + 1]
            if next_char and next_char.isalnum() or next_char == "_":
                code_parts.append(code[:pname_len])
            else:
                code_parts.append(
                    ", ".join(f":__{cleaned_pname}_{i}" for i in range(size))
                )
            code = code[pname_len:]
    return code


def _propagate_positional_params(
    list_param_sizes: dict[int, int], next_idx_var: list[int], code: str
) -> str:
    split_code = code.split("?")
    code_parts: list[str] = []
    for qmark_idx in range(len(split_code) - 1):
        code_parts.append(split_code[qmark_idx])
        if next_idx_var[0] in list_param_sizes:
            code_parts.append(
                ", ".join("?" for _ in range(list_param_sizes[next_idx_var[0]]))
            )
        else:
            code_parts.append("?")
        next_idx_var[0] += 1
    return "".join(code_parts) + split_code[-1]


def _adjust_sql(sql: str, propagate_func: Callable) -> str:
    res_sqls = []
    last_pos = 0
    for mtch in _SQL_SPLITTER.finditer(sql):
        span = mtch.span()
        code = propagate_func(sql[last_pos : span[0]])
        res_sqls.append(code)
        res_sqls.append(sql[span[0] : span[1]])
        last_pos = span[1]
    if last_pos < len(sql):
        code = propagate_func(sql[last_pos:])
        res_sqls.append(code)
    return "".join(res_sqls)


def _prepare_named_params(sql: str, params: dict) -> tuple[str, dict]:
    list_param_sizes: dict[str, int] = {}
    res_params = {}
    for pname, pvalue in params.items():
        val = _encode_val(pvalue)
        if val is not Ellipsis:
            res_params[pname] = val
        else:
            if isinstance(pvalue, Collection) and not isinstance(pvalue, Mapping):
                # Propagate params
                list_param_sizes[":" + pname] = len(pvalue)
                for idx, pval in enumerate(pvalue):
                    val = _encode_val(pval)
                    if val is Ellipsis:
                        raise TypeError(
                            f"Wrong type of parameter {pname}[{idx}]: "
                            f"{type(pval).__name__}"
                        )
                    else:
                        res_params[f"__{pname}_{idx}"] = val
            else:
                raise TypeError(
                    f"Wrong type of parameter {pname}: {type(pvalue).__name__}"
                )
    if not list_param_sizes:
        res_sql = sql
    else:
        res_sql = _adjust_sql(sql, partial(_propagate_named_params, list_param_sizes))
    return res_sql, res_params


def _prepare_positional_params(sql: str, params: tuple) -> tuple[str, tuple]:
    list_param_sizes: dict[int, int] = {}
    res_params = []
    for pidx, pvalue in enumerate(params):
        val = _encode_val(pvalue)
        if val is not Ellipsis:
            res_params.append(val)
        else:
            if isinstance(pvalue, Collection) and not isinstance(pvalue, Mapping):
                # Propagate params
                list_param_sizes[pidx] = len(pvalue)
                for idx, pval in enumerate(pvalue):
                    val = _encode_val(pval)
                    if val is Ellipsis:
                        raise TypeError(
                            f"Wrong type of parameter {pidx}[{idx}]: "
                            f"{type(pval).__name__}"
                        )
                    else:
                        res_params.append(val)
            else:
                raise TypeError(
                    f"Wrong type of parameter {pidx}: {type(pvalue).__name__}"
                )
    if not list_param_sizes:
        res_sql = sql
    else:
        next_idx_var = [0]
        res_sql = _adjust_sql(
            sql, partial(_propagate_positional_params, list_param_sizes, next_idx_var)
        )

    return res_sql, tuple(res_params)


def sqlite_sql_n_params(
    func, f_args, f_kwargs, default_sql: str | None
) -> tuple[str, dict | tuple] | None:
    sql_n_params = req_sql_n_params(func, f_args, f_kwargs, default_sql)
    if sql_n_params is None:
        return None
    sql, params = sql_n_params
    if isinstance(params, dict):
        sql, params = _prepare_named_params(sql, params)
    else:
        sql, params = _prepare_positional_params(sql, params)
    return sql, params
