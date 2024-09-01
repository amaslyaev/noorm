from enum import Enum
from functools import lru_cache
import inspect

from .registry import get_registry


class CancelExecException(Exception):
    """Do not execute any query and respond empty result"""


class ParamsAutoEnum(Enum):
    """
    When DB-API function returns one of these values the function argumets will be
    automatically passed to the query as accordinly positional or named parameters.
    """

    PARAMS_APPLY_POSITIONAL = 1
    PARAMS_APPLY_NAMED = 2


PARAMS_APPLY_POSITIONAL = ParamsAutoEnum.PARAMS_APPLY_POSITIONAL
PARAMS_APPLY_NAMED = ParamsAutoEnum.PARAMS_APPLY_NAMED


class WrapperBase:
    def __init__(self, func) -> None:
        self._func = func
        get_registry().register(func)

    def unwrapped(self, *args, **kwargs):
        return self._func(*args, **kwargs)


@lru_cache
def _func_args_info(func):
    name_by_idx = {}
    defaults_by_idx = {}
    for idx, (p_k, p_v) in enumerate(inspect.signature(func).parameters.items()):
        name_by_idx[idx] = p_k
        if p_v.default != inspect.Parameter.empty:
            defaults_by_idx[idx] = p_v.default
    return name_by_idx, defaults_by_idx


def args_as_tuple(func, args: tuple, kwargs: dict) -> tuple:
    name_by_idx, defaults_by_idx = _func_args_info(func)
    return args + tuple(
        kwargs[n] if (n := name_by_idx[i]) in kwargs else defaults_by_idx[i]
        for i in range(len(args), len(name_by_idx))
    )


def args_as_dict(func, args: tuple, kwargs: dict) -> dict:
    name_by_idx, defaults_by_idx = _func_args_info(func)
    return kwargs | {
        n: args[i] if i < len(args) else defaults_by_idx[i]
        for i, n in name_by_idx.items()
        if n not in kwargs
    }
