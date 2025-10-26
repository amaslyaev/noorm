from enum import Enum
import sys
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
    def __init__(self, func, bound_unwrapped=None, do_register=True) -> None:
        self._func = func
        self._orig_func = func
        self._bound_unwrapped = bound_unwrapped
        if isinstance(self._func, (classmethod, staticmethod)) and sys.version > "3.13":
            self._func = self._func.__wrapped__
        # update_wrapper(self, func)
        if do_register:
            get_registry().register(func)

    def unwrapped(self, *args, **kwargs):
        if self._bound_unwrapped:
            return self._bound_unwrapped(*args, **kwargs)
        return self._func(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        return self._func(*args, **kwargs)

    def __get__(self, obj, objtype=None):
        # If the function is an object or class method, here we inject `self` or `cls`
        def _bound(*args, **kwargs):
            return self(args[0], obj, *(args[1:]), **kwargs)

        def _bound_unwrapped(*args, **kwargs):
            return self._func(obj, *args, **kwargs)

        if isinstance(self._orig_func, staticmethod) and sys.version > "3.13":
            return self
        else:
            return WrapperBase(_bound, _bound_unwrapped, False)


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
