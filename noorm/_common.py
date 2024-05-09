from .registry import get_registry


class CancelExecException(Exception):
    """Do not execute any query and respond empty result"""


class WrapperBase:
    def __init__(self, func) -> None:
        self._func = func
        get_registry().register(func)

    def unwrapped(self, *args, **kwargs):
        return self._func(*args, **kwargs)
