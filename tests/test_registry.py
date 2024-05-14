from unittest.mock import Mock
import multiprocessing as mp
from queue import Empty as EmptyQException

import pytest

from noorm.registry import get_registry
import noorm.sqlalchemy_sync as nm

from subdir import some_module  # noqa: F401


@nm.sql_scalar_or_none(bool)
def noop(divide_by_zero: bool = False):
    if divide_by_zero:
        return 1 / 0
    raise nm.CancelExecException


def test_funcs():
    registry = get_registry()
    registry.clear_stat()
    event_listener_callback = Mock()
    registry.add_event_listener(event_listener_callback)
    registry.add_event_listener(lambda x: 1 / 0)  # bad listener
    func_name = "test_registry.noop"
    assert func_name in registry.func_names
    assert "subdir.some_module.some_func" in registry.func_names
    assert "subdir.some_module.SomeClass.say_hello" in registry.func_names

    noop(None)
    with pytest.raises(ZeroDivisionError):
        noop(None, divide_by_zero=True)

    got_stat = registry.stat_by_name[func_name]
    assert got_stat.calls == 2
    assert got_stat.duration > 0
    assert got_stat.fails == 1
    assert got_stat.tuples == 0
    assert got_stat.fails_by_error == {"ZeroDivisionError": 1}

    got_callback_calls = [
        (el.args[0].func_name, el.args[0].tuples, el.args[0].error)
        for el in event_listener_callback.call_args_list
    ]
    assert got_callback_calls == [
        (func_name, 0, None),
        (func_name, 0, "ZeroDivisionError"),
    ]


def sideprocess_worker(res_queue: mp.Queue):
    get_registry().init_multiprocess_registry()
    noop(None)
    with pytest.raises(ZeroDivisionError):
        noop(None, divide_by_zero=True)

    res_queue.put("OK")


@pytest.mark.parametrize("start_method", ("fork", "spawn"))
def test_multiprocessing(start_method: str):
    mp.set_start_method(start_method, force=True)
    registry = get_registry()
    registry.clear_stat()
    assert registry.stat_by_name == {}

    def _do(target):
        mp_queue = mp.Queue()
        sp = mp.Process(target=target, args=(mp_queue,))
        sp.start()
        sp.join(timeout=5.0)
        try:
            return mp_queue.get_nowait()
        except EmptyQException:
            return "sideprocess crashed"

    # Test behavior with no listener
    sp_res = _do(sideprocess_worker)
    assert sp_res == "OK"
    assert registry.stat_by_name == {}

    # Test behavior with listener
    registry.init_multiprocess_registry()

    sp_res = _do(sideprocess_worker)
    assert sp_res == "OK"

    assert "test_registry.noop" in registry.stat_by_name
    got_stat = registry.stat_by_name["test_registry.noop"]
    assert got_stat.calls == 2
    assert got_stat.duration > 0
    assert got_stat.fails == 1
    assert got_stat.tuples == 0
    assert got_stat.fails_by_error == {"ZeroDivisionError": 1}

    registry.close_multiprocess_registry()
