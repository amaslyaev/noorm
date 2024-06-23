import logging
from typing import Callable
from dataclasses import dataclass, field
from collections import defaultdict
from functools import lru_cache
from time import perf_counter
from multiprocessing import current_process
from threading import Thread
import socket
import os
import pickle


@dataclass
class Stat:
    calls: int = 0
    duration: float = 0.0
    tuples: int = 0
    fails: int = 0
    fails_by_error: dict[str, int] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class FuncCallEvent:
    func_name: str
    duration: float | None
    tuples: int
    error: str | None


class Registry:
    def __init__(self) -> None:
        self.name_by_func: dict[Callable, str] = {}
        self.func_names: set[str] = set()
        self.stat_by_name: defaultdict[str, Stat] = defaultdict(Stat)
        self._event_listeners: list[Callable[[FuncCallEvent], None]] = []
        self._mp_listener_started = False
        self._is_main_process = current_process().name == "MainProcess"
        self._mp_socket: socket.socket | None = None
        self._mp_port = 0
        if not self._is_main_process:
            self._mp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            reg_port = int(os.getenv("NOORM_REGISTRY_PORT") or "0")
            if reg_port == 0:
                logging.warning(
                    "Noorm works in a child process, but 'init_multiprocess_registry' "
                    "on a main process was not invoked. All statistics on this child "
                    "process will be lost."
                )
            self._mp_port = reg_port

    def _main_process_socket_listener(self) -> None:
        while self._mp_socket is not None:
            bin_data, _ = self._mp_socket.recvfrom(4096)
            try:
                data = pickle.loads(bin_data)
                # data is a func name or a FuncCallEvent
                if isinstance(data, str):
                    if data == "#stop#":
                        self._mp_socket.close()
                        self._mp_socket = None
                        return
                    self.func_names.add(data)
                else:
                    assert isinstance(data, FuncCallEvent)
                    self.on_event(data)
            except Exception:
                pass  # do nothing

    def init_multiprocess_registry(self) -> None:
        if not self._mp_listener_started:
            if not self._is_main_process:
                logging.warning(
                    "Cannot init multiprocess noorm registry in a child process"
                )
                return
            self._mp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self._mp_socket.bind(("localhost", 0))
            self._mp_port = self._mp_socket.getsockname()[1]
            os.environ["NOORM_REGISTRY_PORT"] = str(self._mp_port)
            self._mp_listener_started = True
            Thread(target=self._main_process_socket_listener, daemon=True).start()

    def close_multiprocess_registry(self) -> None:
        """Mainly for testing purposes"""
        if self._mp_listener_started:
            socket.socket(socket.AF_INET, socket.SOCK_DGRAM).sendto(
                pickle.dumps("#stop#"), ("localhost", self._mp_port)
            )
            del os.environ["NOORM_REGISTRY_PORT"]
            self._mp_listener_started = False

    def register(self, func: Callable) -> str:
        func_name = func.__module__ + "." + func.__qualname__
        self.func_names.add(func_name)
        self.name_by_func[func] = func_name
        if not self._is_main_process and self._mp_port and self._mp_socket is not None:
            try:
                self._mp_socket.sendto(
                    pickle.dumps(func_name), ("localhost", self._mp_port)
                )
            except Exception:
                pass  # do nothing
        return func_name

    def add_event_listener(self, callback: Callable[[FuncCallEvent], None]) -> None:
        self._event_listeners.append(callback)

    def on_event(self, event: FuncCallEvent) -> None:
        if not self._is_main_process:
            if self._mp_port and self._mp_socket is not None:
                try:
                    self._mp_socket.sendto(
                        pickle.dumps(event), ("localhost", self._mp_port)
                    )
                except Exception:
                    pass  # do nothing
        else:
            stat = self.stat_by_name[event.func_name]
            stat.calls += 1
            stat.duration += event.duration or 0
            stat.tuples += event.tuples
            if event.error:
                stat.fails += 1
                stat.fails_by_error[event.error] = (
                    stat.fails_by_error.get(event.error, 0) + 1
                )

            for callback in self._event_listeners:
                try:
                    callback(event)
                except Exception:
                    pass  # Just ignore all the fails in external listeners

    def clear_stat(self) -> None:
        self.stat_by_name.clear()


class MetricsCollector:
    def __init__(self, func: Callable) -> None:
        self.registry = get_registry()
        self.func = func
        self.tuples = 0
        self.start_time: float | None = None

    def finish(self, exc_type: type | None) -> None:
        if self.start_time is not None:
            duration = perf_counter() - self.start_time
            self.start_time = None
            self.registry.on_event(
                FuncCallEvent(
                    (
                        self.registry.name_by_func.get(self.func)
                        or self.registry.register(self.func)
                    ),
                    duration,
                    self.tuples,
                    None if exc_type is None else exc_type.__name__,
                )
            )

    def __enter__(self):
        self.start_time = perf_counter()
        return self

    def __exit__(self, exc_type: type | None, exc_value, traceback) -> None:
        self.finish(exc_type)


@lru_cache
def _get_registry(pid):
    return Registry()


def get_registry():
    return _get_registry(current_process().pid)
