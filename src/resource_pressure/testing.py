"""Deterministic injection, explicitly opt-in; never selected by auto()."""
from __future__ import annotations

import queue
from threading import Event

from .backends.base import Emit, Ready
from .model import PressureEvent, PressureLevel


class ManualBackend:
    name = "manual-test-backend"

    def __init__(self, initial: PressureLevel = PressureLevel.NORMAL):
        self.initial = PressureLevel(initial)
        self._queue: queue.Queue[PressureLevel | BaseException] = queue.Queue()

    def set_level(self, level: PressureLevel) -> None:
        self._queue.put(PressureLevel(level))

    def fail(self, error: BaseException | None = None) -> None:
        self._queue.put(error or OSError("injected sensor failure"))

    def run(self, stop: Event, emit: Emit, ready: Ready) -> None:
        emit(PressureEvent(self.initial, self.name, "explicit test signal"))
        ready()
        while not stop.is_set():
            try:
                item = self._queue.get(timeout=0.05)
            except queue.Empty:
                continue
            if isinstance(item, BaseException):
                raise item
            emit(PressureEvent(item, self.name, "explicit test signal"))
