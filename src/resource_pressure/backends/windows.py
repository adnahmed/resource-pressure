"""Windows memory resource notification objects via the standard-library ctypes."""
from __future__ import annotations

import ctypes
import sys
from threading import Event
from typing import Any

from ..model import BackendUnavailable, PressureEvent, PressureLevel
from .base import Emit, Ready


def _map_state(low: bool, high: bool, previous: PressureLevel) -> PressureLevel:
    # Low wins in a race between the two independent queries. No invented third
    # severity: Windows exposes low/high, not warning/critical.
    if low:
        return PressureLevel.PRESSURED
    if high:
        return PressureLevel.NORMAL
    # Neither condition is a real Windows state. Keep the previous decision;
    # at startup, conservatively hold admission until high is observed.
    return previous if previous != PressureLevel.UNKNOWN else PressureLevel.PRESSURED


class _WindowsAPI:
    def __init__(self) -> None:
        from ctypes import wintypes as w
        self.w = w
        self.lib = ctypes.WinDLL("kernel32", use_last_error=True)  # type: ignore[attr-defined]
        self.lib.CreateMemoryResourceNotification.argtypes = [ctypes.c_int]
        self.lib.CreateMemoryResourceNotification.restype = w.HANDLE
        self.lib.QueryMemoryResourceNotification.argtypes = [w.HANDLE, ctypes.POINTER(w.BOOL)]
        self.lib.QueryMemoryResourceNotification.restype = w.BOOL
        self.lib.WaitForSingleObject.argtypes = [w.HANDLE, w.DWORD]
        self.lib.WaitForSingleObject.restype = w.DWORD
        self.lib.CloseHandle.argtypes = [w.HANDLE]
        self.lib.CloseHandle.restype = w.BOOL

    def create(self, kind: int) -> Any:
        handle = self.lib.CreateMemoryResourceNotification(kind)
        if not handle:
            raise ctypes.WinError(ctypes.get_last_error())  # type: ignore[attr-defined]
        return handle

    def query(self, handle: Any) -> bool:
        state = self.w.BOOL()
        if not self.lib.QueryMemoryResourceNotification(handle, ctypes.byref(state)):
            raise ctypes.WinError(ctypes.get_last_error())  # type: ignore[attr-defined]
        return bool(state.value)

    def wait(self, handle: Any, milliseconds: int) -> bool:
        result = self.lib.WaitForSingleObject(handle, milliseconds)
        if result == 0:
            return True
        if result == 258:  # WAIT_TIMEOUT: check only shutdown, do not invent state.
            return False
        if result == 0xFFFFFFFF:
            raise ctypes.WinError(ctypes.get_last_error())  # type: ignore[attr-defined]
        raise OSError(f"Unexpected WaitForSingleObject result: {result:#x}")

    def close(self, handle: Any) -> None:
        self.lib.CloseHandle(handle)


class WindowsMemoryBackend:
    name = "windows-memory-resource-notification"

    def run(self, stop: Event, emit: Emit, ready: Ready) -> None:
        if sys.platform != "win32":
            raise BackendUnavailable("Memory resource notifications require Windows")
        api = _WindowsAPI()
        handles: list[Any] = []
        try:
            try:
                low = api.create(0)
                handles.append(low)
                high = api.create(1)
                handles.append(high)
                level = _map_state(api.query(low), api.query(high), PressureLevel.UNKNOWN)
            except OSError as exc:
                raise BackendUnavailable(f"Windows memory notification initialization: {exc}") from exc
            emit(PressureEvent(level, self.name, "initial OS low/high notification state"))
            ready()
            while not stop.is_set():
                # These objects are level-triggered. Waiting on both when either
                # is signalled causes a busy loop. Wait only for the opposite
                # condition: low when admitted, high while pressure is latched.
                target = low if level == PressureLevel.NORMAL else high
                if not api.wait(target, 200):
                    continue
                updated = _map_state(api.query(low), api.query(high), level)
                if updated != level:
                    level = updated
                    reason = ("HighMemoryResourceNotification" if level == PressureLevel.NORMAL
                              else "LowMemoryResourceNotification")
                    emit(PressureEvent(level, self.name, reason))
                else:
                    # Handles can race with queries, or both can briefly appear
                    # signalled across two queries. Bound the retry rate.
                    stop.wait(0.05)
        finally:
            for handle in reversed(handles):
                api.close(handle)
