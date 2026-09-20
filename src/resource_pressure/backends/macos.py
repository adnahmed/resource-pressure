"""libdispatch native memory-pressure events; no PyObjC or compiler dependency."""
from __future__ import annotations

import ctypes
import queue
import sys
from threading import Event

from ..model import BackendUnavailable, PressureEvent, PressureLevel
from .base import Emit, Ready


def _map_flags(flags: int) -> PressureLevel:
    # libdispatch can coalesce flags. Prefer the most severe observed condition.
    if flags & ~7:
        raise OSError(f"Unknown Darwin memory-pressure flags: {flags:#x}")
    if flags & 4:
        return PressureLevel.CRITICAL
    if flags & 2:
        return PressureLevel.PRESSURED
    if flags & 1:
        return PressureLevel.NORMAL
    return PressureLevel.UNKNOWN


class _DarwinSource:
    def __init__(self) -> None:
        self.events: queue.Queue[int | BaseException] = queue.Queue()
        self.cancelled = Event()
        self.source = None
        self.dispatch_queue = None
        self.activated = False
        self.lib = ctypes.CDLL("/usr/lib/system/libdispatch.dylib")
        p = ctypes.c_void_p
        u = ctypes.c_ulong
        self.callback_type = ctypes.CFUNCTYPE(None, p)
        signatures = {
            "dispatch_queue_create": ([ctypes.c_char_p, p], p),
            "dispatch_source_create": ([p, ctypes.c_size_t, u, p], p),
            "dispatch_source_set_event_handler_f": ([p, self.callback_type], None),
            "dispatch_source_set_cancel_handler_f": ([p, self.callback_type], None),
            "dispatch_source_get_data": ([p], u),
            "dispatch_resume": ([p], None),
            "dispatch_source_cancel": ([p], None),
            "dispatch_sync_f": ([p, p, self.callback_type], None),
            "dispatch_release": ([p], None),
        }
        for name, (args, result) in signatures.items():
            fn = getattr(self.lib, name)
            fn.argtypes = args
            fn.restype = result

        def on_event(context: object) -> None:
            try:
                flags = int(self.lib.dispatch_source_get_data(self.source))
                if flags & (flags - 1):
                    # A coalesced NORMAL|CRITICAL is not ordered. Resolve with
                    # the native semantic state rather than latching forever.
                    flags = self._initial_flags()
                self.events.put(flags)
            except BaseException as exc:
                self.events.put(exc)  # Never propagate through a C callback.

        def on_cancel(context: object) -> None:
            self.cancelled.set()

        self.event_callback = self.callback_type(on_event)
        self.cancel_callback = self.callback_type(on_cancel)
        self.barrier_callback = self.callback_type(lambda context: None)
        try:
            self.dispatch_queue = self.lib.dispatch_queue_create(b"resource-pressure.memory", None)
            if not self.dispatch_queue:
                raise OSError("dispatch_queue_create returned NULL")
            # The exported symbol is a struct, not a pointer variable. Take its
            # address; c_void_p.in_dll(...).value would read the wrong thing.
            type_symbol = ctypes.c_byte.in_dll(self.lib, "_dispatch_source_type_memorypressure")
            type_address = ctypes.addressof(type_symbol)
            self.source = self.lib.dispatch_source_create(type_address, 0, 7, self.dispatch_queue)
            if not self.source:
                raise OSError("dispatch_source_create returned NULL")
            self.lib.dispatch_source_set_event_handler_f(self.source, self.event_callback)
            self.lib.dispatch_source_set_cancel_handler_f(self.source, self.cancel_callback)
            # XNU exports dispatch-compatible flags through this sysctl. This is
            # an implementation interface, not a promised stable Apple API. It
            # bootstraps state before events; failures are explicit, not NORMAL.
            self.events.put(self._initial_flags())
            self.lib.dispatch_resume(self.source)
            self.activated = True
        except BaseException:
            self.close()
            raise

    @staticmethod
    def _initial_flags() -> int:
        libc = ctypes.CDLL("/usr/lib/libSystem.B.dylib", use_errno=True)
        libc.sysctlbyname.argtypes = [ctypes.c_char_p, ctypes.c_void_p,
                                     ctypes.POINTER(ctypes.c_size_t), ctypes.c_void_p,
                                     ctypes.c_size_t]
        libc.sysctlbyname.restype = ctypes.c_int
        value = ctypes.c_uint32()
        size = ctypes.c_size_t(ctypes.sizeof(value))
        if libc.sysctlbyname(b"kern.memorystatus_vm_pressure_level", ctypes.byref(value),
                            ctypes.byref(size), None, 0) != 0:
            raise OSError(ctypes.get_errno(), "Cannot read Darwin initial pressure state")
        if size.value != ctypes.sizeof(value) or value.value not in (1, 2, 4):
            raise OSError(f"Unsupported initial Darwin pressure value: {value.value}")
        return value.value

    def close(self) -> None:
        if self.source:
            # A suspended source must be resumed for cancellation to complete.
            if not self.activated:
                self.lib.dispatch_resume(self.source)
                self.activated = True
            self.lib.dispatch_source_cancel(self.source)
            # Keep ctypes callback objects alive until cancellation AND a serial
            # queue barrier finish. Event.set alone does not prove C returned.
            self.cancelled.wait()
            self.lib.dispatch_sync_f(self.dispatch_queue, None, self.barrier_callback)
            self.lib.dispatch_release(self.source)
            self.source = None
        if self.dispatch_queue:
            self.lib.dispatch_release(self.dispatch_queue)
            self.dispatch_queue = None


class MacOSMemoryBackend:
    name = "macos-dispatch-memorypressure"

    def run(self, stop: Event, emit: Emit, ready: Ready) -> None:
        if sys.platform != "darwin":
            raise BackendUnavailable("libdispatch memory pressure requires macOS")
        try:
            source = _DarwinSource()
        except (OSError, ValueError, AttributeError) as exc:
            raise BackendUnavailable(f"Cannot initialize Darwin memory-pressure source: {exc}") from exc
        try:
            first = True
            previous: PressureLevel | None = None
            while not stop.is_set():
                try:
                    item = source.events.get(timeout=0.2)
                except queue.Empty:
                    continue
                if isinstance(item, BaseException):
                    raise item
                level = _map_flags(item)
                if level != previous:
                    emit(PressureEvent(level, self.name, f"native dispatch pressure flags {item:#x}"))
                    previous = level
                if first:
                    first = False
                    ready()
        finally:
            source.close()
