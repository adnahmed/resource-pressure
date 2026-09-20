# resource-pressure

**Native memory-pressure signals → bounded task admission → optional native process containment.**

Python 3.10+. The monitoring/admission core uses only the standard library.
No `psutil`, RAM percentages, free-memory reserves, per-worker memory estimates,
or adaptive thread-pool algorithm. No GLib, PyGObject, MSYS2, or compiler is
needed for the core wheel.

**Version 0.1.0 is an alpha implementation, not a production-validated release.**
The supplied source and wheel have not been published to PyPI. Use the local
paths below; the distribution name is not a claim of ownership of a PyPI name.
See `docs/TEST_REPORT.md` for what actually ran and what remains unverified.

## Install

After extracting the archive, from its parent directory:

```bash
# Core native sensors and admission.
uv add ./resource-pressure

# Also install Dask and the processkit containment adapter dependencies.
uv add ./resource-pressure --extra all
```

For an editable source checkout:

```bash
uv add --editable ./resource-pressure --extra all
```

Or install the supplied universal core wheel:

```bash
python -m pip install ./resource_pressure-0.1.0-py3-none-any.whl
```

The wheel itself has no mandatory runtime dependencies. The optional
`containment` extra uses `processkit-py>=1.0,<2`; its own native wheel availability
and platform restrictions still apply. The `dask` extra installs `distributed`.

## The API

```python
import asyncio
from resource_pressure import PressureGovernor

async def main():
    async with PressureGovernor.auto(max_in_flight=4) as governor:
        async with governor.slot():
            await perform_one_heavy_operation()

# asyncio.run(main())  # Define your own perform_one_heavy_operation first.
```

Synchronous code uses the same context-manager surface:

```python
from resource_pressure import PressureGovernor

with PressureGovernor.auto(max_in_flight=4) as governor:
    with governor.slot(timeout=30):
        result = perform_one_heavy_operation()
```

A runnable simulation with no native prerequisites is included:

```bash
uv run python examples/simulated_demo.py
```

`max_in_flight` is a fixed ceiling on admitted operations, **not** a RAM threshold.
When omitted it defaults to the available process CPU count (or CPU count on
older Python). It is an operational concurrency limit, not a safe-memory estimate.

The admission rule is intentionally small:

```text
UNKNOWN     → admit nothing while initializing
NORMAL      → admit up to max_in_flight operations
PRESSURED   → admit nothing new; existing operations finish
CRITICAL    → admit nothing new; existing operations finish
sensor error / closed governor → wake waiters and raise
```

Recovery reopens admission to the same ceiling. The package does not kill,
suspend, shrink, or restart already-running tasks on a pressure transition.
It does not calculate an optimal worker count.

For observation without reserving a slot:

```python
await governor.wait_until_safe()
# Or governor.wait_until_safe_sync() in blocking code.
```

**That call is a state check, not a reservation.** Use `slot()` or
`acquire()`/`acquire_sync()` when admitting work. Hold a lease until the work
finishes; releasing immediately after submission defeats the bound.

## What each platform actually does

| Platform | Pressure sensor | Native states and mapping | Containment extra |
|---|---|---|---|
| Windows | `CreateMemoryResourceNotification`, query, native wait | High → NORMAL; low → PRESSURED; no fabricated CRITICAL tier | Job Objects through processkit |
| Linux | PSI trigger registration and `poll(POLLPRI)` | Library maps `some` to PRESSURED and `full` to CRITICAL | cgroup v2 through processkit, when available |
| macOS | libdispatch memory-pressure source | Native normal/warn/critical → NORMAL/PRESSURED/CRITICAL | POSIX process groups only, explicit opt-in; no whole-tree memory cap |

OS details are documented in the primary references in `docs/SOURCES.md`.
The levels are useful common labels, **not calibrated equivalents across OSes**.

### Linux: honest policy, not pretend OS severity

PSI reports memory-related execution stalls and accepts user-specified stall-time
triggers. Linux does **not** label these as NORMAL/PRESSURED/CRITICAL for us.
The library registers these defaults, on separate file descriptors:

```text
some 150000 2000000
full  50000 2000000
```

These are **library-selected defaults**: 150 ms of some-stall or 50 ms of full-stall
within a two-second window. They are not percentages of RAM and are not asserted
to be optimal for every workload. PSI has no recovery event; this implementation
infers recovery after four seconds without either trigger. CRITICAL is retained
until its own quiet interval expires, even if less severe events arrive.
Initial admission waits through one observation window.

Application code needs no RAM policy, but the underlying Linux policy cannot be
eliminated. It is explicit and configurable:

```python
from resource_pressure import PSIConfig, PressureGovernor

governor = PressureGovernor.auto(
    psi=PSIConfig(some_stall_us=150_000, full_stall_us=50_000,
                  window_us=2_000_000, quiet_seconds=4.0)
)
```

Default scope is `/proc/pressure/memory` only. To monitor a deployment-provided
cgroup and the host, explicitly supply both paths:

```python
governor = PressureGovernor.auto(psi_paths=[
    "/proc/pressure/memory",
    "/sys/fs/cgroup/YOUR_DELEGATED_GROUP/memory.pressure",
])
```

All requested paths must support writable PSI triggers. Merely reading PSI
counters is insufficient for this event-only backend. Missing PSI, read-only
mounts, permission failures, or disappearing cgroups raise errors; there is no
fallback to polling RAM or silently dropping a requested scope. Two-second
window multiples satisfy the kernel's unprivileged window rule, but do not grant
filesystem permissions. Administrators should provision access rather than
running an entire application as root just for the sensor.

**A system-wide signal does not guarantee warning before a cgroup-specific
memory limit is hit.** Per-cgroup sensing is explicit; the processkit adapter
does not discover or export its private cgroup path into the sensor automatically.

### Windows: use the OS's decision

The backend waits for low-memory while NORMAL, and for high-memory after
pressure. It does not wait repeatedly on an already-signalled handle. The
neutral region where neither object is signalled retains the previous decision;
at startup, a neutral state conservatively blocks until high is observed.
Timeouts only allow orderly thread shutdown; they do not sample RAM.

### macOS: native events, with a bootstrap qualification

libdispatch supplies normal, warning, and critical events. A native semantic
sysctl, `kern.memorystatus_vm_pressure_level`, bootstraps initial state and resolves
coalesced events whose ordering is ambiguous. This sysctl exists in Apple's
published XNU implementation, but is **not a promised stable public API**.
Failure to read it makes startup fail explicitly. The ctypes callback lifetime,
cancellation, and queue-barrier cleanup still require actual macOS validation.

## Dask

Keep admission in the **producer**, before submitting independent heavy tasks.
The supplied adapter uses public `Client.submit` and Future APIs, not scheduler
internals or worker monkey-patches. Keep Dask's existing memory protections.

```python
from contextlib import closing
from distributed import Client, LocalCluster
from resource_pressure import PressureGovernor
from resource_pressure.integrations.dask import DaskAdmission


def heavy_task(item):
    return item * item


def main():
    with LocalCluster(n_workers=4, threads_per_worker=1) as cluster:
        with Client(cluster) as client, PressureGovernor.auto(max_in_flight=4) as governor:
            admitted = DaskAdmission(client, governor)
            with closing(admitted.map_unordered(heavy_task, range(100))) as results:
                for result in results:
                    print(result)


if __name__ == "__main__":
    main()
```

`map_unordered` is the recommended bounded streaming interface. It retains at
most `max_in_flight` submitted futures plus one input lookahead, drains completed
results even during pressure, and releases futures as results are consumed.
Closing the iterator cancels/releases the remaining futures and releases leases.
Large results retained by your application are still your application's memory.
Input iteration should be cheap; one lookahead can happen while admission is shut.

`submit()` and `await asubmit()` return ordinary Dask futures. Their logical lease
lasts until success, failure, or cancellation. They bound unfinished admitted
futures, **not** an ever-growing list of completed results retained by the caller.
Use the streaming API rather than building a giant list of futures.

Important boundaries:

- This sensor observes the producer's machine/configured PSI scopes, not every
  remote worker. Multi-host global pressure aggregation is not implemented.
- It does not resize Dask workers, gate existing queued graphs, cover submissions
  through another client, or govern arbitrary dependent graph nodes.
- Future cancellation does not guarantee that already-running Python code stops.
  Thus the bound is on logical admissions, not an inviolable physical-work count
  after cancellation or worker failure.
- A completed remote task can retain data. A pressure gate is not proof of freed
  RAM; release results and keep Dask's spill/pause/termination mechanisms enabled.

Never put a blocking governor inside every Dask worker task indiscriminately;
blocked workers can occupy the very slots needed by dependencies that would
finish and free memory.

## Unified containment

The same governor can create a pressure-gated process runner:

```python
import sys
from resource_pressure import PressureGovernor

with PressureGovernor.auto(max_in_flight=2) as governor:
    with governor.process_group(max_memory=512 * 1024 * 1024) as children:
        print(children.mechanism)  # must be job_object or cgroup_v2 by default
        result = children.run(sys.executable, ["-c", "print('hello')"], timeout=20)
        print(result.stdout)
```

`run()`/`await arun()` acquire admission before process execution and release it
when the command's output operation completes. They return a
`processkit.ProcessResult`; nonzero exit codes are data and should be checked.
Captured output is bounded to 1 MiB by default (`output_limit=` changes this).
Arguments are passed as a sequence, never implicitly through a shell.

`max_memory` is an **optional, explicit deployment budget** for the whole group,
not automatically chosen from system RAM. Omit it for lifecycle containment
without a memory cap. `max_processes` is also optional. Fixed limits are independent
of pressure signals; no automatic `memory.high` tuning is performed.

The adapter rejects process-group fallback unless
`allow_process_group_fallback=True` is explicit. Such fallback cannot enforce the
requested whole-tree memory/process limits. It is not a security sandbox, and
session-changing descendants can escape POSIX process groups.

**Only children started through this runner are covered.** Merely constructing a
group does not contain your existing process, browser, or Dask cluster.
To contain Dask's hierarchy, launch the supervisor inside a group **before** it
creates workers. `examples/contained_dask_launcher.py` demonstrates that outer
boundary; its child runs `examples/dask_local.py` with per-task admission.
Already-running workers are not adopted automatically. Finish all concurrent
`run`/`arun` calls before leaving the group context.

A root command can exit while descendants remain; the shared group's final
context exit handles its owned tree. An admission lease does not count every
remaining descendant. Teardown and abrupt-parent-death guarantees are those of
the reported processkit mechanism; do not assume Linux/macOS match Windows.

## Diagnostics and events

```bash
uv run resource-pressure doctor
uv run resource-pressure watch
uv run resource-pressure doctor --psi-path /path/to/delegated/memory.pressure
```

`doctor` registers the actual native monitor, reports its backend and initial
state, and closes it. It does not claim to test containment or induce pressure.
Missing/failed native support exits with code 2, not a fake NORMAL status.

```python
unsubscribe = governor.subscribe(
    lambda event: print(event.level.name, event.backend, event.reason)
)
# Later: unsubscribe()
```

Subscribers run on the monitor thread and must be short/nonblocking. Exceptions
are logged and isolated. Use `loop.call_soon_threadsafe` to notify an asyncio
loop; do not perform expensive memory reclamation inside the callback.
Subscribers receive level transitions, not a durable audit log. Call
`check_health()` for explicit status; admission methods automatically check it.

## Development and validation

```bash
uv sync --extra dev
uv run pytest -q -ra
uv build
```

Or with an existing Python environment:

```bash
python -m pip install -e ".[dev]"
python -m pytest -q -ra
python -m build
```

The included CI workflow defines Linux, Windows, and macOS jobs. It has **not**
been run on hosted CI as part of this delivery. Real sensor tests require
`RESOURCE_PRESSURE_NATIVE=1`; dependency tests run only when their extras are
installed. No automated test deliberately exhausts host memory.

Read `docs/ARCHITECTURE.md`, `docs/TEST_REPORT.md`, and `docs/SOURCES.md` before
production deployment. This is a pressure-responsive admission library, **not an
OOM-proof memory manager or a security boundary**.
