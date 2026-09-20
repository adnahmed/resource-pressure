# Architecture and operational contract

## Separation of concerns

```
Native sensor                         Application
Windows memory notifications          producer / independent task queue
Linux PSI triggers                                  |
macOS libdispatch                                   v
         |                              PressureGovernor admission
         +---- PressureEvent ----------> state + fixed in-flight ceiling
                                                    |
                                   +----------------+----------------+
                                   |                                 |
                              Dask Future                 GovernedProcessGroup
                              public API                  processkit native core
                                                                     |
                                                       Job Object / cgroup v2
```

The standard-library core selects and owns one native monitor thread. It does
not start subprocesses, set resource limits, or change Dask configuration on
import. Construction alone does not start monitoring. Entering a sync/async
context initializes the sensor and checks that registration succeeded.

A backend implements `run(stop, emit, ready)`. Initialization errors are surfaced
before the context returns. A backend must close all its native resources in a
`finally` block. Returning unexpectedly or raising after initialization closes
admission and wakes both blocking and async callers with a typed exception.
There is no successful silent no-op backend.

## Admission and lifetime

All readiness/capacity decisions and lease increments use the same condition
lock. Sync waiters use a condition; asyncio waiters register a loop-local Future
under the same lock. Native threads wake them through `call_soon_threadsafe`.
Slots are claimed only after a waiter actually resumes and rechecks state.
Cancellation before that point cannot reserve capacity accidentally.

A lease releases exactly once, even when completion, cleanup, and cancellation
race. Acquired work is not revoked by later pressure. Context exit closes new
admission immediately and stops the native monitor; it does not wait for or kill
arbitrary existing application work. Finish/cancel your work before leaving the
governor scope. Lease release is still allowed after governor close.

Fairness is best-effort, not FIFO. Instances are process-local, not serializable
shared semaphores. Forked reuse is explicitly rejected before acquiring inherited
locks. Create governors inside spawned child processes, not at module import.

Python allocation and operating-system notification latency remain. No decision
is atomic with arbitrary future allocations; a single task can outgrow memory,
and a burst can arrive before the next event. The fixed ceiling limits initial
fan-out but does not estimate what fits.

## Native details and policy ownership

Windows owns the low/high thresholds. The library provides a latch, including a
conservative startup decision in the neither-signalled interval. It deliberately
does not fabricate a third native severity. Native waits alternate between low
and high handles, avoiding repeated wakeup on a level-triggered current signal.

Linux owns measurement and trigger delivery; this library owns the trigger
configuration, severity labels, initial observation period, and quiet-time
recovery. `PSIConfig` rejects invalid/nonfinite values and uses two-second window
multiples. Each requested metric/scope has a separate open descriptor.
Registration is all-or-nothing: no dropping a cgroup sensor merely because host
PSI works. Global pressure is the default, and cgroup pressure must be configured
explicitly. All requested scopes feed one conservative, maximum-severity latch.

macOS owns its semantic event classifications. libdispatch callbacks only put
flags/errors on a queue. The monitor thread invokes application subscribers.
Cancellation completion plus a serial-queue barrier precedes callback/native
object release. Initial/coalesced-state lookup uses an XNU sysctl that is not a
stable public API, so macOS runtime compatibility needs specific testing.

## Dask limits

`DaskAdmission.submit` returns a normal Future, with a callback releasing logical
admission on completion/error/cancellation. It defaults to `pure=False` so two
independent submissions are not accidentally deduplicated. Passing `pure=True`
is the caller's choice; each submission still counts as its own logical admission.
Retries/lost-worker recomputation and Dask cancellation semantics are owned by
Dask, not overridden by this library.

`map_unordered` holds leases until results are consumed and references released.
A one-element input lookahead detects end-of-input without needing another
admission. This matters: after the last result, lingering pressure must not make
an exhausted iterator wait forever. Completed futures drain while pressure
blocks new inputs. The consumer must close a partially consumed iterator with
`contextlib.closing`; abandoning a generator is not an explicit cleanup strategy.

The adapter does not know where a task will be scheduled. Host-local producer
sensing is appropriate for a local cluster, not a claim of cluster-wide health.
A true multi-host implementation would need per-host agents, health/expiry for
those signals, placement-aware admission, and failure/partition policy. None of
that is hidden behind `auto()` in this alpha.

## Containment limits

processkit owns safe process creation and native tree teardown. The adapter
validates the reported mechanism and never equates a POSIX group with a hard
memory container. Explicit resource budgets are passed unchanged to processkit;
the adapter does not invent a budget or lower limits reactively during pressure.

This layer contains only its launched children and future descendants, subject
to the chosen OS mechanism. It neither migrates the existing Python process nor
retroactively captures an existing worker tree. The included launcher establishes
an outer boundary first, then starts Dask within it.

Group setup/exit is a single-owner operation: do not close a group concurrently
with its running methods. Join all those operations first. Captured output is
bounded, but task-specific allocations and persistent descendant memory are not
accounted for by an admission lease. Cgroup limits, delegation, browser
sandboxing, and service-manager ownership remain deployment responsibilities.

## Explicit exclusions for 0.1.0

No global distributed coordinator; no remote node telemetry transport; no
adaptive concurrency estimation; no cgroup-v1 fallback; no default PSI-counter
polling; no RAM-percentage fallback; no GIO dependency; no autonomous hard-limit
selection; no worker monkey-patching; no scheduler-private APIs; no security or
OOM-proof claim. The architecture leaves room for additional backends without
making these features appear to exist today.
