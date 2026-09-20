# Validation report — resource-pressure 0.1.1

Prepared September 20, 2026. This remains an alpha implementation, not a
production-ready or platform-certified release.

## Executed in this delivery environment

Environment: Linux execution sandbox, CPython 3.13.5.

```text
python -m pytest -q -ra
104 passed, 3 skipped
```

The passing tests exercise the real Python implementation with deterministic
native-interface or external-library doubles where necessary:

- State transitions, existing-work preservation, bounded sync/async concurrency,
  timeout validation, cancellation, idempotent lease release, subscriber error
  isolation, explicit failure/close wakeup, and fork-reuse rejection.
- New admission pacing validation, including non-blocking burst suppression and
  sync/async wakeup when the configured minimum start interval expires.
- Linux trigger formatting, separate fd ownership, backend control flow with
  simulated poll events, warmup/quiet-time policy, and registration cleanup.
- Windows low/high/neutral state mapping, alternating wait-handle selection, and
  partial handle cleanup through an injected API implementation.
- macOS flag mapping, event dispatch to the governor, and source cleanup through
  an injected source implementation.
- Dask adapter behavior through Future/Client doubles: blocking submission,
  non-blocking `try_submit`, managed submission lifetime, completed-but-undrained
  capacity retention, kwargs passthrough, cancellation/error cleanup, bounded
  streaming, draining during pressure, end-of-input, and iterator cleanup.
- processkit facade behavior through module doubles: requested limits, mechanism
  validation, explicit POSIX fallback, sync/async commands, pressure admission,
  and cleanup.
- CLI success/error reporting.

Syntax compilation of package and example modules also passed.

## Installed-wheel validation

The locally built `resource_pressure-0.1.1-py3-none-any.whl` was installed into
an isolated target with `pip --no-index --no-deps`.

- Import/version smoke test passed (`resource_pressure.__version__ == "0.1.1"`).
- Admission pacing smoke test passed from the installed wheel.
- `ManagedSubmission` imported from the installed Dask integration module.
- The full deterministic suite was rerun against the installed wheel with the
  source-tree pythonpath disabled: **104 passed, 3 skipped**.

## Not executed here

1. Real Windows kernel Memory Resource Notification and Job Object behavior.
2. Real macOS libdispatch/sysctl operation.
3. Successful real Linux PSI registration/event delivery; this sandbox does not
   expose `/proc/pressure/memory`.
4. Real Dask or processkit dependency integration; neither optional dependency is
   installed in this environment.
5. Deliberately induced system memory pressure, hard-limit enforcement, long soak
   tests, notification-latency characterization, or a full platform matrix.

The three skips are the native-sensor opt-in module and the two missing optional-
dependency integration modules. No skipped native test is counted as passed.

## Operational interpretation of 0.1.1 changes

`try_submit_managed()` closes the producer-liveness gap identified in 0.1.0:
custom coordinators can refuse new work without blocking the code path that
harvests completed futures. Its lease remains held until explicit result release,
so a stream of quickly completed but retained results cannot continuously reopen
capacity.

`min_admission_interval` mitigates, but cannot mathematically eliminate, the
window between starting memory-heavy work and the operating system reporting
pressure. It is an explicit fixed pacing control. It does not infer safe worker
counts, inspect RAM utilization, revoke running tasks, or guarantee prevention of
OOM from a single large allocation.

## Acceptance before production deployment

Install the intended extras and run the full suite on each target interpreter and
OS. Enable `RESOURCE_PRESSURE_NATIVE=1` for native initialization/teardown smoke
tests. Run `resource-pressure doctor` with the exact host/cgroup permissions used
by the application. Validate real pressure/recovery behavior in disposable VMs,
including startup under pressure, cancelled work, descendant processes, and
container limits. Keep Dask's own spill/pause/termination safeguards enabled.
