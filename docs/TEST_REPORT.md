# Validation report — resource-pressure 0.1.0

Prepared September 20, 2026. This is an alpha implementation, not a
production-ready or platform-certified release.

## Executed in this delivery environment

Environment: Linux execution sandbox, CPython 3.13.5.

```
python -m pytest -q -ra
92 passed, 3 skipped
```

The passing tests exercise real Python implementation code with deterministic
native-interface or external-library doubles where necessary:

- State transitions, existing-work preservation, bounded sync/async concurrency,
  timeout validation, cancellation, idempotent lease release, subscriber error
  isolation, explicit failure/close wakeup, and fork-reuse rejection.
- Linux trigger formatting, separate fd ownership, real backend control flow
  with simulated poll events, warmup/quiet-time policy, and partial registration
  cleanup. Missing PSI is tested as an error rather than a fake NORMAL state.
- Windows low/high/neutral state mapping, alternating wait-handle selection, and
  partial handle cleanup through an injected API implementation.
- macOS flag mapping, event dispatch to the governor, and source cleanup through
  an injected source implementation.
- Dask public-API adapter behavior through Future/Client doubles: admission,
  completion/cancellation release, bounded streaming, result draining while
  pressured, end-of-input handling, iterator cleanup, and error paths.
- processkit facade behavior through module doubles: requested limits,
  mechanism validation, explicit POSIX fallback, no false hard-limit guarantee,
  sync/async commands, pressure admission, and cleanup.
- CLI success/error reporting.

Also executed: syntax compilation of package/examples, the runnable injected-
pressure demo, and an actual native `doctor` invocation. The native doctor
correctly reports that `/proc/pressure/memory` is absent in this sandbox.

Packaging validation is recorded in `PACKAGING_REPORT.md` after the build.

## Not executed here

1. Real Windows kernel notification and Job Object behavior. There is no Windows
   kernel in this environment. ctypes ABI correctness and real pressure/recovery
   delivery are not established by the simulated tests.
2. Real macOS libdispatch/sysctl operation. There is no Darwin kernel here.
   Callback lifetime cleanup is implemented, but actual native runtime behavior
   remains unverified.
3. Successful real Linux PSI registration/event delivery. This sandbox exposes
   no `/proc/pressure/memory`; no privileged or delegated cgroup was supplied.
4. Real Dask or processkit dependency integration. Neither is installed; package
   installation was attempted but package-index DNS/network access was
   unavailable. Their tests are included and skipped explicitly.
5. Real memory-limit enforcement, deliberately induced pressure, long-duration
   soak tests, latency/performance characterization, and a full platform matrix.

The three reported skips are the native-sensor opt-in test and the two missing-
optional-dependency test modules. No skipped native test is counted as passed.
The CI workflow is supplied but was not executed on hosted runners here.

## Acceptance before deployment

Install the intended extras and run the full suite on the target interpreter.
Enable `RESOURCE_PRESSURE_NATIVE=1` for real initialization/teardown tests.
Run `resource-pressure doctor` with the exact host/cgroup permissions used by the
application. Confirm that the reported containment mechanism is the intended
Job Object or cgroup v2, not a weaker fallback.

In disposable VMs, validate OS-generated pressure/recovery transitions and
process-tree limit/cleanup behavior, including startup under pressure, monitor
failure, cancelled work, descendant processes, and outer-container budgets.
Validate Linux stall thresholds against the actual workload; their defaults are
policy choices, not measured optimum values. Retain Dask's built-in safeguards.
