# Changelog

## 0.1.1 — 2026-09-20

Producer-safety release. Added non-blocking Dask submission (`try_submit`) and
managed submissions (`submit_managed` / `try_submit_managed`) for coordinators
that must keep draining completed futures while pressure closes admission. A
managed submission keeps its governor lease until the caller explicitly releases
the consumed result, bounding completed-but-retained results as well as unfinished
work. Added optional `min_admission_interval` pacing to narrow the native-sensor
notification window when a large configured ceiling could otherwise be filled
immediately. Pacing is a fixed start-rate limit, not RAM estimation or adaptive
concurrency. Added a generic custom-producer Dask example and expanded tests.

## 0.1.0 — 2026-09-20

Initial alpha implementation: native Windows low/high memory-resource notification
backend; Linux PSI trigger backend with explicit stall-time policy and multiple
scopes; macOS libdispatch backend with native semantic state bootstrap; sync and
async admission leases; explicit sensor/containment failures; public-API Dask
submission and bounded streaming; optional processkit containment facade; CLI,
examples, deterministic tests, and opt-in real integration smoke tests.

Initial PyPI release. The test report separates deterministic validation from
native/third-party validation.
