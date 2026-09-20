# Primary implementation references

Checked during preparation on September 20, 2026. These references justify API
choices; they do not certify this implementation or its workload-specific policy.

1. Linux kernel, **Pressure Stall Information** — trigger syntax, separate file
   descriptors, polling semantics, unprivileged window multiples, and per-cgroup
   interfaces: https://docs.kernel.org/accounting/psi.html
2. Microsoft, **CreateMemoryResourceNotification** — native low/high objects,
   system-wide semantics, neither-signalled interval, lifetime:
   https://learn.microsoft.com/en-us/windows/win32/api/memoryapi/nf-memoryapi-creatememoryresourcenotification
3. Apple, **libdispatch source.h** — memory-pressure source, flags, creation and
   callback/cancellation API:
   https://github.com/apple/swift-corelibs-libdispatch/blob/main/dispatch/source.h
4. Apple, **XNU kern_memorystatus_notify.c** — sysctl pressure-state bootstrap and
   conversion to dispatch-compatible levels:
   https://github.com/apple-oss-distributions/xnu/blob/main/bsd/kern/kern_memorystatus_notify.c
5. Linux kernel, **Control Group v2** — memory control and delegation background:
   https://docs.kernel.org/admin-guide/cgroup-v2.html
6. processkit, **Process groups** — group construction, public run/output verbs,
   native mechanisms, limits, and containment scope/teardown caveats:
   https://github.com/ZelAnton/processkit-py/blob/main/docs/process-groups.md
7. processkit, **Public type stubs** — constructor and output/aoutput API:
   https://github.com/ZelAnton/processkit-py/blob/main/src/processkit/_processkit.pyi
8. Dask distributed, **Public API** — immediate submit, Future completion
   callbacks, cancellation, and result release:
   https://distributed.dask.org/en/stable/api.html

The PSI thresholds and four-second quiet period in this package are our alpha
policy choices, not copied kernel defaults or empirically validated guarantees.
Windows and macOS pressure semantics are not numerically equated to Linux PSI.
