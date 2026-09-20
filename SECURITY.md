# Security and safety scope

This library is not a security sandbox and cannot guarantee that the host will
avoid OOM or remain responsive. Native pressure notification may arrive too late
for a sudden allocation. Containment/resource limits depend on the selected
processkit mechanism and deployment permissions; POSIX process groups are not
hard memory containment.

Never install a different package from an unverified index solely because its
name matches this local project. No PyPI publication or signing is claimed here.
Review and test the delivered alpha before deployment. Keep dependencies updated
within your validated compatibility policy.

Do not grant full administrative privileges to the entire application just to
obtain writable PSI or cgroup access. Use administrator-provisioned delegation or
a narrowly scoped service design. No privilege elevation is performed here.

Do not deliberately exhaust a shared/production host to test memory-pressure
handling. Validate real event behavior and hard limits in isolated disposable
VMs with independent control/monitoring and deployment-specific budgets.
