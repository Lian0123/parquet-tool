# Release-tool dependency mitigation

The reported `ip-address <10.7.4` is bundled inside the development-only `npm`
package used by `@semantic-release/npm`. It is not part of the browser bundle or
published application dependencies. npm overrides do not replace bundled tarball
contents. Tested upstream npm 11.21.1 and 12.2.1 still bundle 10.7.2.

A direct development dependency on official `ip-address ^10.7.4` supplies the fixed
package (currently locked to 10.7.5). The postinstall script copies that package,
including its license, over the vulnerable bundled copy. `npm run security:check`
verifies the actual installed copies; CI and release jobs run it after installation.
Runtime-only consumer installs skip the patch when the development npm bundle is
absent. Installations using `--ignore-scripts` must execute the patch before release
tools are used; otherwise the security check fails.

The lockfile truthfully retains the upstream bundle version. It does not claim the
original npm tarball contains the patched dependency. Consequently Dependabot can
still report the bundled dependency and fail to propose an upstream version bump.
This is an installation-time mitigation, not a complete upstream/Dependabot fix.
Do not dismiss the alert on the assumption that overrides resolved it. Remove this
workaround once a supported upstream npm release ships `ip-address >=10.7.4`, then
refresh the lockfile and verify a clean install without the patch.

Validation: `npm ci`, `npm run security:check`. Do not commit `node_modules`.
