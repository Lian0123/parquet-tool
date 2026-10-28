# Release-tool dependency security

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

## postcss-selector-parser (fixed in the dependency graph)

The vulnerable 7.1.4 copy was bundled inside npm 11.19.1, through
`@semantic-release/npm -> npm -> @npmcli/arborist -> @npmcli/query`.
The root npm override now requires `^11.21.1`, whose official tarball bundles
postcss-selector-parser 7.1.6. The lockfile records that actual upstream version;
no postinstall rewriting or invented lock metadata is used for this dependency.

After any dependency refresh, run `npm ci`, `npm run security:check` and
`npm ls npm postcss-selector-parser`. The check rejects vulnerable locked or
installed selector-parser versions. This addresses the selector-parser Dependabot
update conflict after the new lockfile reaches the default branch. It does not
claim all other npm audit alerts are resolved. The separate ip-address workaround
above is still needed: npm 11.21.1 bundles ip-address 10.7.2.
