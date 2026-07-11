<!-- AI-DOC: owner=maintainers; verified=2026-07-11; sources=package.json,CMakeLists.txt,scripts/install.cjs,.github/workflows/ci.yml,.github/workflows/release.yml -->
# Build and Installation Index

| Goal | Command | Note |
|---|---|---|
| Install | `npm ci` | The install hook may handle native binaries. |
| Full build | `npm run build` | Native build, then TypeScript build. |
| Native build | `npm run build:native` | Requires a CMake/C++ toolchain. |
| TypeScript build | `npm run build:ts` | Does not prove the addon loads. |
| Quality loop | `npm run ci` | Reinstall, build, lint, test, and docs check. |

Addon lookup order is `prebuilds/{platform}-{arch}`, `build/Release`, then `build/Debug`. Published release targets and semantic-release settings are defined in `package.json` and `.github/workflows/release.yml`.
