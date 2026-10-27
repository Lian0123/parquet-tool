<!-- AI-DOC: owner=maintainers; verified=2026-10-27; sources=package.json,CMakeLists.txt,scripts/install.cjs,.github/workflows/ci.yml,.github/workflows/release.yml,.github/workflows/pages.yml,site -->
# Build and Installation Index

| Goal | Command | Note |
|---|---|---|
| Install | `npm ci` | The install hook may handle native binaries. |
| Full build | `npm run build` | Native build, then TypeScript build. |
| Native build | `npm run build:native` | Requires a CMake/C++ toolchain. |
| TypeScript build | `npm run build:ts` | Does not prove the addon loads. |
| Quality loop | `npm run ci` | Reinstall, build, lint, test, and docs check. |

Addon lookup order is `prebuilds/{platform}-{arch}`, `build/Release`, then `build/Debug`. Published release targets and semantic-release settings are defined in `package.json` and `.github/workflows/release.yml`.

## Static examples website

`site/` is a buildless Traditional Chinese HTML/CSS/JavaScript guide. It displays
CLI/CommonJS recipes and sample downloads; native Parquet operations run in Node.js,
not in the browser. It has no runtime dependency on the npm package or Flask viewer.

`.github/workflows/pages.yml` uploads only `site/` to GitHub Pages on matching
`main` pushes or manual dispatch. Repository Settings → Pages must use GitHub Actions.
Deployment needs `pages: write`, `id-token: write`, and the `github-pages` environment.
Expected project prefix: `/parquet-tool/`; assets must stay relative.

Preview: `python3 -m http.server 4173 --directory site --bind 127.0.0.1`.
Recipe definitions: `site/examples.js`; interactions: `site/app.js`.
Validate copyable snippets against `src/cli/index.ts` and the public library exports.
Full deployment/maintenance instructions are in `site/README.md`.
