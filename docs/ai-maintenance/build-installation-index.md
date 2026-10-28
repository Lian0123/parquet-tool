<!-- AI-DOC: owner=maintainers; verified=2026-10-28; sources=package.json,CMakeLists.txt,scripts/install.cjs,.github/workflows/ci.yml,.github/workflows/release.yml,.github/workflows/pages.yml,site,tools/site-converter -->
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

`site/` contains static Traditional Chinese, English (`en/`) and Japanese (`ja/`)
guides and converters. `src/wasm/browser.cpp` compiles the same `src/native/parquet.h`
and `thrift.h` core as the addon to local WebAssembly. Only flat uncompressed PLAIN
v1 pages are supported; old project schema layouts remain readable. Browser/native
cross-engine and independent PyArrow fixtures cover
format compatibility. See `site/README.md` for rebuild instructions and limits.

`tools/site-converter/` owns a separate lockfile and Node.js 24.15+ build/test tools.
Run `npm ci --prefix tools/site-converter`, `npm run build --prefix tools/site-converter`,
and `npm test --prefix tools/site-converter`. Commit generated `site/vendor/` and
license notices. Serving the checked-in website needs no build or npm dependencies.

`.github/workflows/pages.yml` uploads only `site/` to GitHub Pages on matching
`main` pushes or manual dispatch, after rebuilding/verifying the bundle, converter
tests, syntax checks, and docs check. Changes to `tools/site-converter/` also trigger
deployment. Actions publishing uploads `site/` directly. Existing `main /(root)`
branch publishing is also supported: nine root HTML entry points forward to the
matching `site/` page, preserving the query and recipe hash. `pages-mode.mjs` chooses
the configured publisher; the workflow requests a branch rebuild rather than
racing it with an artifact deployment. Missing `build_type` is handled using the
explicit main-root source. Other source branches/directories require configuration.
`verify-deployment.mjs` checks the live nine pages and exact engine asset hashes
with bounded CDN retries before the workflow reports success. It checks the actual
directory URL as well as `index.html`, requires HTML/JavaScript/WASM MIME types,
and resolves branch redirect destinations before locating engine assets. A rejected
branch rebuild request is logged; live verification still decides success because
the automatic branch publisher can already be running.
Deployment needs `pages: write`, `id-token: write`, and the `github-pages` environment.
Expected project prefix: `/parquet-tool/`; assets must stay relative.

Preview: `python3 -m http.server 4173 --directory site --bind 127.0.0.1`.
Recipe definitions: `site/examples.js`; interactions: `site/app.js`.
Validate copyable snippets against `src/cli/index.ts` and the public library exports.
Full deployment/maintenance instructions are in `site/README.md`.

## Search metadata and static documentation

`tools/site-converter/seo.config.mjs` is the source of canonical base URLs, metadata,
visible FAQ content and manually verified review date. `seo.mjs` generates marked
SEO/FAQ blocks plus `site/guide.html`, `guide.md`, `sitemap.xml` and `llms.txt`; recipe
code comes directly from `site/examples.js`. Run `npm run seo --prefix tools/site-converter`
after edits and `npm run seo:check --prefix tools/site-converter` to detect stale
outputs. The normal browser build regenerates these files, while Pages checks
freshness before building. The share-card PNG is exported separately from its SVG.

For project Pages hosting, `/parquet-tool/robots.txt` is ineffective: robots policy
is controlled at `https://lian0123.github.io/robots.txt` in the separate root site.
This repository changes no host-root crawler/training policy. Public maintenance
and post-deployment Search Console instructions are in `docs/seo.md`.

## Bundled release-tool security mitigation

`npm` bundled under semantic-release contains an `ip-address` version that npm
`overrides` cannot replace. `scripts/patch-bundled-ip-address.cjs` copies the official
patched direct development dependency into that bundle after installation. Run
`npm run security:check` after `npm ci`; CI/release enforce this. Do not disable
install scripts for release tooling. This mitigation does not change the upstream
npm tarball; see `docs/security-dependencies.md` for Dependabot implications.

The root override `npm: ^11.21.1` upgrades the actual bundled
`postcss-selector-parser` from 7.1.4 to 7.1.6. `scripts/check-security-dependencies.cjs`
checks both locked and installed selector-parser versions and the installed
ip-address mitigation. See `docs/security-dependencies.md` for the remaining
upstream ip-address limitation.
