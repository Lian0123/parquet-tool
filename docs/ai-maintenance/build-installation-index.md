<!-- AI-DOC: owner=maintainers; verified=2026-10-27; sources=package.json,CMakeLists.txt,scripts/install.cjs,.github/workflows/ci.yml,.github/workflows/release.yml,.github/workflows/pages.yml,site,tools/site-converter -->
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

`site/` is a static Traditional Chinese HTML/CSS/JavaScript guide and browser
converter. Recipes run in Node.js. `converter.html` converts CSV/JSON/flat Parquet
in a module worker with local bundled dependencies, independent of the native npm
package and Flask viewer. Its Parquet files currently do not interoperate with the
native CLI; use CSV/JSON between engines (see the risks index).

`tools/site-converter/` owns a separate lockfile and Node.js 24.15+ build/test tools.
Run `npm ci --prefix tools/site-converter`, `npm run build --prefix tools/site-converter`,
and `npm test --prefix tools/site-converter`. Commit generated `site/vendor/` and
license notices. Serving the checked-in website needs no build or npm dependencies.

`.github/workflows/pages.yml` uploads only `site/` to GitHub Pages on matching
`main` pushes or manual dispatch, after rebuilding/verifying the bundle, converter
tests, syntax checks, and docs check. Changes to `tools/site-converter/` also trigger
deployment. Repository Settings → Pages must use GitHub Actions.
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
