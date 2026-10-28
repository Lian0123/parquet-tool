# GitHub Pages examples website

Traditional Chinese, English and Japanese usage guides and browser converter for
`parquet-tool`. All runtime assets are local. The converter uses WebAssembly compiled
from this project's `src/native/parquet.h` and `thrift.h`, through `src/wasm/browser.cpp`.

## Preview

From the repository root:

```sh
python3 -m http.server 4173 --directory site --bind 127.0.0.1
```

Open <http://127.0.0.1:4173>. Stop the server with Ctrl+C.
You can also open `site/index.html` directly; when clipboard access is unavailable,
the copy button selects the code for manual copying. The converter requires HTTP(S)
and a modern browser with module workers; open `/converter.html` on the preview
server. Opening its HTML with `file://` displays an explanation instead.

## Browser converter

- CSV ↔ Parquet, JSON ↔ Parquet, CSV ↔ JSON; file picker, drag-and-drop, sample data,
  first-ten-row preview with inferred types, conversion, explicit download, cancel,
  and clear. Processing occurs in a terminable worker; no file upload or telemetry.
- UTF-8 CSV with a header: automatic/comma/semicolon/tab delimiters; optional scalar
  inference. Empty fields (including quoted empty fields) become null. Leading zeros
  and unsafe integers stay strings. CSV output includes a UTF-8 BOM. Formula-like
  strings are prefixed with an apostrophe by default; users may disable this.
- JSON: object arrays or `{ rows: [...] }`; source `schema` metadata is ignored.
  Missing values become null, heterogeneous scalar columns become strings, unsafe
  JSON integers are rejected. Use decimal strings for exact large integers.
- Flat Parquet: uncompressed PLAIN data pages v1, one page per column chunk.
  BOOLEAN, INT32, INT64, FLOAT, DOUBLE and UTF-8 strings are supported. Compression,
  dictionaries, nested/repeated fields and non-UTF8 logical types are rejected.
  Dates remain text. INT64 values export as decimal strings in JSON/CSV.
  Metadata/logical schemas are not preserved.
- Limits: 20 MiB input, 100,000 rows, 200 columns, 2,000,000 cells; WASM memory max
  256 MiB. Processing materializes tables in a cancellable worker.
- Native/WASM share the implementation. New files use standard schema field IDs;
  the reader also accepts the project's earlier schema layout. Independent PyArrow
  fixtures and native/WASM interchange tests cover supported formats.

### Rebuild and test

The generated `site/vendor/` bundle and notices are committed. Rebuilding requires
Node.js 24.15+ and the separate, pinned browser build dependencies:

```sh
npm ci --prefix tools/site-converter
npm run build --prefix tools/site-converter
npm test --prefix tools/site-converter
npm run docs:check
```

For shared C++ changes, install Emscripten **3.1.74**, activate its environment and run:

```sh
npm run build:wasm --prefix tools/site-converter
npm run build --prefix tools/site-converter
npm test --prefix tools/site-converter
```

Commit `project-parquet.mjs`, `.wasm` and `.sources.json` together. Check the source-hash
manifest when shared C++ or the build script changes, and recompile before publishing. Pages serves the committed local WASM and rebuilds the Papa Parse bundle.
The WASM compiler is a development dependency only, outside npm runtime installs.

## Publish to GitHub Pages

The workflow supports both existing publishing configurations:

- **GitHub Actions**: uploads only `site/`, then deploys that artifact at the project
  root. This is the preferred canonical layout.
- **Deploy from a branch — main /(root)**: the root `index.html`, `guide.html`,
  `converter.html` and their `en/` / `ja/` equivalents forward to the real pages in
  `site/`. Query strings and recipe hashes survive. A visible link and meta refresh
  also work without JavaScript. The workflow requests the existing branch publisher
  to rebuild and verifies the result; it does not run a competing artifact deployment.

Push/merge changes to `main` and run **Deploy examples to GitHub Pages**. The previous
hard failure on a branch publishing configuration has been removed. The mode check
only rejects unknown sources, such as a different branch or `/docs`, which do not
contain these root entry points. No admin token or expanded repository permissions
are needed for supported existing configurations. Pages must already be enabled.

Both modes verify all nine public pages, script/style HTTP links, the WASM MIME
type, and exact converter/engine asset hashes against this checkout. Stale README
or error pages returning HTTP 200 do not pass. Bounded retries allow CDN refresh.

When changing publishing mode, use Settings → Pages → Source → GitHub Actions.
The workflow's standard token cannot change that administrative setting itself.
The expected URL is <https://lian0123.github.io/parquet-tool/>. In branch mode the
browser forwards to `/parquet-tool/site/`; in Actions mode it stays at the root.

See the [official custom-workflow guide](https://docs.github.com/en/pages/getting-started-with-github-pages/using-custom-workflows-with-github-pages)
for Pages permissions and configuration.

## Maintain

- `index.html`: layout, quick start, resource links, and no-JavaScript fallback.
- `styles.css`: responsive design, keyboard focus, and reduced-motion preference.
- `examples.js`: recipe metadata and copyable CLI/CommonJS snippets.
- `app.js`: search, category filter, language tabs, permalinks, clipboard fallback.
- `samples/`: matching three-row JSON and CSV fixtures.
- `converter.html`, `converter.css`, `converter.mjs`: converter UI and state.
- `converter-worker.mjs`, `converter-core.mjs`, `converter-shared.mjs`: worker
  protocol, table parsing/export, validation, and shared limits.
- `vendor/`: local project WASM, source hashes, CSV bundle and third-party notices.
- `en/`, `ja/`: generated localized pages, guides and recipe metadata.
- `i18n.js`: localized dynamic statuses and errors.
- `../tools/site-converter/`: pinned build dependencies, bundler, conversion tests,
  and DOM controller tests. Test tools are not published with the website.
- `guide.html`, `guide.md`: generated static versions of all twelve recipes.
- `sitemap.xml`, `llms.txt`: canonical-page sitemap and optional machine-readable
  content index; `social-card.svg` / `.png`: share-card source and exported PNG.
- `../tools/site-converter/seo.config.mjs`, `seo.mjs`: centralized metadata, FAQ
  content, static-guide generator and freshness checker.

## Search and AI-search

Run `npm run seo --prefix tools/site-converter` after changing recipes, metadata or
FAQ copy. `npm run seo:check --prefix tools/site-converter` verifies freshness without
writing. All SEO output is static HTML or text. The browser build regenerates it,
and Pages checks freshness before building. Tests verify exact recipe snippets,
visible FAQ/JSON-LD agreement, canonical/sitemap URLs, real limits, and share images.

See [search maintenance](../docs/seo.md) for deployment checks and external sources.
For this project site, robots policy belongs at the **host root**
`https://lian0123.github.io/robots.txt`; a nested project file would not apply.
No root policy or search-account configuration was changed. Structured data and
the optional LLM index do not guarantee indexing, rankings or AI citations.

Validate recipe behavior against `src/cli/index.ts` and `src/lib/` when editing.
Check desktop/mobile layouts, search with no results, API-only Buffer recipes,
keyboard tab navigation, direct `#example-<id>` links, downloads, and copy behavior.
Use a fresh temporary working directory to execute snippets so append operations
and generated files cannot affect user data. Run `npm run docs:check` after changes.

## Verification scope

Website tests cover conversions, independent standard and legacy fixtures, nine
static language pages, real local links, language alternatives, metadata, freshness,
DOM controls, limits and WASM source hashes. Native tests cover cross-engine file
exchange. DOM tests simulate worker transport; actual browser worker/download and
responsive visual review still require a connected browser.

Remote deployment is separate from local validation. Push/merge to `main`, select
GitHub Actions in Pages settings, run the workflow, then verify the public URLs.
The security mitigation and outstanding upstream Dependabot limitation are documented
in [dependency security](../docs/security-dependencies.md).

### Verification record — 2026-10-28

- Clean root `npm ci` applied the bundled dependency mitigation; security check
  verified both installed copies at 10.7.5.
- Native/TypeScript build, lint, 8 Jest suites / 44 tests and docs check passed.
- 50 website tests, SEO freshness and generated bundle build passed.
- Independent PyArrow 17 read both native and WASM output, including Unicode,
  nulls and booleans. All nine language pages and four engine assets returned 200
  from a temporary HTTP server; WASM MIME type was correct.
- No connected browser provider was available. Actual browser module workers,
  downloads and responsive visual review, Node 18 and cross-platform CI were not
  run locally. The test HTTP server was stopped.
- Live root currently serves the repository/Jekyll site: `guide.html` returns 404,
  while `site/guide.html` returns 200. Local changes have not been pushed/deployed;
  select GitHub Actions as the Pages Source before deploying `site/`.

### Deployment and selector-parser fix — 2026-10-28

- Clean root install, security check, native/TypeScript build, lint and all 44 native
  tests passed. The actual npm 11.21.1 tarball/lock/installation use selector-parser
  7.1.6. The separate installed ip-address mitigation remains at 10.7.5.
- Website build, 54 website tests, SEO freshness, redirect freshness, docs check and
  diff whitespace checks passed. Both publishing modes passed real temporary HTTP
  checks of all nine pages and exact engine assets, including module/WASM MIME.
- The existing production `https://lian0123.github.io/parquet-tool/site/` passed the
  same nine-page and exact asset verification. Root entry points and the workflow
  fix still need to reach `main` before the root URL uses the new behavior.
- Browser inventory had no connected provider; native Brave access timed out after
  20 seconds. Responsive visual review and actual browser module-worker/download
  interaction remain unverified. Cross-platform and Node 18 checks remain in CI.

### Recheck — 2026-10-28

The public root still serves old content and fails the live verification; `/site/`
passes. The verifier now explicitly checks the directory URL and HTML MIME type,
closing the gap where correct `index.html` content could hide a broken root URL.
Branch rebuild-request errors no longer prevent the authoritative live check.

A separate QA harness loaded the actual production worker modules and fetched the
production WASM with the browser execution branch, in an isolated JS VM without
Node process/filesystem. Unicode/null/boolean JSON → Parquet → JSON passed. This
covers module/WASM loading and conversion logic; it is not a visual browser test.
Root entry/workflow changes still have to reach `main` and pass the real workflow.
