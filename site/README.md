# GitHub Pages examples website

Traditional Chinese usage guide and browser converter for `parquet-tool`. All
runtime assets are local; no server API, external fonts, or CDN is needed to serve
the checked-in website. Recipes run in Node.js; `converter.html` processes files
directly in a module Web Worker using bundled Hyparquet, Hyparquet Writer and Papa
Parse. The browser engine is separate from the C++ native package.

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
- Flat Parquet: common decompression codecs through hyparquet-compressors; Snappy
  or uncompressed output. Nested and binary columns are rejected. Date values are
  exported as ISO strings, INT64 values as decimal strings in JSON/CSV. This is a
  table conversion tool, not a schema/metadata-preserving round trip.
- Limits: 20 MiB input, 100,000 rows, 200 columns, 2,000,000 cells. Input is read in
  memory; compressed size does not bound decoded memory. Cancel terminates the
  worker. Result Blob URLs are revoked when replaced, cleared, or leaving the page.
- **Current native interoperability limitation:** a standard Hyparquet Writer file
  (both Snappy and uncompressed) fails in the current C++ reader with
  `thrift: unknown type to skip`. Its SchemaElement field IDs in `src/native/parquet.h`
  also differ from the standard schema. Use CSV/JSON to exchange data between this
  browser engine and the current CLI. Native compatibility is outside this change.

### Rebuild and test

The generated `site/vendor/` bundle and notices are committed. Rebuilding requires
Node.js 24.15+ and the separate, pinned browser build dependencies:

```sh
npm ci --prefix tools/site-converter
npm run build --prefix tools/site-converter
npm test --prefix tools/site-converter
npm run docs:check
```

The builder includes local compression implementations and generates third-party
license notices from the bundled packages. No additional WASM fetch or runtime CDN
is used. The Pages workflow rebuilds, verifies the checked-in bundle, and runs the
converter tests before deployment. Root npm runtime dependencies are unaffected.

## Publish to GitHub Pages

1. Push these files to the `main` branch of `Lian0123/parquet-tool`.
2. In the repository's **Settings → Pages → Build and deployment**, set **Source**
   to **GitHub Actions**.
3. Run **Deploy examples to GitHub Pages** from the Actions tab for the first
   deployment, or push a subsequent change under `site/` to trigger it automatically.
4. After deployment succeeds, the expected project URL is
   <https://lian0123.github.io/parquet-tool/>.

The workflow uploads only `site/`. Relative asset and sample paths work under the
`/parquet-tool/` project prefix, as well as on a standalone domain. `.nojekyll`
also allows this folder to be served without Jekyll in a branch-based deployment.
The website is independent of the npm package and the Flask viewer.

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
- `vendor/`: reproducible browser bundle and third-party notices.
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

## Verification record — 2026-10-27

- `npm run ci` passed locally on Node.js 26.5.0: native/TypeScript builds, lint,
  all 7 suites / 40 tests, and maintenance documentation checks.
- All 23 available CLI/CommonJS recipe variants ran in isolated temporary
  directories against the local build. Generated Parquet files were validated;
  row data, conversion outputs, and invalid-file CLI exit status were checked.
- JavaScript syntax, HTML IDs/relative assets/section anchors, download fixtures,
  Pages workflow YAML/permissions/upload directory, and `git diff --check` passed.
- A local HTTP preview returned 200. Browser automation could not establish a
  working session; responsive visual review and browser interaction checks remain
  unverified. The preview server was stopped after validation.
- GitHub Pages was not remotely enabled or deployed. Cross-platform and Node.js 18
  checks were not run locally; the existing CI workflow retains those jobs.

Browser converter extension, verified on the same date:

- Full root `npm run ci` passed again (native/TypeScript builds, lint, 40 existing
  tests, documentation check). All 32 converter/data/DOM/static-asset tests passed
  with `npm test --prefix tools/site-converter` on Node.js 26.5.0.
- Two successive dependency builds produced identical bundle and license-notice
  hashes. Pages YAML, module syntax, relative paths, and `git diff --check` passed.
  The converter page, worker, core, and bundle returned HTTP 200 in local preview.
- DOM tests exercise real conversions with a simulated Worker transport; they do
  not replace actual module-worker or visual browser review. No connected browser
  provider was available. Desktop/mobile visual review and real browser downloads
  remain unverified. The local preview server was stopped after checks.
- Standard writer output was also tried against the current native reader; both
  Snappy and uncompressed files failed as documented under interoperability above.
  No native compatibility changes or remote Pages deployment were performed.

SEO / AI-search extension, verified on the same date:

- `npm test --prefix tools/site-converter` passed all 40 website tests, including
  metadata, JSON-LD/visible FAQ agreement, sitemap, canonical URLs, share-image PNG,
  static recipes/Markdown, local links, and the existing converter/DOM tests.
- Full root `npm run ci` passed: native/TypeScript builds, lint, 40 existing tests,
  and docs checks. SEO freshness, browser bundle build, deterministic SEO generation,
  JavaScript syntax, and `git diff --check` passed. The PNG share card was visually
  inspected locally.
- No connected browser provider was available for visual page review. External
  production pages/root robots could not be retrieved with the web tool. Search
  Console, Bing submission, real indexing/ranking and AI citations were not tested
  or changed. No remote deployment was performed.
