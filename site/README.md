# GitHub Pages examples website

Buildless Traditional Chinese usage guide for `parquet-tool`. All assets are local;
no npm install, bundler, server API, external fonts, or CDN is needed to serve it.
The browser displays recipes; actual Parquet processing runs in local Node.js.

## Preview

From the repository root:

```sh
python3 -m http.server 4173 --directory site --bind 127.0.0.1
```

Open <http://127.0.0.1:4173>. Stop the server with Ctrl+C.
You can also open `site/index.html` directly; when clipboard access is unavailable,
the copy button selects the code for manual copying.

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
