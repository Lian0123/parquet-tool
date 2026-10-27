<!-- AI-DOC: owner=maintainers; verified=2026-10-27; sources=package.json,jest.config.ts,.eslintrc.js,tests,.github/workflows/ci.yml,site,tools/site-converter,.github/workflows/pages.yml -->
# Testing and Quality Index

| Change | Minimum validation |
|---|---|
| Maintenance Markdown | `npm run docs:check` |
| TypeScript module | `npm run build:ts`, relevant test, `npm run lint` |
| Public API or CLI | Build, relevant tests, lint, docs check |
| Native or format behavior | Native build, TypeScript build, full tests, lint |
| Build or release | `npm run ci` and workflow review, including the dedicated Node 18 job |
| Viewer | Python syntax, Docker build, route smoke tests |
| Static examples website | JS syntax and local asset/link checks; execute changed CLI/CommonJS recipes in a temporary directory; inspect responsive layout, search/empty state, tabs, copy/downloads, and permalinks when browser access is available; docs check |
| Browser converter | Rebuild bundled dependencies, `npm test --prefix tools/site-converter` (real format round trips, null/precision/CSV cases, invalid inputs, limits, DOM state/cancel/download); verify bundle reproducibility; inspect browser layout/worker behavior when browser access is available; docs check |
| SEO / AI-search content | `npm run seo:check --prefix tools/site-converter`; website tests cover metadata, canonical/sitemap URLs, social PNG, visible FAQ/JSON-LD agreement, complete static recipes, actual engine limits and local links. External indexing/citations require post-deployment observation; docs check |

Normal, boundary, and error paths need evidence. Check handles, temporary files, buffers, generated artifacts, secrets, and `git diff --check` before completion.

Test map: `roundtrip`, `append`, `features`, `cli`, `splitter`, `parallel`, and `buffer`.
