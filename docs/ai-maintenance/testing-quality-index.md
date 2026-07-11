<!-- AI-DOC: owner=maintainers; verified=2026-07-11; sources=package.json,jest.config.ts,.eslintrc.js,tests,.github/workflows/ci.yml -->
# Testing and Quality Index

| Change | Minimum validation |
|---|---|
| Maintenance Markdown | `npm run docs:check` |
| TypeScript module | `npm run build:ts`, relevant test, `npm run lint` |
| Public API or CLI | Build, relevant tests, lint, docs check |
| Native or format behavior | Native build, TypeScript build, full tests, lint |
| Build or release | `npm run ci` and workflow review |
| Viewer | Python syntax, Docker build, route smoke tests |

Normal, boundary, and error paths need evidence. Check handles, temporary files, buffers, generated artifacts, secrets, and `git diff --check` before completion.

Test map: `roundtrip`, `append`, `features`, `cli`, `splitter`, `parallel`, and `buffer`.
