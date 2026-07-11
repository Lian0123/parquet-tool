<!-- AI-DOC: owner=maintainers; verified=2026-07-11; sources=scripts/check-ai-docs.cjs,docs/ai-maintenance/entry.md -->
# Documentation Protocol

Update the relevant index when modules, symbols, runtime, architecture, data flow, risks, CI, release targets, or maintenance rules change. User-visible behavior also updates the appropriate README, API/CLI docs, or CHANGELOG.

Every maintenance Markdown file starts with:

```text
<!-- AI-DOC: owner=maintainers; verified=YYYY-MM-DD; sources=repository paths -->
```

`verified` is the last source-verification date. `sources` must point to existing repository paths. Run `npm run docs:check`; it checks metadata, sources, links, reachability from the entry, and a 120-day freshness limit.

Create a new page only when the entry cannot locate an independent responsibility area within two links. Keep each page single-purpose and link to authoritative content instead of copying implementation details.
