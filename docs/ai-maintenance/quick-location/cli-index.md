<!-- AI-DOC: owner=maintainers; verified=2026-07-11; sources=src/cli/index.ts,tests/cli.test.ts,package.json -->
# CLI Index

The only CLI entry is `src/cli/index.ts`. Commander commands call public exports from `src/lib/index.ts`. The binary is `parquet-tool`; during development use `npm run cli -- <args>` after building.

When commands, flags, defaults, or output change, update `docs/cli.md`, language README pages, and `tests/cli.test.ts`. Preserve machine-readable output and non-zero exit codes on failure.

Find commands with `rg -n "\\.command\\(" src/cli/index.ts`. Minimum check: `npm test -- --runInBand tests/cli.test.ts` and `node dist/cli/index.js --help`.
