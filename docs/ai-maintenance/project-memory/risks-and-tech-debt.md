<!-- AI-DOC: owner=maintainers; verified=2026-07-11; sources=src/lib/reader.ts,src/lib/writer.ts,src/lib/parallel.ts,src/lib/types.ts,viewer/app.py,.github/workflows -->
# Risks and Technical Debt

| Level | Observed issue | Impact and validation |
|---|---|---|
| Low | Reader and Writer still expose manual `close()` lifecycles | `withReader`, `withWriter`, and `withAppender` cover the preferred safe path; keep using callback helpers in new code. |
| Resolved | Eager whole-file reads are no longer the default maintenance path | `iterateRowGroups`, `iterateRows`, CLI preview streaming, and `parallelWrite` merge-by-row-group now avoid `readAll()` as the default path. `readAll()` remains intentionally eager. |
| Low | `parallelWrite` still materializes chunk files before the final merge | Failure-path temp cleanup is now centralized, but the design still duplicates IO and disk usage for large jobs. |
| Resolved | `ReadOptions` now drive reader projection and row-group selection | `readRowGroup`, `readAll`, `readRows`, `iterateRowGroups`, and `iterateRows` share one validation path. |
| Resolved | Unsupported enum members now fail fast with explicit errors | The public enum remains spec-shaped, but schema creation, reader open, and writer paths now reject unsupported physical types consistently. |
| Low | Viewer still reads a full Arrow table before preview conversion | Invalid `limit` and path traversal are now guarded, but preview reads are still not row-group streaming. |
| Resolved | CI now includes a dedicated Node 18 compatibility job | The primary matrix stays on current OS targets and a separate Ubuntu job continuously verifies the minimum supported runtime. |
| Resolved | CLI version is read from `package.json` at runtime | Keep the regression test for `--version` aligned with packaging changes. |
| Low | Viewer has no automated tests | Path safety, limits, and route regressions need smoke tests. |

Security baseline: constrain file paths to trusted roots, treat external formats as untrusted, never commit secrets, and release resources on success and failure paths.
