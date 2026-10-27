<!-- AI-DOC: owner=maintainers; verified=2026-10-27; sources=src/lib/reader.ts,src/lib/writer.ts,src/lib/parallel.ts,src/lib/types.ts,src/native/parquet.h,viewer/app.py,.github/workflows,site/converter-core.mjs -->
# Risks and Technical Debt

| Level | Observed issue | Impact and validation |
|---|---|---|
| High | Native Parquet SchemaElement field IDs differ from standard Parquet; standard Hyparquet Writer output fails with `thrift: unknown type to skip` in both tested compression modes | Browser converter uses an independent standard engine. Exchange CSV/JSON with the current CLI. Native interoperability needs a separate compatibility change and external reader/writer fixtures. |
| Medium | Browser converter materializes full tables in worker memory | Input/row/column/cell limits reduce normal load, but decoded memory may exceed compressed size. Cancel terminates the worker. Nested/binary fields are rejected; metadata and logical types are not preserved. |
| Low | Reader and Writer still expose manual `close()` lifecycles | `withReader`, `withWriter`, and `withAppender` cover the preferred safe path; keep using callback helpers in new code. |
| Low | `parallelWrite` still materializes chunk files before the final merge | Failure-path temp cleanup is now centralized, but the design still duplicates IO and disk usage for large jobs. |
| Low | Viewer still reads a full Arrow table before preview conversion | Invalid `limit` and path traversal are now guarded, but preview reads are still not row-group streaming. |
| Low | Viewer has no automated tests | Path safety, limits, and route regressions need smoke tests. |

Security baseline: constrain file paths to trusted roots, treat external formats as untrusted, never commit secrets, and release resources on success and failure paths.
