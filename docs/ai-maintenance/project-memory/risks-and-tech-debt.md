<!-- AI-DOC: owner=maintainers; verified=2026-10-28; sources=src/lib/reader.ts,src/lib/writer.ts,src/lib/parallel.ts,src/lib/types.ts,src/native/parquet.h,viewer/app.py,.github/workflows,site/converter-core.mjs -->
# Risks and Technical Debt

| Level | Observed issue | Impact and validation |
|---|---|---|
| Medium | Native and WASM readers support only flat uncompressed PLAIN data pages v1, one page per column chunk | Standard schema field IDs are fixed; legacy schema files remain readable. Unsupported compression/encoding is explicitly rejected. Independent standard fixtures and native/WASM interchange tests guard compatibility. |
| Medium | Browser converter materializes full tables in worker memory | Input/row/column/cell limits reduce normal load, but decoded memory may exceed compressed size. Cancel or a 60-second busy-stage timeout terminates the worker. The worker must keep synchronous handler registration before dynamic engine initialization. Nested/binary fields are rejected; metadata and logical types are not preserved. |
| Low | Reader and Writer still expose manual `close()` lifecycles | `withReader`, `withWriter`, and `withAppender` cover the preferred safe path; keep using callback helpers in new code. |
| Low | `parallelWrite` still materializes chunk files before the final merge | Failure-path temp cleanup is now centralized, but the design still duplicates IO and disk usage for large jobs. |
| Low | Viewer still reads a full Arrow table before preview conversion | Invalid `limit` and path traversal are now guarded, but preview reads are still not row-group streaming. |
| Low | Viewer has no automated tests | Path safety, limits, and route regressions need smoke tests. |

Security baseline: constrain file paths to trusted roots, treat external formats as untrusted, never commit secrets, and release resources on success and failure paths.
