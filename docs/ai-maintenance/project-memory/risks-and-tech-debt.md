<!-- AI-DOC: owner=maintainers; verified=2026-07-11; sources=src/lib/reader.ts,src/lib/writer.ts,src/lib/parallel.ts,src/lib/types.ts,viewer/app.py,.github/workflows -->
# Risks and Technical Debt

| Level | Observed issue | Impact and validation |
|---|---|---|
| High | Reader and Writer rely on callers to invoke `close()` | Exception paths may leak handles or leave incomplete files; test cleanup. |
| High | Several APIs use `readAll()` | Large files may exhaust memory; evaluate streaming before claiming large-file support. |
| Medium | `parallelWrite` lacks centralized cleanup on failure | Temporary files may remain after an error; add failure-path coverage. |
| Medium | Public `ReadOptions` is not currently applied by Reader methods | Do not claim projection or row-group selection is implemented. |
| Medium | Enum members do not guarantee INT96/FIXED_LEN_BYTE_ARRAY support | Verify every format path before documenting support. |
| Medium | Viewer parses `limit` directly and reads before `head()` | Invalid input can return 500 and large files are still fully read. |
| Medium | CI validates newer Node versions than the minimum engine | Node 18 compatibility lacks continuous evidence. |
| Low | CLI version is hard-coded and may drift from `package.json` | `--version` can be inaccurate after release. |
| Low | Viewer has no automated tests | Path safety, limits, and route regressions need smoke tests. |

Security baseline: constrain file paths to trusted roots, treat external formats as untrusted, never commit secrets, and release resources on success and failure paths.
