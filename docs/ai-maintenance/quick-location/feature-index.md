<!-- AI-DOC: owner=maintainers; verified=2026-07-11; sources=src,tests,viewer -->
# Feature Index

| Responsibility | Implementation | Tests | Search terms |
|---|---|---|---|
| Public exports | `src/lib/index.ts` | All library tests | `export {` |
| Schema and types | `src/lib/schema.ts`, `types.ts` | `roundtrip.test.ts` | `ParquetType`, `Schema.create` |
| Read, write, append | `reader.ts`, `writer.ts`, `rows.ts` | `roundtrip`, `append` | `readRowGroup`, `openForAppend` |
| Native contract | `binding.ts`, `src/native/` | Indirectly covered | `NativeAddon`, `NODE_API_MODULE` |
| Conversion | `csv.ts`, `arrow.ts`, `json.ts`, `xml.ts`, `conversion.ts` | `features`, `cli` | `ToParquet`, `parquetTo` |
| Buffer | `buffer.ts` | `buffer.test.ts` | `parquetToBuffer`, `bufferToParquet` |
| Split, merge, validate | `splitter.ts`, `merge.ts`, `validate.ts` | `splitter`, `features` | `splitParquetFile`, `validateParquetFile` |
| Parallel processing | `parallel.ts` | `parallel.test.ts` | `parallelRead`, `parallelWrite` |
| CLI | `src/cli/index.ts` | `cli.test.ts` | `.command(` |
| Viewer | `viewer/app.py` | None | `PARQUET_DIR`, `/api/read` |
