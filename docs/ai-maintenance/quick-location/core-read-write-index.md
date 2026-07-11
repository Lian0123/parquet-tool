<!-- AI-DOC: owner=maintainers; verified=2026-07-11; sources=src/lib/reader.ts,src/lib/writer.ts,src/lib/schema.ts,src/lib/types.ts,src/lib/rows.ts -->
# Core Read/Write Index

`Schema.create` and `Schema.addColumn` require at least one column and reject unsupported physical types. `ParquetWriter.write` buffers rows and flushes row groups; `close()` finalizes the file. `openForAppend` reuses the file schema and adds row groups. `ParquetReader` exposes metadata, eager reads, lazy row-group/row iterators, and `ReadOptions`; a closed reader cannot be used.

Column arrays and `numRows` must stay consistent. Public type changes affect declarations, converters, and the native contract.

Minimum checks: `npm run build:ts` and `npm test -- --runInBand tests/roundtrip.test.ts tests/append.test.ts`.

Search: `rg -n "ParquetReader|ParquetWriter|Schema.create|ParquetType|RowGroupData" src tests`.
