<!-- AI-DOC: owner=maintainers; verified=2026-07-11; sources=src/lib/csv.ts,src/lib/arrow.ts,src/lib/json.ts,src/lib/xml.ts,src/lib/conversion.ts,src/lib/buffer.ts,src/lib/splitter.ts,src/lib/merge.ts,src/lib/validate.ts,src/lib/parallel.ts -->
# Conversion and Processing Index

| Area | Source | Main concerns |
|---|---|---|
| CSV | `csv.ts` | Header, delimiter, inference, coercion |
| Arrow | `arrow.ts` | Type mapping and schema inference |
| JSON/XML | `json.ts`, `xml.ts`, `conversion.ts` | Schema preservation, BigInt, tags |
| Buffer | `buffer.ts` | Temporary files, overwrite, validation |
| Split/merge | `splitter.ts`, `merge.ts` | Naming, row counts, schema compatibility |
| Validation | `validate.ts` | Invalid files must produce issues and `valid: false` |
| Parallel | `parallel.ts` | Ordering, concurrency, closing readers, cleanup |

Shared coercion and row conversion belongs in `conversion.ts` and `rows.ts`. Search with `rg -n "inferSchema|coerce|serialize|tempFile|validateSchema|readParquetRows" src/lib tests`.
