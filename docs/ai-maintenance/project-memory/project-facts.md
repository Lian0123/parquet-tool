<!-- AI-DOC: owner=maintainers; verified=2026-07-11; sources=package.json,tsconfig.json,CMakeLists.txt,src/lib/index.ts,src/lib/binding.ts,viewer/app.py -->
# Project Facts

| Area | Verified fact | Source |
|---|---|---|
| Runtime | Node.js `>=18`; TypeScript ES2020, CommonJS, strict mode | `package.json`, `tsconfig.json` |
| Package entry | JS and types are under `dist/lib`; CLI is `dist/cli/index.js` | `package.json` |
| Native layer | `cmake-js` builds the addon; `node-gyp-build` searches prebuilds, Release, then Debug | `CMakeLists.txt`, `src/lib/binding.ts` |
| Public API | Only symbols exported by `src/lib/index.ts` are public | `src/lib/index.ts` |
| Data model | Public rows are objects; native row groups use column-oriented arrays | `src/lib/types.ts`, `src/lib/rows.ts` |
| Tests | Jest + ts-jest; tests are in `tests/*.test.ts` and require the native addon | `jest.config.ts`, `tests/` |
| CI | Linux, macOS, and Windows build/test; lint runs on Linux; a dedicated Ubuntu job verifies Node 18 compatibility; release builds three prebuild targets | `.github/workflows/` |
| Viewer | Separate Flask/PyArrow/Pandas app; default `/data`, port 8080 | `viewer/`, `docker-compose.yml` |

## Main data flow

```text
Library or CLI -> TypeScript library -> binding.ts contract
-> C++ Node-API addon -> parquet.h format implementation -> .parquet file
```

Readers and writers can still be managed manually with `close()`, but `withReader`, `withWriter`, and `withAppender` are the preferred safe path. Writer close flushes the final row group. Append adds row groups; it does not rewrite existing rows. `parallel*` uses Promise-based buckets and multiple reader handles, not worker threads or separate processes.

## Support boundaries

- Scalars are `boolean | number | bigint | string | null`.
- End-to-end supported physical types are BOOLEAN, INT32, INT64, FLOAT, DOUBLE, and BYTE_ARRAY. Unsupported enum values now fail fast in schema, reader, and writer entry points.
- Published prebuild targets are Linux x64, macOS arm64, and Windows x64. Other platforms may need a source build.
- README files are user documentation. This directory is maintenance navigation; unknown behavior must be verified in source or by execution.
