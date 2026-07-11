<!-- AI-DOC: owner=maintainers; verified=2026-07-11; sources=src/lib/index.ts,src/lib/binding.ts,package.json -->
# Change Decision Process

1. Does the change alter a public API, type, CLI flag, or output? Check exports, SemVer, and user documentation.
2. Does it cross the native boundary? Update the TypeScript contract and C++ registration together, then rebuild and run all tests.
3. Does it alter Parquet or conversion semantics? Define round-trip, null, BigInt, schema, and compatibility expectations before coding.
4. Does it affect resources or large files? Check handles, temporary files, buffers, concurrency, and exception cleanup.
5. Is there an existing shared rule? Prefer `conversion.ts`, `rows.ts`, and established patterns to avoid format drift.
6. Is evidence missing? Mark the behavior unknown and verify it with an experiment or test.
