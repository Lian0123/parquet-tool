# Examples

Repository examples are organized by workflow so each scenario is isolated in
its own folder.

## Available examples

- `basic-read-write/` - Create a file, append rows, then read rows and metadata
- `merge-and-validate/` - Build multiple files, merge them, and validate the result
- `conversions/` - Convert between CSV, JSON, XML, Parquet, and Arrow IPC
- `split-and-parallel/` - Split large files and process row groups in parallel
- `buffer-roundtrip/` - Read and write raw Parquet bytes with validation

## Run examples

From the repository root:

```bash
npx ts-node examples/basic-read-write/index.ts
npx ts-node examples/merge-and-validate/index.ts
npx ts-node examples/conversions/index.ts
npx ts-node examples/split-and-parallel/index.ts
npx ts-node examples/buffer-roundtrip/index.ts
```

Or from inside `examples/`:

```bash
npm run basic
npm run merge
npm run convert
npm run parallel
npm run buffer
```