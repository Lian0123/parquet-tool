# parquet-tool

[![npm version](https://img.shields.io/npm/v/parquet-tool.svg)](https://www.npmjs.com/package/parquet-tool) [![license](https://img.shields.io/npm/l/parquet-tool.svg)](LICENSE) [![build status](https://img.shields.io/github/actions/workflow/status/Lian0123/parquet-tool/ci.yml?branch=main)](https://github.com/Lian0123/parquet-tool/actions)

A Parquet processing toolkit built with TypeScript + C++ Native Addon.
It does not depend on existing npm parquet packages; core Parquet read/write logic is implemented in this repository.

Language docs:

- Chinese (Traditional): [docs/README.zh-TW.md](https://github.com/Lian0123/parquet-tool/blob/main/docs/README.zh-TW.md)
- Japanese: [docs/README.ja.md](https://github.com/Lian0123/parquet-tool/blob/main/docs/README.ja.md)

## Features

- Create and read/write Parquet files
- Append mode (called "apply" in earlier requirements) for adding new row groups to existing files
- Merge multiple Parquet files with schema compatibility checks
- Validate Parquet structure, metadata, and row groups
- Convert between CSV and Parquet
- Convert between Apache Arrow IPC and Parquet
- Split large Parquet files into smaller files
- Parallel read/process/write helpers
- Debug mode for CLI and library APIs
- CLI commands: `info`, `read`, `write`, `append`, `split`, `merge`, `validate`, `csv-to-parquet`, `parquet-to-csv`, `arrow-to-parquet`, `parquet-to-arrow`
- Docker Compose viewer for quick verification

## Quick Start

```bash
npm install
npm run build
```

## Examples

### 1. Basic write, read, and append

```ts
import { ParquetReader, ParquetWriter, Schema } from 'parquet-tool';

const schema = Schema.create({
  id: 'INT32',
  name: 'STRING',
  score: { type: 'DOUBLE', optional: true },
});

const writer = new ParquetWriter('output.parquet', schema);
writer.write([
  { id: 1, name: 'Alice', score: 98.5 },
  { id: 2, name: 'Bob' },
]);
writer.close();

const appender = ParquetWriter.openForAppend('output.parquet');
appender.write({ id: 3, name: 'Charlie', score: 75.0 });
appender.close();

const reader = ParquetReader.open('output.parquet');
const all = reader.readAll();
console.log(all.numRows, all.columns);
reader.close();
```

### 2. Merge and validate

```ts
import { mergeParquetFiles, validateParquetFile } from 'parquet-tool';

mergeParquetFiles(['part-1.parquet', 'part-2.parquet'], 'merged.parquet');

const report = validateParquetFile('merged.parquet');
if (!report.valid) {
  console.error(report.issues);
}
```

### 3. CSV and Arrow conversions

```ts
import {
  arrowToParquet,
  csvToParquet,
  parquetToArrow,
  parquetToCsv,
} from 'parquet-tool';

csvToParquet('input.csv', 'input.parquet');
parquetToCsv('input.parquet', 'roundtrip.csv');

parquetToArrow('input.parquet', 'input.arrow');
arrowToParquet('input.arrow', 'from-arrow.parquet');
```

### 4. Split and parallel processing

```ts
import {
  parallelProcess,
  parallelRead,
  splitParquetFile,
} from 'parquet-tool';

const files = splitParquetFile('large.parquet', {
  maxRowsPerFile: 100_000,
  outputDir: './parts',
  prefix: 'large',
});
console.log(files);

const combined = await parallelRead('large.parquet', { concurrency: 4 });
console.log(combined.numRows);

const names = await parallelProcess(
  'large.parquet',
  (rows) => rows.map((row) => String(row.name ?? '')),
  { concurrency: 4 },
);
console.log(names.length);
```

### 5. Run the repository example

```bash
npx ts-node examples/example.ts
```

This generates `examples/sample.parquet` and prints metadata and contents.

## CLI Usage

```bash
# Metadata
npx parquet-tool info data.parquet

# Read rows
npx parquet-tool read data.parquet --json
npx parquet-tool read data.parquet --limit 50

# Write from JSON
npx parquet-tool write out.parquet -i input.json -s "id:INT32,name:STRING"

# Append rows
npx parquet-tool append out.parquet -i more.json

# Split / Merge
npx parquet-tool split large.parquet -n 10000 -o ./output
npx parquet-tool merge merged.parquet part1.parquet part2.parquet

# Validation
npx parquet-tool validate merged.parquet

# CSV <-> Parquet
npx parquet-tool csv-to-parquet input.csv output.parquet
npx parquet-tool parquet-to-csv output.parquet output.csv

# Arrow <-> Parquet
npx parquet-tool arrow-to-parquet input.arrow output.parquet
npx parquet-tool parquet-to-arrow output.parquet output.arrow

# Debug mode
npx parquet-tool --debug validate data.parquet
```

## Docker Viewer

```bash
mkdir -p data
cp your_file.parquet data/
docker-compose up --build
```

Open `http://localhost:8080`.

## Development

```bash
npm install
npm run build:native
npm run build:ts
npm test
npm run clean
```

## Release

This project uses Commitizen + semantic-release.

```bash
npm run cz
npm run release
```

Configured semantic-release plugins:

- `@semantic-release/commit-analyzer`
- `@semantic-release/release-notes-generator`
- `@semantic-release/changelog`
- `@semantic-release/npm`
- `@semantic-release/github`
- `@semantic-release/git`

Branch strategy:

- `main`: stable releases

## Supported Types

| Parquet Type | TypeScript Type | Notes |
|---|---|---|
| BOOLEAN | boolean | Boolean values |
| INT32 | number | 32-bit integer |
| INT64 | bigint | 64-bit integer |
| FLOAT | number | 32-bit float |
| DOUBLE | number | 64-bit float |
| BYTE_ARRAY | string | UTF-8 string |

## License

MIT
