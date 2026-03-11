# API Reference

## Schema

Build a schema definition used for writing/reading.

```ts
import { Schema, ParquetType } from 'parquet-tool';

const schema = Schema.create({
  id: 'INT32',
  name: { type: 'STRING', optional: true },
});
```

## ParquetWriter

```ts
const writer = new ParquetWriter('out.parquet', schema, { rowGroupSize: 10000 });
writer.write({ id: 1, name: 'Alice' });
writer.close();

// append mode
const appender = ParquetWriter.openForAppend('out.parquet');
appender.write([{ id: 2 }, { id: 3 }]);
appender.close();
```

## ParquetReader

```ts
const reader = ParquetReader.open('out.parquet');
console.log(reader.getMetadata());
const data = reader.readAll();
reader.close();
```

## Utilities

- `splitParquetFile(input, options)` – splits file into smaller pieces
- `parallelRead(file, options)` – reads row groups in parallel
- `parallelProcess(file, processor, options)` – process row groups concurrently
- `parallelWrite(file, schema, chunks, options)` – write chunks in parallel

For type definitions see `src/lib/types.ts`.
