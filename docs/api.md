# API Reference

This page documents the public APIs exported from the library entry.

If you use TypeScript, you'll also get inline API docs via TSDoc.

## Schema

Build a schema definition used for writing/reading.

```ts
import { Schema, ParquetType } from 'parquet-tool';

const schema = Schema.create({
  id: 'INT32',
  name: { type: 'STRING', optional: true },
});
```

### Functions

- `Schema.create(definition)` – create a schema from a plain object
- `Schema.parseType(type)` – parse a string type name into `ParquetType`

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

### Methods

- `new ParquetWriter(filePath, schema, options?)` – create a new file writer
- `write(rowOrRows)` – write one row or a batch of rows
- `flush()` – force flush buffered rows into a row group
- `close()` – flush & close the file
- `ParquetWriter.openForAppend(filePath, options?)` – append new row groups to an existing file

## ParquetReader

```ts
const reader = ParquetReader.open('out.parquet');
console.log(reader.getMetadata());
const data = reader.readAll();
reader.close();
```

### Methods

- `ParquetReader.open(filePath)` – open a file reader
- `getMetadata()` – file-level metadata (schema, row groups, row counts)
- `getSchema()` – schema only
- `readRowGroup(index)` – read a specific row group
- `readAll()` – read and merge all row groups
- `readRows()` – read as row-oriented objects
- `close()` – close and release native resources
- `ParquetReader.readMetadata(filePath)` – metadata-only read (no full reader)

## Buffer utilities

Raw byte-level conversion helpers.

- `paquetToBuffer(filePath, options?)` – read a Parquet file as `Buffer` (optionally validate first)
- `bufferToPaquet(buffer, filePath, options?)` – write a `Buffer` to a Parquet file (optionally validate after writing)

## Utilities

### File helpers

- `splitParquetFile(inputPath, options?)` – split a file into multiple smaller Parquet files
- `mergeParquetFiles(inputPaths, outputPath, options?)` – merge multiple files (with optional schema validation)
- `validateParquetFile(filePath)` – validate structure, schema, and row groups

### CSV

- `csvToParquet(csvPath, parquetPath, options?)` – convert CSV → Parquet (optional schema inference)
- `parquetToCsv(parquetPath, csvPath, options?)` – convert Parquet → CSV

### JSON

- `jsonToParquet(jsonPath, parquetPath, options?)` – convert JSON → Parquet (accepts either an array of rows or a `{ schema, rows }` document)
- `parquetToJson(parquetPath, jsonPath, options?)` – convert Parquet → JSON (includes schema by default so round-trip can preserve types)

### XML

- `xmlToParquet(xmlPath, parquetPath, options?)` – convert XML → Parquet (accepts row data with optional embedded schema)
- `parquetToXml(parquetPath, xmlPath, options?)` – convert Parquet → XML (includes schema by default so round-trip can preserve types)

### Arrow IPC

- `arrowToParquet(arrowPath, parquetPath, options?)` – convert Arrow IPC → Parquet
- `parquetToArrow(parquetPath, arrowPath)` – convert Parquet → Arrow IPC

### Parallel helpers

- `parallelRead(filePath, options?)` – read row groups concurrently into a merged result
- `parallelProcess(filePath, processor, options?)` – process row groups concurrently with a user function
- `parallelWrite(filePath, schema, dataChunks, options?)` – write chunks concurrently via temp files, then merge

## Debug mode

```ts
import { configureDebugMode } from 'parquet-tool';

configureDebugMode({ enabled: true });
```

You can also set `PARQUET_TOOL_DEBUG=1` before running your application.

For type definitions see `src/lib/types.ts`.
