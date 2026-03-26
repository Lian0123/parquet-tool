import * as fs from 'fs';
import * as path from 'path';
import {
  parallelProcess,
  parallelRead,
  parallelWrite,
  Schema,
  splitParquetFile,
} from '../../src/lib';

async function main(): Promise<void> {
  const schema = Schema.create({
    id: 'INT32',
    name: 'STRING',
    region: 'STRING',
  });

  const sourcePath = path.join(__dirname, 'large.parquet');
  const partsDir = path.join(__dirname, 'parts');
  const tempDir = path.join(__dirname, 'tmp');

  fs.mkdirSync(partsDir, { recursive: true });
  fs.mkdirSync(tempDir, { recursive: true });

  const chunks = [
    [
      { id: 1, name: 'Alice', region: 'apac' },
      { id: 2, name: 'Bob', region: 'emea' },
      { id: 3, name: 'Charlie', region: 'amer' },
    ],
    [
      { id: 4, name: 'Diana', region: 'apac' },
      { id: 5, name: 'Evan', region: 'emea' },
      { id: 6, name: 'Fiona', region: 'amer' },
    ],
    [
      { id: 7, name: 'George', region: 'apac' },
      { id: 8, name: 'Helen', region: 'emea' },
      { id: 9, name: 'Ian', region: 'amer' },
    ],
  ];

  await parallelWrite(sourcePath, schema, chunks, { concurrency: 2, tempDir });

  const files = splitParquetFile(sourcePath, {
    maxRowsPerFile: 4,
    outputDir: partsDir,
    prefix: 'large',
  });

  const combined = await parallelRead(sourcePath, { concurrency: 2 });
  const names = await parallelProcess(
    sourcePath,
    (rows) => rows.map((row) => String(row.name ?? '')),
    { concurrency: 2 },
  );

  console.log('source:', sourcePath);
  console.log('split files:', files);
  console.log('total rows:', combined.numRows);
  console.log('names:', names);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});