import * as os from 'os';
import * as path from 'path';
import * as fs from 'fs';
import { ParquetReader } from './reader';
import { ParquetWriter } from './writer';
import { ParquetSchema, RowGroupData } from './types';

export interface ParallelReadOptions {
  /** Number of concurrent readers (defaults to min(cpus, 4)). */
  concurrency?: number;
}

/**
 * Read multiple row groups in parallel by opening independent reader
 * instances (each with its own file handle).
 */
export async function parallelRead(
  filePath: string,
  options: ParallelReadOptions = {},
): Promise<RowGroupData> {
  const concurrency = options.concurrency ?? Math.min(os.cpus().length, 4);

  const reader = ParquetReader.open(filePath);
  const metadata = reader.getMetadata();
  const schema = reader.getSchema();
  reader.close();

  const numRG = metadata.numRowGroups;
  const results: RowGroupData[] = new Array(numRG);

  // Distribute row groups across workers
  const buckets: number[][] = Array.from({ length: concurrency }, () => []);
  for (let i = 0; i < numRG; i++) {
    buckets[i % concurrency].push(i);
  }

  await Promise.all(
    buckets.map(async (indices) => {
      if (indices.length === 0) return;
      const r = ParquetReader.open(filePath);
      for (const idx of indices) {
        results[idx] = r.readRowGroup(idx);
      }
      r.close();
    }),
  );

  // Merge
  const columns: Record<string, any[]> = {};
  for (const col of schema.columns) {
    columns[col.name] = [];
  }
  let totalRows = 0;
  for (const rg of results) {
    if (!rg) continue;
    for (const [name, values] of Object.entries(rg.columns)) {
      columns[name].push(...values);
    }
    totalRows += rg.numRows;
  }

  return { numRows: totalRows, columns };
}

/**
 * Process row groups in parallel with a user‑supplied function.
 */
export async function parallelProcess<T>(
  filePath: string,
  processor: (rows: Record<string, any>[]) => T[],
  options: { concurrency?: number } = {},
): Promise<T[]> {
  const concurrency = options.concurrency ?? Math.min(os.cpus().length, 4);

  const reader = ParquetReader.open(filePath);
  const metadata = reader.getMetadata();
  reader.close();

  const numRG = metadata.numRowGroups;
  const allResults: T[][] = new Array(numRG);

  const buckets: number[][] = Array.from({ length: concurrency }, () => []);
  for (let i = 0; i < numRG; i++) {
    buckets[i % concurrency].push(i);
  }

  await Promise.all(
    buckets.map(async (indices) => {
      if (indices.length === 0) return;
      const r = ParquetReader.open(filePath);
      for (const idx of indices) {
        const rg = r.readRowGroup(idx);
        const rows: Record<string, any>[] = [];
        const colNames = Object.keys(rg.columns);
        for (let i = 0; i < rg.numRows; i++) {
          const row: Record<string, any> = {};
          for (const name of colNames) {
            row[name] = rg.columns[name][i];
          }
          rows.push(row);
        }
        allResults[idx] = processor(rows);
      }
      r.close();
    }),
  );

  return allResults.flat();
}

/**
 * Write data chunks to a Parquet file using parallel temporary files,
 * then merge them into a single output.
 */
export async function parallelWrite(
  filePath: string,
  schema: ParquetSchema,
  dataChunks: Record<string, any>[][],
  options: { concurrency?: number; tempDir?: string } = {},
): Promise<void> {
  const concurrency =
    options.concurrency ?? Math.min(os.cpus().length, 4);
  const tempDir = options.tempDir ?? os.tmpdir();

  const tempFiles: string[] = new Array(dataChunks.length);

  const buckets: { index: number; data: Record<string, any>[] }[][] =
    Array.from({ length: concurrency }, () => []);
  for (let i = 0; i < dataChunks.length; i++) {
    buckets[i % concurrency].push({ index: i, data: dataChunks[i] });
  }

  await Promise.all(
    buckets.map(async (items) => {
      for (const item of items) {
        const tmpPath = path.join(
          tempDir,
          `pq_tmp_${item.index}_${Date.now()}_${Math.random().toString(36).slice(2, 8)}.parquet`,
        );
        const w = new ParquetWriter(tmpPath, schema);
        w.write(item.data);
        w.close();
        tempFiles[item.index] = tmpPath;
      }
    }),
  );

  // Merge
  const writer = new ParquetWriter(filePath, schema);
  for (const tmpFile of tempFiles) {
    const reader = ParquetReader.open(tmpFile);
    const data = reader.readAll();
    reader.close();

    const colNames = Object.keys(data.columns);
    const rows: Record<string, any>[] = [];
    for (let i = 0; i < data.numRows; i++) {
      const row: Record<string, any> = {};
      for (const name of colNames) {
        row[name] = data.columns[name][i];
      }
      rows.push(row);
    }
    writer.write(rows);

    fs.unlinkSync(tmpFile);
  }
  writer.close();
}
