import * as os from 'os';
import * as path from 'path';
import * as fs from 'fs';
import { ParquetReader } from './reader';
import { ParquetWriter } from './writer';
import {
  ParquetColumns,
  ParallelProcessor,
  ParquetRow,
  ParquetSchema,
  RowGroupData,
} from './types';
import { rowGroupToRows } from './rows';

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

  const { metadata, schema } = ParquetReader.withReader(filePath, (reader) => ({
    metadata: reader.getMetadata(),
    schema: reader.getSchema(),
  }));

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
      ParquetReader.withReader(filePath, (reader) => {
        for (const idx of indices) {
          results[idx] = reader.readRowGroup(idx);
        }
      });
    }),
  );

  // Merge
  const columns: ParquetColumns = {};
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
  processor: ParallelProcessor<T>,
  options: { concurrency?: number } = {},
): Promise<T[]> {
  const concurrency = options.concurrency ?? Math.min(os.cpus().length, 4);

  const metadata = ParquetReader.withReader(filePath, (reader) =>
    reader.getMetadata(),
  );

  const numRG = metadata.numRowGroups;
  const allResults: T[][] = new Array(numRG);

  const buckets: number[][] = Array.from({ length: concurrency }, () => []);
  for (let i = 0; i < numRG; i++) {
    buckets[i % concurrency].push(i);
  }

  await Promise.all(
    buckets.map(async (indices) => {
      if (indices.length === 0) return;
      ParquetReader.withReader(filePath, (reader) => {
        for (const idx of indices) {
          const rg = reader.readRowGroup(idx);
          const rows = rowGroupToRows(rg);
          allResults[idx] = processor(rows);
        }
      });
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
  dataChunks: ParquetRow[][],
  options: { concurrency?: number; tempDir?: string } = {},
): Promise<void> {
  const concurrency =
    options.concurrency ?? Math.min(os.cpus().length, 4);
  const tempDir = options.tempDir ?? os.tmpdir();

  const tempFiles: string[] = new Array(dataChunks.length);

  const buckets: { index: number; data: ParquetRow[] }[][] =
    Array.from({ length: concurrency }, () => []);
  for (let i = 0; i < dataChunks.length; i++) {
    buckets[i % concurrency].push({ index: i, data: dataChunks[i] });
  }

  try {
    await Promise.all(
      buckets.map(async (items) => {
        for (const item of items) {
          const tmpPath = path.join(
            tempDir,
            `pq_tmp_${item.index}_${Date.now()}_${Math.random().toString(36).slice(2, 8)}.parquet`,
          );
          ParquetWriter.withWriter(tmpPath, schema, (writer) => {
            writer.write(item.data);
          });
          tempFiles[item.index] = tmpPath;
        }
      }),
    );

    ParquetWriter.withWriter(filePath, schema, (writer) => {
      for (const tmpFile of tempFiles) {
        ParquetReader.withReader(tmpFile, (reader) => {
          for (const rowGroup of reader.iterateRowGroups()) {
            writer.write(rowGroupToRows(rowGroup));
          }
        });
      }
    });
  } finally {
    for (const tmpFile of tempFiles) {
      if (tmpFile && fs.existsSync(tmpFile)) {
        fs.unlinkSync(tmpFile);
      }
    }
  }
}
