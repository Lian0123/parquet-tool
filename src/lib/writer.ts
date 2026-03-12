import { native } from './binding';
import { debugLog } from './debug';
import {
  NativeSchemaColumn,
  ParquetColumns,
  ParquetRow,
  ParquetSchema,
  WriteOptions,
} from './types';

/**
 * Write Parquet files row‑by‑row or in batches.
 *
 * ```ts
 * const writer = new ParquetWriter('out.parquet', schema);
 * writer.write([{ id: 1, name: 'Alice' }, { id: 2, name: 'Bob' }]);
 * writer.close();
 * ```
 */
export class ParquetWriter {
  private handle: number | null = null;
  private schema: ParquetSchema;
  private rowGroupSize: number;
  private buffer: ParquetColumns = {};
  private bufferSize = 0;

  constructor(
    filePath: string,
    schema: ParquetSchema,
    options: WriteOptions = {},
  ) {
    this.schema = schema;
    this.rowGroupSize = options.rowGroupSize ?? 10_000;
    debugLog('writer: create', { filePath, rowGroupSize: this.rowGroupSize });

    const nativeSchema: NativeSchemaColumn[] = schema.columns.map((c) => ({
      name: c.name,
      type: c.type,
      optional: c.optional ?? false,
    }));
    this.handle = native.createWriter(filePath, nativeSchema);

    for (const col of schema.columns) {
      this.buffer[col.name] = [];
    }
  }

  /** Write one or more rows. Automatically flushes row groups. */
  write(rows: ParquetRow | ParquetRow[]): void {
    if (this.handle === null) throw new Error('Writer is closed');
    const arr = Array.isArray(rows) ? rows : [rows];
    for (const row of arr) {
      for (const col of this.schema.columns) {
        this.buffer[col.name].push(row[col.name] ?? null);
      }
      this.bufferSize++;
      if (this.bufferSize >= this.rowGroupSize) {
        this.flushRowGroup();
      }
    }
  }

  /** Force flush the current buffer as a row group. */
  flush(): void {
    this.flushRowGroup();
  }

  /** Close the writer and finalise the file. */
  close(): void {
    if (this.handle === null) return;
    this.flushRowGroup();
    debugLog('writer: close');
    native.closeWriter(this.handle);
    this.handle = null;
  }

  /**
   * Open an existing Parquet file for appending new row groups.
   */
  static openForAppend(
    filePath: string,
    options: WriteOptions = {},
  ): ParquetWriter {
    debugLog('writer: append', { filePath });
    const result = native.openAppender(filePath);
    const meta = result.metadata;
    const schema: ParquetSchema = {
      columns: meta.schema.map((s) => ({
        name: s.name,
        type: s.type,
        optional: s.optional,
      })),
    };

    // Build writer manually without calling the constructor's createWriter
    const writer = Object.create(ParquetWriter.prototype) as ParquetWriter;
    writer.handle = result.handle;
    writer.schema = schema;
    writer.rowGroupSize = options.rowGroupSize ?? 10_000;
    writer.buffer = {};
    writer.bufferSize = 0;
    for (const col of schema.columns) {
      writer.buffer[col.name] = [];
    }
    return writer;
  }

  private flushRowGroup(): void {
    if (this.bufferSize === 0) return;

    const columns = this.schema.columns.map((col) => ({
      values: this.buffer[col.name],
    }));

    native.writeRowGroup(this.handle!, columns);

    for (const col of this.schema.columns) {
      this.buffer[col.name] = [];
    }
    this.bufferSize = 0;
  }
}
