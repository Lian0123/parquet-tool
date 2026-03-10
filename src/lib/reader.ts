import { native } from './binding';
import { FileMetadata, ParquetSchema, RowGroupData } from './types';

/**
 * Read Parquet files — access metadata, iterate over row groups, or
 * read all data at once.
 *
 * ```ts
 * const reader = ParquetReader.open('data.parquet');
 * const data = reader.readAll();
 * reader.close();
 * ```
 */
export class ParquetReader {
  private handle: number | null = null;
  private meta: FileMetadata;

  private constructor(handle: number, metadata: FileMetadata) {
    this.handle = handle;
    this.meta = metadata;
  }

  /** Open a Parquet file for reading. */
  static open(filePath: string): ParquetReader {
    const result = native.openReader(filePath);
    return new ParquetReader(result.handle, result.metadata as FileMetadata);
  }

  /** Return file-level metadata. */
  getMetadata(): FileMetadata {
    return this.meta;
  }

  /** Return the schema. */
  getSchema(): ParquetSchema {
    return { columns: this.meta.schema };
  }

  /** Read a single row group by index. */
  readRowGroup(index: number): RowGroupData {
    if (this.handle === null) throw new Error('Reader is closed');
    if (index < 0 || index >= this.meta.numRowGroups)
      throw new RangeError(
        `Row group ${index} out of range [0, ${this.meta.numRowGroups})`,
      );
    return native.readRowGroup(this.handle, index) as RowGroupData;
  }

  /** Read all row groups and merge them. */
  readAll(): RowGroupData {
    if (this.handle === null) throw new Error('Reader is closed');

    const columns: Record<string, any[]> = {};
    for (const col of this.meta.schema) {
      columns[col.name] = [];
    }
    let totalRows = 0;

    for (let i = 0; i < this.meta.numRowGroups; i++) {
      const rg = this.readRowGroup(i);
      for (const [name, values] of Object.entries(rg.columns)) {
        columns[name].push(...values);
      }
      totalRows += rg.numRows;
    }

    return { numRows: totalRows, columns };
  }

  /** Iterate over rows one by one (generator). */
  *[Symbol.iterator](): Generator<Record<string, any>> {
    for (let i = 0; i < this.meta.numRowGroups; i++) {
      const rg = this.readRowGroup(i);
      const names = Object.keys(rg.columns);
      for (let r = 0; r < rg.numRows; r++) {
        const row: Record<string, any> = {};
        for (const n of names) {
          row[n] = rg.columns[n][r];
        }
        yield row;
      }
    }
  }

  /** Close the reader and release resources. */
  close(): void {
    if (this.handle === null) return;
    native.closeReader(this.handle);
    this.handle = null;
  }

  /** Read metadata without opening a full reader. */
  static readMetadata(filePath: string): FileMetadata {
    return native.getMetadata(filePath) as FileMetadata;
  }
}
