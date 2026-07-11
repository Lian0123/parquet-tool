import { native } from './binding';
import { debugLog } from './debug';
import {
  FileMetadata,
  ParquetColumns,
  ParquetRow,
  ReadOptions,
  ParquetSchema,
  RowGroupData,
} from './types';
import { rowGroupToRows } from './rows';
import { assertSupportedParquetType } from './type-support';

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
    debugLog('reader: open', { filePath });
    const result = native.openReader(filePath);
    const metadata = result.metadata as FileMetadata;
    metadata.schema.forEach((column) => {
      assertSupportedParquetType(
        column.type,
        `ParquetReader for column "${column.name}"`,
      );
    });
    return new ParquetReader(result.handle, metadata);
  }

  /**
   * Open a reader for the duration of a callback and always close it.
   */
  static withReader<T>(
    filePath: string,
    fn: (reader: ParquetReader) => T,
  ): T {
    const reader = ParquetReader.open(filePath);
    try {
      return fn(reader);
    } finally {
      reader.close();
    }
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
  readRowGroup(index: number, options: Pick<ReadOptions, 'columns'> = {}): RowGroupData {
    if (this.handle === null) throw new Error('Reader is closed');
    if (index < 0 || index >= this.meta.numRowGroups)
      throw new RangeError(
        `Row group ${index} out of range [0, ${this.meta.numRowGroups})`,
      );
    const rowGroup = native.readRowGroup(this.handle, index) as RowGroupData;
    return this.projectRowGroup(rowGroup, options.columns);
  }

  /** Read all row groups and merge them. */
  readAll(options: ReadOptions = {}): RowGroupData {
    if (this.handle === null) throw new Error('Reader is closed');

    const columns: ParquetColumns = {};
    const plan = this.createReadPlan(options);
    for (const name of plan.columns) columns[name] = [];
    let totalRows = 0;

    for (const index of plan.rowGroups) {
      const rg = this.readRowGroup(index, { columns: plan.columns });
      for (const [name, values] of Object.entries(rg.columns)) {
        columns[name].push(...values);
      }
      totalRows += rg.numRows;
    }

    return { numRows: totalRows, columns };
  }

  /** Read all row groups as row-oriented objects. */
  readRows(options: ReadOptions = {}): ParquetRow[] {
    return Array.from(this.iterateRows(options));
  }

  /** Iterate over row groups without merging them. */
  *iterateRowGroups(options: ReadOptions = {}): Generator<RowGroupData> {
    const plan = this.createReadPlan(options);
    for (const index of plan.rowGroups) {
      yield this.readRowGroup(index, { columns: plan.columns });
    }
  }

  /** Iterate over rows one by one without reading the full file at once. */
  *iterateRows(options: ReadOptions = {}): Generator<ParquetRow> {
    for (const rowGroup of this.iterateRowGroups(options)) {
      yield* rowGroupToRows(rowGroup);
    }
  }

  /** Iterate over rows one by one (generator). */
  *[Symbol.iterator](): Generator<ParquetRow> {
    yield* this.iterateRows();
  }

  /** Close the reader and release resources. */
  close(): void {
    if (this.handle === null) return;
    debugLog('reader: close');
    native.closeReader(this.handle);
    this.handle = null;
  }

  /** Read metadata without opening a full reader. */
  static readMetadata(filePath: string): FileMetadata {
    return native.getMetadata(filePath) as FileMetadata;
  }

  private createReadPlan(options: ReadOptions): {
    columns: string[];
    rowGroups: number[];
  } {
    const selectedColumns =
      options.columns && options.columns.length > 0
        ? options.columns
        : this.meta.schema.map((column) => column.name);
    const availableColumns = new Set(this.meta.schema.map((column) => column.name));

    for (const name of selectedColumns) {
      if (!availableColumns.has(name)) {
        throw new RangeError(`Unknown column "${name}".`);
      }
    }

    const rowGroups =
      options.rowGroups && options.rowGroups.length > 0
        ? options.rowGroups
        : Array.from({ length: this.meta.numRowGroups }, (_, index) => index);

    for (const index of rowGroups) {
      if (!Number.isInteger(index)) {
        throw new TypeError(`Row group index must be an integer: ${index}`);
      }
      if (index < 0 || index >= this.meta.numRowGroups) {
        throw new RangeError(
          `Row group ${index} out of range [0, ${this.meta.numRowGroups})`,
        );
      }
    }

    return {
      columns: Array.from(new Set(selectedColumns)),
      rowGroups: Array.from(new Set(rowGroups)),
    };
  }

  private projectRowGroup(
    rowGroup: RowGroupData,
    selectedColumns?: string[],
  ): RowGroupData {
    if (!selectedColumns || selectedColumns.length === 0) {
      return rowGroup;
    }

    const columns: ParquetColumns = {};
    for (const name of selectedColumns) {
      columns[name] = rowGroup.columns[name];
    }
    return { numRows: rowGroup.numRows, columns };
  }
}
