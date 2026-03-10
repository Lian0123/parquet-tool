/** Parquet physical types (matching the Parquet spec enum values). */
export enum ParquetType {
  BOOLEAN = 0,
  INT32 = 1,
  INT64 = 2,
  INT96 = 3,
  FLOAT = 4,
  DOUBLE = 5,
  BYTE_ARRAY = 6,
  FIXED_LEN_BYTE_ARRAY = 7,
}

export interface SchemaColumn {
  name: string;
  type: ParquetType;
  optional?: boolean;
}

export interface ParquetSchema {
  columns: SchemaColumn[];
}

export interface RowGroupData {
  numRows: number;
  columns: Record<string, any[]>;
}

export interface FileMetadata {
  version: number;
  numRows: number;
  numRowGroups: number;
  schema: SchemaColumn[];
  createdBy: string;
  rowGroups: RowGroupInfo[];
}

export interface RowGroupInfo {
  numRows: number;
  totalByteSize: number;
  columns: ColumnChunkInfo[];
}

export interface ColumnChunkInfo {
  name: string;
  type: ParquetType;
  numValues: number;
  compressedSize: number;
  uncompressedSize: number;
}

export interface WriteOptions {
  /** Maximum rows per row group (default: 10 000). */
  rowGroupSize?: number;
}

export interface ReadOptions {
  columns?: string[];
  rowGroups?: number[];
}
