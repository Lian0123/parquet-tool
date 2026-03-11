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

export type ParquetScalar = boolean | number | bigint | string | null;

export type ParquetColumns = Record<string, ParquetScalar[]>;

export type ParquetRow = Record<string, ParquetScalar>;

export interface RowGroupData {
  numRows: number;
  columns: ParquetColumns;
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

export interface DebugOptions {
  enabled?: boolean;
  logger?: (message: string, payload?: unknown) => void;
}

export interface ValidationIssue {
  level: 'error' | 'warning';
  code: string;
  message: string;
}

export interface ValidationResult {
  valid: boolean;
  filePath: string;
  metadata: FileMetadata | null;
  issues: ValidationIssue[];
}

export interface MergeOptions extends WriteOptions {
  validateSchema?: boolean;
}

export interface CsvToParquetOptions extends WriteOptions {
  schema?: ParquetSchema;
  header?: boolean;
  delimiter?: string;
  inferSchema?: boolean;
}

export interface ParquetToCsvOptions {
  delimiter?: string;
  header?: boolean;
}

export interface ArrowConversionOptions extends WriteOptions {
  schema?: ParquetSchema;
}
