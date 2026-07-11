import { ParquetType } from './types';

export const SUPPORTED_PARQUET_TYPES: readonly ParquetType[] = [
  ParquetType.BOOLEAN,
  ParquetType.INT32,
  ParquetType.INT64,
  ParquetType.FLOAT,
  ParquetType.DOUBLE,
  ParquetType.BYTE_ARRAY,
];

const SUPPORTED_TYPE_SET = new Set<number>(SUPPORTED_PARQUET_TYPES);

export function isParquetTypeSupported(type: ParquetType): boolean {
  return SUPPORTED_TYPE_SET.has(type);
}

export function assertSupportedParquetType(
  type: ParquetType,
  context = 'Parquet operation',
): void {
  if (isParquetTypeSupported(type)) {
    return;
  }

  const typeName = ParquetType[type] ?? `UNKNOWN(${type})`;
  throw new Error(
    `${context} does not support ${typeName}. Supported types: ${SUPPORTED_PARQUET_TYPES.map((value) => ParquetType[value]).join(', ')}.`,
  );
}
