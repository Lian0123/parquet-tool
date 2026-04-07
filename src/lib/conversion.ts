import * as fs from 'fs';
import { ParquetReader } from './reader';
import { rowGroupToRows } from './rows';
import { Schema } from './schema';
import { ParquetRow, ParquetScalar, ParquetSchema, ParquetType } from './types';
import { ParquetWriter } from './writer';

interface SerializedSchemaColumn {
  name: string;
  type: string;
  optional?: boolean;
}

export interface SerializedSchema {
  columns: SerializedSchemaColumn[];
}

export function inferValueType(values: Array<ParquetScalar | string>): ParquetType {
  const filtered = values.filter((value) => value !== null && value !== '');
  if (filtered.length === 0) {
    return ParquetType.BYTE_ARRAY;
  }

  if (filtered.every((value) => typeof value === 'boolean' || value === 'true' || value === 'false')) {
    return ParquetType.BOOLEAN;
  }

  if (filtered.every((value) => typeof value === 'bigint')) {
    return ParquetType.INT64;
  }

  if (filtered.every((value) => typeof value === 'number' && Number.isInteger(value))) {
    return ParquetType.INT32;
  }

  if (filtered.every((value) => typeof value === 'number')) {
    return ParquetType.DOUBLE;
  }

  if (
    filtered.every((value) => {
      if (typeof value !== 'string') return false;
      return /^-?\d+$/.test(value);
    })
  ) {
    return filtered.some((value) => !Number.isSafeInteger(Number(value)))
      ? ParquetType.INT64
      : ParquetType.INT32;
  }

  if (
    filtered.every((value) => {
      if (typeof value !== 'string') return false;
      return /^-?\d+(\.\d+)?$/.test(value);
    })
  ) {
    return ParquetType.DOUBLE;
  }

  return ParquetType.BYTE_ARRAY;
}

export function inferSchemaFromRows(rows: ParquetRow[]): ParquetSchema {
  const firstRow = rows[0];
  if (!firstRow) {
    throw new Error('Cannot infer schema from empty rows.');
  }

  const builder = new Schema();
  const columnNames = Array.from(
    rows.reduce((names, row) => {
      Object.keys(row).forEach((name) => names.add(name));
      return names;
    }, new Set<string>()),
  );

  for (const name of columnNames) {
    const values = rows.map((row) => row[name] ?? null);
    const optional = values.some((value) => value === null || value === undefined);
    builder.addColumn(name, inferValueType(values), optional);
  }

  return builder.build();
}

export function readParquetRows(parquetPath: string): { rows: ParquetRow[]; schema: ParquetSchema } {
  const reader = ParquetReader.open(parquetPath);

  try {
    const schema = reader.getSchema();
    const rows: ParquetRow[] = [];
    for (let index = 0; index < reader.getMetadata().numRowGroups; index++) {
      rows.push(...rowGroupToRows(reader.readRowGroup(index)));
    }
    return { rows, schema };
  } finally {
    reader.close();
  }
}

export function writeRowsToParquet(
  parquetPath: string,
  rows: ParquetRow[],
  schema: ParquetSchema,
  options: { rowGroupSize?: number } = {},
): ParquetSchema {
  const writer = new ParquetWriter(parquetPath, schema, options);
  writer.write(rows);
  writer.close();
  return schema;
}

export function serializeSchema(schema: ParquetSchema): SerializedSchema {
  return {
    columns: schema.columns.map((column) => ({
      name: column.name,
      type: ParquetType[column.type],
      optional: column.optional,
    })),
  };
}

export function deserializeSchema(schema?: SerializedSchema | ParquetSchema | null): ParquetSchema | null {
  if (!schema) {
    return null;
  }

  const columns = (schema as unknown as { columns: Array<Record<string, unknown>> }).columns;
  if (!Array.isArray(columns)) {
    throw new Error('Invalid schema format.');
  }

  return {
    columns: columns.map((column) => {
      const type = column.type;
      return {
        name: String(column.name),
        type: typeof type === 'number' ? type : Schema.parseType(String(type)),
        optional: Boolean(column.optional),
      };
    }),
  };
}

export function serializeScalar(value: ParquetScalar): string | number | boolean | null {
  if (typeof value === 'bigint') {
    return value.toString();
  }
  return value;
}

export function serializeRows(rows: ParquetRow[], schema: ParquetSchema): ParquetRow[] {
  return rows.map((row) => {
    const serialized: ParquetRow = {};
    for (const column of schema.columns) {
      serialized[column.name] = serializeScalar(row[column.name] ?? null);
    }
    return serialized;
  });
}

export function coerceValue(value: unknown, type: ParquetType): ParquetScalar {
  if (value === null || value === undefined || value === '') {
    return null;
  }

  switch (type) {
    case ParquetType.BOOLEAN:
      return typeof value === 'boolean' ? value : String(value) === 'true';
    case ParquetType.INT32:
      return typeof value === 'number' ? Math.trunc(value) : Number.parseInt(String(value), 10);
    case ParquetType.INT64:
      return typeof value === 'bigint' ? value : BigInt(String(value));
    case ParquetType.FLOAT:
    case ParquetType.DOUBLE:
      return typeof value === 'number' ? value : Number.parseFloat(String(value));
    case ParquetType.BYTE_ARRAY:
    default:
      return String(value);
  }
}

export function coerceRows(rows: ParquetRow[], schema: ParquetSchema): ParquetRow[] {
  return rows.map((row) => {
    const normalized: ParquetRow = {};
    for (const column of schema.columns) {
      normalized[column.name] = coerceValue(row[column.name], column.type);
    }
    return normalized;
  });
}

export function ensureRows(rows: unknown): ParquetRow[] {
  if (!Array.isArray(rows)) {
    throw new Error('Input data must contain a rows array.');
  }

  return rows.map((row) => {
    if (!row || typeof row !== 'object' || Array.isArray(row)) {
      throw new Error('Each row must be an object.');
    }
    return row as ParquetRow;
  });
}

export function readJsonFile<T>(filePath: string): T {
  return JSON.parse(fs.readFileSync(filePath, 'utf8')) as T;
}