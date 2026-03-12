import * as fs from 'fs';
import { parse } from 'csv-parse/sync';
import { stringify } from 'csv-stringify/sync';
import { debugLog } from './debug';
import { rowGroupToRows } from './rows';
import {
  CsvToParquetOptions,
  ParquetRow,
  ParquetSchema,
  ParquetToCsvOptions,
  ParquetType,
} from './types';
import { ParquetReader } from './reader';
import { Schema } from './schema';
import { ParquetWriter } from './writer';

function inferValueType(values: string[]): ParquetType {
  const filtered = values.filter((value) => value !== '');
  if (filtered.length === 0) {
    return ParquetType.BYTE_ARRAY;
  }

  if (filtered.every((value) => value === 'true' || value === 'false')) {
    return ParquetType.BOOLEAN;
  }

  if (filtered.every((value) => /^-?\d+$/.test(value))) {
    return filtered.some((value) => !Number.isSafeInteger(Number(value)))
      ? ParquetType.INT64
      : ParquetType.INT32;
  }

  if (filtered.every((value) => /^-?\d+(\.\d+)?$/.test(value))) {
    return ParquetType.DOUBLE;
  }

  return ParquetType.BYTE_ARRAY;
}

function inferSchema(records: Record<string, string>[]): ParquetSchema {
  const builder = new Schema();
  const firstRecord = records[0];
  if (!firstRecord) {
    throw new Error('Cannot infer schema from an empty CSV file.');
  }

  for (const name of Object.keys(firstRecord)) {
    const values = records.map((record) => record[name] ?? '');
    builder.addColumn(name, inferValueType(values), true);
  }

  return builder.build();
}

function normalizeCsvRecords(
  records: Array<Record<string, string> | string[]>,
): Record<string, string>[] {
  if (records.length === 0) {
    return [];
  }

  if (!Array.isArray(records[0])) {
    return records as Record<string, string>[];
  }

  return (records as string[][]).map((values) => {
    const record: Record<string, string> = {};
    values.forEach((value, index) => {
      record[`column_${index + 1}`] = value;
    });
    return record;
  });
}

function coerceValue(raw: string, type: ParquetType): ParquetRow[string] {
  if (raw === '') {
    return null;
  }

  switch (type) {
    case ParquetType.BOOLEAN:
      return raw === 'true';
    case ParquetType.INT32:
      return Number.parseInt(raw, 10);
    case ParquetType.INT64:
      return BigInt(raw);
    case ParquetType.FLOAT:
    case ParquetType.DOUBLE:
      return Number.parseFloat(raw);
    case ParquetType.BYTE_ARRAY:
    default:
      return raw;
  }
}

export function csvToParquet(
  csvPath: string,
  parquetPath: string,
  options: CsvToParquetOptions = {},
): ParquetSchema {
  const delimiter = options.delimiter ?? ',';
  const header = options.header ?? true;
  const source = fs.readFileSync(csvPath, 'utf8');
  const rawRecords = parse(source, {
    columns: header,
    delimiter,
    skip_empty_lines: true,
    trim: true,
  }) as Array<Record<string, string> | string[]>;
  const records = normalizeCsvRecords(rawRecords);

  const schema = options.schema ?? (options.inferSchema ?? true ? inferSchema(records) : null);
  if (!schema) {
    throw new Error('A schema is required when inferSchema is disabled.');
  }

  const rows: ParquetRow[] = records.map((record) => {
    const row: ParquetRow = {};
    for (const column of schema.columns) {
      row[column.name] = coerceValue(record[column.name] ?? '', column.type);
    }
    return row;
  });

  debugLog('csvToParquet: parsed rows', { csvPath, rows: rows.length });
  const writer = new ParquetWriter(parquetPath, schema, options);
  writer.write(rows);
  writer.close();
  return schema;
}

export function parquetToCsv(
  parquetPath: string,
  csvPath: string,
  options: ParquetToCsvOptions = {},
): void {
  const reader = ParquetReader.open(parquetPath);
  const rows: ParquetRow[] = [];
  const header = options.header ?? true;
  const delimiter = options.delimiter ?? ',';

  try {
    for (let index = 0; index < reader.getMetadata().numRowGroups; index++) {
      rows.push(...rowGroupToRows(reader.readRowGroup(index)));
    }
  } finally {
    reader.close();
  }

  const output = stringify(rows, {
    header,
    delimiter,
    cast: {
      boolean: (value) => String(value),
      bigint: (value) => value.toString(),
    },
  });

  fs.writeFileSync(csvPath, output, 'utf8');
}