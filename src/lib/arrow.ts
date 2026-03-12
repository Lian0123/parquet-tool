import * as fs from 'fs';
import { tableFromArrays, tableFromIPC, tableToIPC } from 'apache-arrow';
import { debugLog } from './debug';
import { rowGroupToRows } from './rows';
import { ArrowConversionOptions, ParquetRow, ParquetScalar, ParquetSchema } from './types';
import { ParquetReader } from './reader';
import { Schema } from './schema';
import { ParquetType } from './types';
import { ParquetWriter } from './writer';

function inferSchemaFromRows(rows: ParquetRow[]): ParquetSchema {
  const builder = new Schema();
  const firstRow = rows[0];
  if (!firstRow) {
    throw new Error('Cannot infer schema from empty Arrow data.');
  }

  for (const [name, value] of Object.entries(firstRow)) {
    if (typeof value === 'boolean') {
      builder.addColumn(name, ParquetType.BOOLEAN, true);
    } else if (typeof value === 'bigint') {
      builder.addColumn(name, ParquetType.INT64, true);
    } else if (typeof value === 'number') {
      builder.addColumn(name, Number.isInteger(value) ? ParquetType.INT32 : ParquetType.DOUBLE, true);
    } else {
      builder.addColumn(name, ParquetType.BYTE_ARRAY, true);
    }
  }

  return builder.build();
}

export function parquetToArrow(parquetPath: string, arrowPath: string): void {
  const reader = ParquetReader.open(parquetPath);
  const rows: ParquetRow[] = [];

  try {
    for (let index = 0; index < reader.getMetadata().numRowGroups; index++) {
      rows.push(...rowGroupToRows(reader.readRowGroup(index)));
    }
  } finally {
    reader.close();
  }

  const columns: Record<string, ParquetScalar[]> = {};
  for (const row of rows) {
    for (const [name, value] of Object.entries(row)) {
      columns[name] ??= [];
      columns[name].push(value);
    }
  }

  debugLog('parquetToArrow: converting', { parquetPath, arrowPath, rows: rows.length });
  const table = tableFromArrays(columns);
  fs.writeFileSync(arrowPath, Buffer.from(tableToIPC(table)));
}

export function arrowToParquet(
  arrowPath: string,
  parquetPath: string,
  options: ArrowConversionOptions = {},
): ParquetSchema {
  const buffer = fs.readFileSync(arrowPath);
  const table = tableFromIPC(buffer);
  const rows = Array.from(table).map((row) => ({ ...row.toJSON() })) as ParquetRow[];
  const schema = options.schema ?? inferSchemaFromRows(rows);

  debugLog('arrowToParquet: converting', { arrowPath, parquetPath, rows: rows.length });
  const writer = new ParquetWriter(parquetPath, schema, options);
  writer.write(rows);
  writer.close();
  return schema;
}