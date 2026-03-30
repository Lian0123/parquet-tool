import * as fs from 'fs';
import {
  coerceRows,
  deserializeSchema,
  ensureRows,
  inferSchemaFromRows,
  readJsonFile,
  readParquetRows,
  serializeRows,
  serializeSchema,
  writeRowsToParquet,
} from './conversion';
import { JsonToParquetOptions, ParquetRow, ParquetSchema, ParquetToJsonOptions } from './types';

interface JsonDocument {
  schema?: ParquetSchema;
  rows?: ParquetRow[];
}

export function parquetToJson(
  parquetPath: string,
  jsonPath: string,
  options: ParquetToJsonOptions = {},
): void {
  const { rows, schema } = readParquetRows(parquetPath);
  const pretty = options.pretty ?? 2;

  if (options.includeSchema ?? true) {
    fs.writeFileSync(
      jsonPath,
      JSON.stringify(
        {
          schema: serializeSchema(schema),
          rows: serializeRows(rows, schema),
        },
        null,
        pretty,
      ),
      'utf8',
    );
    return;
  }

  fs.writeFileSync(jsonPath, JSON.stringify(serializeRows(rows, schema), null, pretty), 'utf8');
}

export function jsonToParquet(
  jsonPath: string,
  parquetPath: string,
  options: JsonToParquetOptions = {},
): ParquetSchema {
  const parsed = readJsonFile<JsonDocument | ParquetRow[]>(jsonPath);
  const rows = ensureRows(Array.isArray(parsed) ? parsed : parsed.rows);
  const schema =
    options.schema ??
    deserializeSchema(Array.isArray(parsed) ? null : parsed.schema) ??
    inferSchemaFromRows(rows);

  return writeRowsToParquet(parquetPath, coerceRows(rows, schema), schema, options);
}