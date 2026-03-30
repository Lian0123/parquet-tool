import * as fs from 'fs';
import { XMLBuilder, XMLParser } from 'fast-xml-parser';
import {
  coerceRows,
  deserializeSchema,
  ensureRows,
  inferSchemaFromRows,
  readParquetRows,
  serializeRows,
  serializeSchema,
  writeRowsToParquet,
} from './conversion';
import { ParquetRow, ParquetSchema, ParquetToXmlOptions, XmlToParquetOptions } from './types';

interface XmlColumnNode {
  '@_name': string;
  '@_type': string;
  '@_optional'?: string;
}

function valueToXmlNode(value: unknown): unknown {
  if (value === null || value === undefined) {
    return { '@_null': 'true' };
  }
  return value;
}

function valueFromXmlNode(value: unknown): unknown {
  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value === 'object' && !Array.isArray(value)) {
    const node = value as Record<string, unknown>;
    if (node['@_null'] === 'true' || node['@_null'] === true) {
      return null;
    }
    if ('#text' in node) {
      return node['#text'];
    }
    return '';
  }

  return value;
}

export function parquetToXml(
  parquetPath: string,
  xmlPath: string,
  options: ParquetToXmlOptions = {},
): void {
  const rootName = options.rootName ?? 'parquet';
  const rowTag = options.rowTag ?? 'row';
  const { rows, schema } = readParquetRows(parquetPath);
  const serializedRows = serializeRows(rows, schema);
  const builder = new XMLBuilder({
    ignoreAttributes: false,
    format: options.pretty ?? true,
    suppressEmptyNode: false,
    suppressBooleanAttributes: false,
  });

  const document = {
    [rootName]: {
      ...(options.includeSchema ?? true
        ? {
            schema: {
              column: serializeSchema(schema).columns.map((column) => ({
                '@_name': column.name,
                '@_type': column.type,
                '@_optional': String(Boolean(column.optional)),
              })),
            },
          }
        : {}),
      rows: {
        [rowTag]: serializedRows.map((row) => {
          const rowNode: Record<string, unknown> = {};
          for (const [name, value] of Object.entries(row)) {
            rowNode[name] = valueToXmlNode(value);
          }
          return rowNode;
        }),
      },
    },
  };

  fs.writeFileSync(xmlPath, builder.build(document), 'utf8');
}

export function xmlToParquet(
  xmlPath: string,
  parquetPath: string,
  options: XmlToParquetOptions = {},
): ParquetSchema {
  const rootName = options.rootName ?? 'parquet';
  const rowTag = options.rowTag ?? 'row';
  const parser = new XMLParser({
    ignoreAttributes: false,
    parseTagValue: false,
    trimValues: true,
  });

  const parsed = parser.parse(fs.readFileSync(xmlPath, 'utf8')) as Record<string, unknown>;
  const root = parsed[rootName] as Record<string, unknown> | undefined;
  if (!root) {
    throw new Error(`Missing root element: ${rootName}`);
  }

  const rowsContainer = root.rows as Record<string, unknown> | undefined;
  const rawRows = rowsContainer?.[rowTag];
  const rows = ensureRows(
    (Array.isArray(rawRows) ? rawRows : rawRows ? [rawRows] : []).map((row) => {
      const rowObject = row as Record<string, unknown>;
      const normalized: ParquetRow = {};
      for (const [name, value] of Object.entries(rowObject)) {
        normalized[name] = valueFromXmlNode(value) as ParquetRow[string];
      }
      return normalized;
    }),
  );

  const rawColumns = ((root.schema as Record<string, unknown> | undefined)?.column ?? null) as
    | XmlColumnNode
    | XmlColumnNode[]
    | null;
  const parsedSchema = deserializeSchema(
    rawColumns
      ? {
          columns: (Array.isArray(rawColumns) ? rawColumns : [rawColumns]).map((column) => ({
            name: column['@_name'],
            type: column['@_type'],
            optional: column['@_optional'] === 'true',
          })),
        }
      : null,
  );
  const schema = options.schema ?? parsedSchema ?? inferSchemaFromRows(rows);

  return writeRowsToParquet(parquetPath, coerceRows(rows, schema), schema, options);
}