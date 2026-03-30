#!/usr/bin/env node
import { Command } from 'commander';
import * as fs from 'fs';
import * as path from 'path';
import {
  arrowToParquet,
  configureDebugMode,
  csvToParquet,
  jsonToParquet,
  mergeParquetFiles,
  parquetToArrow,
  parquetToCsv,
  parquetToJson,
  parquetToXml,
  ParquetReader,
  ParquetRow,
  ParquetSchema,
  ParquetType,
  ParquetWriter,
  Schema,
  splitParquetFile,
  validateParquetFile,
  xmlToParquet,
} from '../lib';

const program = new Command();

function parseSchemaDefinition(definition: string): ParquetSchema {
  const schemaDef: Record<string, string> = {};
  for (const part of definition.split(',')) {
    const [name, type] = part.split(':');
    if (!name || !type) {
      throw new Error(`Invalid schema part: "${part}". Expected "name:TYPE"`);
    }
    schemaDef[name.trim()] = type.trim();
  }

  return Schema.create(schemaDef);
}

function readJsonRows(inputPath?: string): ParquetRow[] {
  const raw = inputPath
    ? fs.readFileSync(path.resolve(inputPath), 'utf-8')
    : fs.readFileSync(0, 'utf-8');

  return JSON.parse(raw) as ParquetRow[];
}

program
  .name('parquet-tool')
  .description('CLI tool for reading, writing and processing Parquet files')
  .version('1.0.0')
  .option('--debug', 'Enable debug mode');

program.hook('preAction', (thisCommand) => {
  const debug = thisCommand.optsWithGlobals().debug as boolean | undefined;
  configureDebugMode({ enabled: Boolean(debug) });
});

// ── info ─────────────────────────────────────────────────────
program
  .command('info')
  .description('Show metadata of a Parquet file')
  .argument('<file>', 'Parquet file path')
  .action((file: string) => {
    const meta = ParquetReader.readMetadata(path.resolve(file));
    console.log('Version     :', meta.version);
    console.log('Rows        :', meta.numRows);
    console.log('Row Groups  :', meta.numRowGroups);
    console.log('Created By  :', meta.createdBy);
    console.log('Schema:');
    for (const col of meta.schema) {
      const typeStr = ParquetType[col.type] ?? `UNKNOWN(${col.type})`;
      const opt = col.optional ? ' (optional)' : '';
      console.log(`  - ${col.name}: ${typeStr}${opt}`);
    }
    console.log('Row Groups:');
    meta.rowGroups.forEach((rg, i) => {
      console.log(`  [${i}] rows=${rg.numRows}, bytes=${rg.totalByteSize}`);
    });
  });

// ── read ─────────────────────────────────────────────────────
program
  .command('read')
  .description('Read and display rows from a Parquet file')
  .argument('<file>', 'Parquet file path')
  .option('-n, --limit <n>', 'Maximum rows to display', '20')
  .option('--json', 'Output as JSON array')
  .option('--row-group <index>', 'Read specific row group')
  .action((file: string, opts) => {
    const reader = ParquetReader.open(path.resolve(file));
    const limit = parseInt(opts.limit, 10);

    let data;
    if (opts.rowGroup !== undefined) {
      data = reader.readRowGroup(parseInt(opts.rowGroup, 10));
    } else {
      data = reader.readAll();
    }
    reader.close();

    const colNames = Object.keys(data.columns);
    const rowCount = Math.min(data.numRows, limit);

    if (opts.json) {
      const rows: ParquetRow[] = [];
      for (let r = 0; r < rowCount; r++) {
        const row: ParquetRow = {};
        for (const c of colNames) {
          const v = data.columns[c][r];
          // Convert BigInt to string for JSON
          row[c] = typeof v === 'bigint' ? v.toString() : v;
        }
        rows.push(row);
      }
      console.log(JSON.stringify(rows, null, 2));
    } else {
      // Table output
      const header = colNames.join('\t');
      console.log(header);
      console.log('-'.repeat(header.length));
      for (let r = 0; r < rowCount; r++) {
        const vals = colNames.map((c) => {
          const v = data.columns[c][r];
          return v === null ? 'NULL' : String(v);
        });
        console.log(vals.join('\t'));
      }
      if (data.numRows > limit) {
        console.log(`... (${data.numRows - limit} more rows)`);
      }
    }
  });

// ── write ────────────────────────────────────────────────────
program
  .command('write')
  .description('Create a Parquet file from JSON input')
  .argument('<file>', 'Output Parquet file path')
  .option('-i, --input <json>', 'Input JSON file (array of objects)')
  .option('-s, --schema <def>', 'Schema definition, e.g. "id:INT32,name:STRING"')
  .option('--row-group-size <n>', 'Rows per row group', '10000')
  .action((file: string, opts) => {
    if (!opts.schema) {
      console.error('Error: --schema is required');
      process.exit(1);
    }
    // Parse schema
    const schema = parseSchemaDefinition(opts.schema);
    const rows = readJsonRows(opts.input);

    const rgSize = parseInt(opts.rowGroupSize, 10);
    const writer = new ParquetWriter(path.resolve(file), schema, {
      rowGroupSize: rgSize,
    });
    writer.write(rows);
    writer.close();
    console.log(`Wrote ${rows.length} rows to ${file}`);
  });

// ── append ───────────────────────────────────────────────────
program
  .command('append')
  .description('Append rows to an existing Parquet file')
  .argument('<file>', 'Existing Parquet file path')
  .option('-i, --input <json>', 'Input JSON file (array of objects)')
  .action((file: string, opts) => {
    const rows = readJsonRows(opts.input);

    const writer = ParquetWriter.openForAppend(path.resolve(file));
    writer.write(rows);
    writer.close();
    console.log(`Appended ${rows.length} rows to ${file}`);
  });

// ── split ────────────────────────────────────────────────────
program
  .command('split')
  .description('Split a Parquet file into smaller files')
  .argument('<file>', 'Input Parquet file path')
  .option('-n, --max-rows <n>', 'Max rows per output file', '100000')
  .option('-o, --output-dir <dir>', 'Output directory')
  .option('-p, --prefix <prefix>', 'Output file prefix')
  .action((file: string, opts) => {
    const files = splitParquetFile(path.resolve(file), {
      maxRowsPerFile: parseInt(opts.maxRows, 10),
      outputDir: opts.outputDir ? path.resolve(opts.outputDir) : undefined,
      prefix: opts.prefix,
    });
    console.log(`Split into ${files.length} files:`);
    for (const f of files) {
      console.log(`  ${f}`);
    }
  });

// ── merge ────────────────────────────────────────────────────
program
  .command('merge')
  .description('Merge multiple Parquet files into one')
  .argument('<output>', 'Output Parquet file path')
  .argument('<files...>', 'Input Parquet files')
  .option('--skip-schema-validation', 'Skip schema compatibility validation')
  .action((output: string, files: string[], opts: { skipSchemaValidation?: boolean }) => {
    if (files.length === 0) {
      console.error('Error: at least one input file is required');
      process.exit(1);
    }

    mergeParquetFiles(
      files.map((file) => path.resolve(file)),
      path.resolve(output),
      { validateSchema: !opts.skipSchemaValidation },
    );
    console.log(`Merged ${files.length} files into ${output}`);
  });

program
  .command('validate')
  .description('Validate a Parquet file structure and row groups')
  .argument('<file>', 'Parquet file path')
  .action((file: string) => {
    const result = validateParquetFile(path.resolve(file));
    console.log(`Valid: ${result.valid ? 'yes' : 'no'}`);
    if (result.metadata) {
      console.log(`Rows: ${result.metadata.numRows}`);
      console.log(`Row Groups: ${result.metadata.numRowGroups}`);
    }
    if (result.issues.length === 0) {
      console.log('No issues found.');
      return;
    }

    for (const issue of result.issues) {
      console.log(`[${issue.level}] ${issue.code}: ${issue.message}`);
    }
    if (!result.valid) {
      process.exitCode = 1;
    }
  });

program
  .command('csv-to-parquet')
  .description('Convert a CSV file to a Parquet file')
  .argument('<csv>', 'Input CSV file path')
  .argument('<parquet>', 'Output Parquet file path')
  .option('-s, --schema <def>', 'Schema definition, e.g. "id:INT32,name:STRING"')
  .option('-d, --delimiter <delimiter>', 'CSV delimiter', ',')
  .option('--no-header', 'Treat CSV as not having a header row')
  .option('--no-infer-schema', 'Disable automatic schema inference')
  .action((csv: string, parquet: string, opts) => {
    const schema = csvToParquet(path.resolve(csv), path.resolve(parquet), {
      schema: opts.schema ? parseSchemaDefinition(opts.schema) : undefined,
      delimiter: opts.delimiter,
      header: opts.header,
      inferSchema: opts.inferSchema,
    });
    console.log(`Converted CSV to Parquet: ${parquet}`);
    console.log(`Schema columns: ${schema.columns.map((column) => column.name).join(', ')}`);
  });

program
  .command('parquet-to-csv')
  .description('Convert a Parquet file to a CSV file')
  .argument('<parquet>', 'Input Parquet file path')
  .argument('<csv>', 'Output CSV file path')
  .option('-d, --delimiter <delimiter>', 'CSV delimiter', ',')
  .option('--no-header', 'Write CSV without a header row')
  .action((parquet: string, csv: string, opts) => {
    parquetToCsv(path.resolve(parquet), path.resolve(csv), {
      delimiter: opts.delimiter,
      header: opts.header,
    });
    console.log(`Converted Parquet to CSV: ${csv}`);
  });

program
  .command('arrow-to-parquet')
  .description('Convert an Apache Arrow IPC file to a Parquet file')
  .argument('<arrow>', 'Input Arrow file path')
  .argument('<parquet>', 'Output Parquet file path')
  .option('-s, --schema <def>', 'Schema definition, e.g. "id:INT32,name:STRING"')
  .action((arrow: string, parquet: string, opts) => {
    arrowToParquet(path.resolve(arrow), path.resolve(parquet), {
      schema: opts.schema ? parseSchemaDefinition(opts.schema) : undefined,
    });
    console.log(`Converted Arrow to Parquet: ${parquet}`);
  });

program
  .command('parquet-to-arrow')
  .description('Convert a Parquet file to an Apache Arrow IPC file')
  .argument('<parquet>', 'Input Parquet file path')
  .argument('<arrow>', 'Output Arrow file path')
  .action((parquet: string, arrow: string) => {
    parquetToArrow(path.resolve(parquet), path.resolve(arrow));
    console.log(`Converted Parquet to Arrow: ${arrow}`);
  });

program
  .command('json-to-parquet')
  .description('Convert a JSON file to a Parquet file')
  .argument('<json>', 'Input JSON file path')
  .argument('<parquet>', 'Output Parquet file path')
  .option('-s, --schema <def>', 'Schema definition, e.g. "id:INT32,name:STRING"')
  .action((json: string, parquet: string, opts) => {
    const schema = jsonToParquet(path.resolve(json), path.resolve(parquet), {
      schema: opts.schema ? parseSchemaDefinition(opts.schema) : undefined,
    });
    console.log(`Converted JSON to Parquet: ${parquet}`);
    console.log(`Schema columns: ${schema.columns.map((column) => column.name).join(', ')}`);
  });

program
  .command('parquet-to-json')
  .description('Convert a Parquet file to a JSON file')
  .argument('<parquet>', 'Input Parquet file path')
  .argument('<json>', 'Output JSON file path')
  .option('--rows-only', 'Write only row data without schema metadata')
  .action((parquet: string, json: string, opts) => {
    parquetToJson(path.resolve(parquet), path.resolve(json), {
      includeSchema: !opts.rowsOnly,
    });
    console.log(`Converted Parquet to JSON: ${json}`);
  });

program
  .command('xml-to-parquet')
  .description('Convert an XML file to a Parquet file')
  .argument('<xml>', 'Input XML file path')
  .argument('<parquet>', 'Output Parquet file path')
  .option('-s, --schema <def>', 'Schema definition, e.g. "id:INT32,name:STRING"')
  .option('--root-name <name>', 'Root element name', 'parquet')
  .option('--row-tag <name>', 'Row element name', 'row')
  .action((xml: string, parquet: string, opts) => {
    const schema = xmlToParquet(path.resolve(xml), path.resolve(parquet), {
      schema: opts.schema ? parseSchemaDefinition(opts.schema) : undefined,
      rootName: opts.rootName,
      rowTag: opts.rowTag,
    });
    console.log(`Converted XML to Parquet: ${parquet}`);
    console.log(`Schema columns: ${schema.columns.map((column) => column.name).join(', ')}`);
  });

program
  .command('parquet-to-xml')
  .description('Convert a Parquet file to an XML file')
  .argument('<parquet>', 'Input Parquet file path')
  .argument('<xml>', 'Output XML file path')
  .option('--rows-only', 'Write only row data without schema metadata')
  .option('--root-name <name>', 'Root element name', 'parquet')
  .option('--row-tag <name>', 'Row element name', 'row')
  .action((parquet: string, xml: string, opts) => {
    parquetToXml(path.resolve(parquet), path.resolve(xml), {
      includeSchema: !opts.rowsOnly,
      rootName: opts.rootName,
      rowTag: opts.rowTag,
    });
    console.log(`Converted Parquet to XML: ${xml}`);
  });

program.parse();
