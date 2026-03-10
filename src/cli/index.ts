#!/usr/bin/env node
import { Command } from 'commander';
import * as fs from 'fs';
import * as path from 'path';
import { ParquetReader } from '../lib/reader';
import { ParquetWriter } from '../lib/writer';
import { Schema } from '../lib/schema';
import { ParquetType } from '../lib/types';
import { splitParquetFile } from '../lib/splitter';

const program = new Command();

program
  .name('parquet-tool')
  .description('CLI tool for reading, writing and processing Parquet files')
  .version('1.0.0');

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
      const rows: Record<string, any>[] = [];
      for (let r = 0; r < rowCount; r++) {
        const row: Record<string, any> = {};
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
    const schemaDef: Record<string, string> = {};
    for (const part of opts.schema.split(',')) {
      const [name, type] = part.split(':');
      if (!name || !type) {
        console.error(`Invalid schema part: "${part}". Expected "name:TYPE"`);
        process.exit(1);
      }
      schemaDef[name.trim()] = type.trim();
    }
    const schema = Schema.create(schemaDef);

    // Read input
    let rows: Record<string, any>[];
    if (opts.input) {
      const raw = fs.readFileSync(path.resolve(opts.input), 'utf-8');
      rows = JSON.parse(raw);
    } else {
      // Read from stdin
      const raw = fs.readFileSync(0, 'utf-8');
      rows = JSON.parse(raw);
    }

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
    let rows: Record<string, any>[];
    if (opts.input) {
      const raw = fs.readFileSync(path.resolve(opts.input), 'utf-8');
      rows = JSON.parse(raw);
    } else {
      const raw = fs.readFileSync(0, 'utf-8');
      rows = JSON.parse(raw);
    }

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
  .action((output: string, files: string[]) => {
    if (files.length === 0) {
      console.error('Error: at least one input file is required');
      process.exit(1);
    }

    // Read schema from first file
    const firstReader = ParquetReader.open(path.resolve(files[0]));
    const schema = firstReader.getSchema();
    firstReader.close();

    const writer = new ParquetWriter(path.resolve(output), schema);
    for (const f of files) {
      const reader = ParquetReader.open(path.resolve(f));
      const data = reader.readAll();
      reader.close();

      const colNames = Object.keys(data.columns);
      const rows: Record<string, any>[] = [];
      for (let i = 0; i < data.numRows; i++) {
        const row: Record<string, any> = {};
        for (const name of colNames) {
          row[name] = data.columns[name][i];
        }
        rows.push(row);
      }
      writer.write(rows);
    }
    writer.close();
    console.log(`Merged ${files.length} files into ${output}`);
  });

program.parse();
