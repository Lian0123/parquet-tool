# Parquet guide and complete examples

https://lian0123.github.io/parquet-tool/en/guide.html

Requires Node.js 18+. Run npm install parquet-tool and save input.json and input.csv in the same directory. Save code as example.cjs and run node example.cjs.

## Create your first Parquet file

Write JSON data with an explicit schema. Set rowGroupSize to 2 so you can inspect row groups later.

Download input.json into your working directory. Running again overwrites data.parquet.

```sh
npx parquet-tool write data.parquet -i input.json -s "id:INT32,name:STRING,active:BOOLEAN" --row-group-size 2
```

```js
const { readFileSync } = require('node:fs');
const { Schema, ParquetWriter } = require('parquet-tool');

const rows = JSON.parse(readFileSync('input.json', 'utf8'));
const schema = Schema.create({
  id: 'INT32', name: 'STRING', active: 'BOOLEAN',
});

ParquetWriter.withWriter('data.parquet', schema,
  { rowGroupSize: 2 }, (writer) => {
    writer.write(rows);
  }); // 自動關閉 writer，完成檔案寫入
```

Creates data.parquet with 3 rows and 2 row groups.

## Read rows

The CLI outputs JSON. Node.js processes rows with an iterator without merging the entire file at once.

Run “Create a file” first to prepare data.parquet.

```sh
npx parquet-tool read data.parquet --json --limit 20
```

```js
const { ParquetReader } = require('parquet-tool');

ParquetReader.withReader('data.parquet', (reader) => {
  for (const row of reader.iterateRows()) {
    console.log(row);
  }
}); // 自動關閉 reader
```

Prints the id, name and active values in the terminal.

## Inspect schema and metadata

Check column types, row count and row-group count to understand the file structure.

Run “Create a file” first to prepare data.parquet.

```sh
npx parquet-tool info data.parquet
```

```js
const { ParquetReader } = require('parquet-tool');

const metadata = ParquetReader.readMetadata('data.parquet');
console.log('Rows:', metadata.numRows);
console.log('Row groups:', metadata.numRowGroups);
console.table(metadata.schema);
```

Shows schema, numRows, numRowGroups and other metadata.

## Append to an existing file

Reuse the schema and append a new row group. Each run adds another row.

Run “Create a file” first. The CLI recipe generates more.json with Node.js and modifies data.parquet.

```sh
node -e "require('node:fs').writeFileSync('more.json', JSON.stringify([{id:4,name:'Dora',active:true}]))"
npx parquet-tool append data.parquet -i more.json
npx parquet-tool info data.parquet
```

```js
const { ParquetWriter, ParquetReader } = require('parquet-tool');

ParquetWriter.withAppender('data.parquet', (writer) => {
  writer.write({ id: 4, name: 'Dora', active: true });
});
console.log(ParquetReader.readMetadata('data.parquet').numRows);
```

The first append increases the original 3 rows to 4.

## Convert CSV and Parquet

Import a CSV with a header, infer column types, then export back to CSV.

Download input.csv into your working directory. No existing data.parquet is needed.

```sh
npx parquet-tool csv-to-parquet input.csv from-csv.parquet
npx parquet-tool parquet-to-csv from-csv.parquet roundtrip.csv
```

```js
const { csvToParquet, parquetToCsv } = require('parquet-tool');

csvToParquet('input.csv', 'from-csv.parquet');
parquetToCsv('from-csv.parquet', 'roundtrip.csv');
```

Creates from-csv.parquet and roundtrip.csv with the 3 sample rows.

## Convert JSON and Parquet

Create Parquet from a JSON array and export rows-only JSON. Without rows-only, schema is included by default.

Download input.json into your working directory. No existing data.parquet is needed.

```sh
npx parquet-tool json-to-parquet input.json from-json.parquet
npx parquet-tool parquet-to-json from-json.parquet roundtrip.json --rows-only
```

```js
const { jsonToParquet, parquetToJson } = require('parquet-tool');

jsonToParquet('input.json', 'from-json.parquet');
parquetToJson('from-json.parquet', 'roundtrip.json', {
  includeSchema: false,
});
```

Creates from-json.parquet and roundtrip.json containing only a data array.

## Convert Apache Arrow IPC

Convert Parquet to an Arrow IPC file and back.

Run “Create a file” first to prepare data.parquet.

```sh
npx parquet-tool parquet-to-arrow data.parquet data.arrow
npx parquet-tool arrow-to-parquet data.arrow from-arrow.parquet
```

```js
const { parquetToArrow, arrowToParquet } = require('parquet-tool');

parquetToArrow('data.parquet', 'data.arrow');
arrowToParquet('data.arrow', 'from-arrow.parquet');
```

Creates data.arrow and from-arrow.parquet.

## Convert XML and Parquet

Export schema with data by default so column types can be restored on import.

Run “Create a file” first to prepare data.parquet.

```sh
npx parquet-tool parquet-to-xml data.parquet data.xml
npx parquet-tool xml-to-parquet data.xml from-xml.parquet
```

```js
const { parquetToXml, xmlToParquet } = require('parquet-tool');

parquetToXml('data.parquet', 'data.xml');
xmlToParquet('data.xml', 'from-xml.parquet');
```

Creates data.xml and from-xml.parquet.

## Split a file and merge it back

Write at most 2 rows per output file. Merging validates schema compatibility by default.

Run “Create a file” again to restore 3 rows. Output files are written to the working directory.

```sh
npx parquet-tool split data.parquet -n 2 -p demo
npx parquet-tool merge merged.parquet demo_part0000.parquet demo_part0001.parquet
npx parquet-tool validate merged.parquet
```

```js
const {
  splitParquetFile, mergeParquetFiles, validateParquetFile,
} = require('parquet-tool');

const files = splitParquetFile('data.parquet', {
  maxRowsPerFile: 2,
  outputDir: '.',
  prefix: 'demo',
});
mergeParquetFiles(files, 'merged.parquet');
console.log(validateParquetFile('merged.parquet').valid);
```

Creates 2 demo_part files; merged.parquet contains 3 rows after merging.

## Validate a Parquet file

Check file structure and row groups. Invalid files produce a nonzero CLI exit code for scripts and CI.

Run “Create a file” first to prepare data.parquet.

```sh
npx parquet-tool validate data.parquet
```

```js
const { validateParquetFile } = require('parquet-tool');

const report = validateParquetFile('data.parquet');
console.log(report.valid, report.issues);
if (!report.valid) {
  process.exitCode = 1;
}
```

A valid sample prints Valid: yes; Node.js prints true with an empty issues array.

## Read selected columns and row groups

The Node.js API selects columns and row groups. The CLI selects row groups only. Indexes start at 0.

Run “Create a file” with rowGroupSize: 2 to prepare data.parquet.

```sh
npx parquet-tool read data.parquet --row-group 0 --json
```

```js
const { ParquetReader } = require('parquet-tool');

ParquetReader.withReader('data.parquet', (reader) => {
  for (const row of reader.iterateRows({
    columns: ['id', 'name'],
    rowGroups: [0],
  })) {
    console.log(row);
  }
});
```

The CLI prints the first row group; Node.js prints its id and name columns. Projection happens after reading the row group.

## Transfer files with Node.js Buffers

Read raw Parquet bytes and write them to another file without decoding rows.

Run “Create a file” first. This feature uses the Node.js API.

```js
const { parquetToBuffer, bufferToParquet } = require('parquet-tool');

const bytes = parquetToBuffer('data.parquet', { validate: true });
bufferToParquet(bytes, 'restored.parquet', {
  validate: true,
  overwrite: true,
});
console.log('Bytes:', bytes.length);
```

Creates validated restored.parquet. overwrite: true allows repeated runs.
