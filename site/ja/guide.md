# Parquet の使い方と完全な使用例

https://lian0123.github.io/parquet-tool/ja/guide.html

Node.js 18 以降。npm install parquet-tool を実行し、input.json と input.csv を同じフォルダーに保存してください。コードは example.cjs として保存し、node example.cjs で実行します。

## 最初の Parquet ファイルを作成

schema を明示して JSON を Parquet に書き込みます。rowGroupSize を 2 にして、後で row group を確認できるようにします。

input.json を作業フォルダーにダウンロードします。再実行すると data.parquet を上書きします。

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

3 行・2 個の row group を含む data.parquet を作成します。

## データ行を読み取る

CLI は JSON を出力します。Node.js は iterator で行を処理し、ファイル全体を一度に結合しません。

先に「ファイル作成」を実行し、data.parquet を用意します。

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

ターミナルに id・name・active の値を表示します。

## schema とメタデータを確認

列の型・行数・row group 数からファイルの構造を確認します。

先に「ファイル作成」を実行し、data.parquet を用意します。

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

schema・numRows・numRowGroups などのメタデータを表示します。

## 既存ファイルに追記

既存の schema を使用し、新しい row group を追加します。実行するたびに 1 行増えます。

先に「ファイル作成」を実行します。CLI の例は Node.js で more.json を作成し、data.parquet を変更します。

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

最初の追記で、元の 3 行が 4 行になります。

## CSV と Parquet を相互変換

ヘッダー付き CSV を読み込み、型を推定してから CSV に出力します。

input.csv を作業フォルダーにダウンロードします。data.parquet は不要です。

```sh
npx parquet-tool csv-to-parquet input.csv from-csv.parquet
npx parquet-tool parquet-to-csv from-csv.parquet roundtrip.csv
```

```js
const { csvToParquet, parquetToCsv } = require('parquet-tool');

csvToParquet('input.csv', 'from-csv.parquet');
parquetToCsv('from-csv.parquet', 'roundtrip.csv');
```

3 行のサンプルを含む from-csv.parquet と roundtrip.csv を作成します。

## JSON と Parquet を相互変換

JSON 配列から Parquet を作成し、行データのみの JSON を出力します。rows-only を省略すると schema も含まれます。

input.json を作業フォルダーにダウンロードします。data.parquet は不要です。

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

from-json.parquet と、データ配列のみの roundtrip.json を作成します。

## Apache Arrow IPC を相互変換

Parquet を Arrow IPC ファイルに変換してから戻します。

先に「ファイル作成」を実行し、data.parquet を用意します。

```sh
npx parquet-tool parquet-to-arrow data.parquet data.arrow
npx parquet-tool arrow-to-parquet data.arrow from-arrow.parquet
```

```js
const { parquetToArrow, arrowToParquet } = require('parquet-tool');

parquetToArrow('data.parquet', 'data.arrow');
arrowToParquet('data.arrow', 'from-arrow.parquet');
```

data.arrow と from-arrow.parquet を作成します。

## XML と Parquet を相互変換

既定では schema とデータを出力し、読み込み時に列の型を復元できます。

先に「ファイル作成」を実行し、data.parquet を用意します。

```sh
npx parquet-tool parquet-to-xml data.parquet data.xml
npx parquet-tool xml-to-parquet data.xml from-xml.parquet
```

```js
const { parquetToXml, xmlToParquet } = require('parquet-tool');

parquetToXml('data.parquet', 'data.xml');
xmlToParquet('data.xml', 'from-xml.parquet');
```

data.xml と from-xml.parquet を作成します。

## ファイルを分割して結合

各出力ファイルを最大 2 行に分割します。結合時は既定で schema の互換性を検証します。

「ファイル作成」を再実行して 3 行に戻します。出力先は現在の作業フォルダーです。

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

demo_part ファイルを 2 個作成し、結合後の merged.parquet は 3 行になります。

## Parquet ファイルを検証

構造と row group を検証します。不正なファイルでは CLI が非ゼロの終了コードを返すので、スクリプトや CI に使えます。

先に「ファイル作成」を実行し、data.parquet を用意します。

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

正常なサンプルでは Valid: yes、Node.js では true と空の issues を表示します。

## 指定した列と row group を処理

Node.js API は列と row group を選択できます。CLI は row group のみ指定できます。インデックスは 0 から始まります。

「ファイル作成」で rowGroupSize: 2 の data.parquet を作成します。

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

CLI は最初の row group 全体、Node.js はその id・name 列を表示します。列の絞り込みは row group を読んだ後に行います。

## Node.js Buffer でファイルを扱う

Parquet のバイト列を読み、行データに復号せず別ファイルに書き込みます。

先に「ファイル作成」を実行してください。この機能は Node.js API を使用します。

```js
const { parquetToBuffer, bufferToParquet } = require('parquet-tool');

const bytes = parquetToBuffer('data.parquet', { validate: true });
bufferToParquet(bytes, 'restored.parquet', {
  validate: true,
  overwrite: true,
});
console.log('Bytes:', bytes.length);
```

検証済みの restored.parquet を作成します。overwrite: true で再実行時に上書きできます。
