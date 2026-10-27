# Parquet 使用教學與完整範例

https://lian0123.github.io/parquet-tool/guide.html

Node.js 18+。先安裝 parquet-tool，下載 [input.json](https://lian0123.github.io/parquet-tool/samples/input.json) 與 [input.csv](https://lian0123.github.io/parquet-tool/samples/input.csv) 到工作資料夾。Node.js 程式碼另存為 example.cjs 後執行 node example.cjs。

```sh
npm init -y
npm install parquet-tool
```

網頁版與目前 C++ CLI 的 Parquet 互通有限，請透過 CSV／JSON 交換資料。

## 建立第一個 Parquet 檔案

明確定義 schema，將 JSON 資料寫入 Parquet。rowGroupSize 設為 2，方便後續觀察 row group。

準備：先下載 input.json 到工作資料夾。重跑會覆寫 data.parquet。

### CLI

```sh
npx parquet-tool write data.parquet -i input.json -s "id:INT32,name:STRING,active:BOOLEAN" --row-group-size 2
```

### Node.js

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

執行後：產生包含 3 筆資料、2 個 row group 的 data.parquet。

[原始碼](https://github.com/Lian0123/parquet-tool/blob/main/src/lib/writer.ts)

## 讀取資料列

CLI 輸出 JSON；Node.js 使用 iterator 逐筆處理，避免一次合併整份檔案。

準備：先執行「建立檔案」，準備 data.parquet。

### CLI

```sh
npx parquet-tool read data.parquet --json --limit 20
```

### Node.js

```js
const { ParquetReader } = require('parquet-tool');

ParquetReader.withReader('data.parquet', (reader) => {
  for (const row of reader.iterateRows()) {
    console.log(row);
  }
}); // 自動關閉 reader
```

執行後：在終端機看到 id、name 與 active 欄位的資料。

[原始碼](https://github.com/Lian0123/parquet-tool/blob/main/src/lib/reader.ts)

## 查看 schema 與中繼資料

確認欄位型別、資料筆數及 row group 數量，先了解檔案結構。

準備：先執行「建立檔案」，準備 data.parquet。

### CLI

```sh
npx parquet-tool info data.parquet
```

### Node.js

```js
const { ParquetReader } = require('parquet-tool');

const metadata = ParquetReader.readMetadata('data.parquet');
console.log('Rows:', metadata.numRows);
console.log('Row groups:', metadata.numRowGroups);
console.table(metadata.schema);
```

執行後：顯示檔案的 schema、numRows、numRowGroups 等中繼資料。

[原始碼](https://github.com/Lian0123/parquet-tool/blob/main/src/lib/reader.ts)

## 為既有檔案新增資料

沿用原有 schema，追加新的 row group。每次執行都會再新增一筆資料。

準備：先執行「建立檔案」。CLI 先用 Node.js 產生 more.json；此範例會修改 data.parquet。

### CLI

```sh
node -e "require('node:fs').writeFileSync('more.json', JSON.stringify([{id:4,name:'Dora',active:true}]))"
npx parquet-tool append data.parquet -i more.json
npx parquet-tool info data.parquet
```

### Node.js

```js
const { ParquetWriter, ParquetReader } = require('parquet-tool');

ParquetWriter.withAppender('data.parquet', (writer) => {
  writer.write({ id: 4, name: 'Dora', active: true });
});
console.log(ParquetReader.readMetadata('data.parquet').numRows);
```

執行後：首次追加後，原始 3 筆資料會增加為 4 筆。

[原始碼](https://github.com/Lian0123/parquet-tool/blob/main/src/lib/writer.ts)

## CSV 與 Parquet 互轉

匯入有標題列的 CSV，自動推斷欄位型別，再匯出成 CSV。

準備：下載 input.csv 到工作資料夾。不需要先建立 data.parquet。

### CLI

```sh
npx parquet-tool csv-to-parquet input.csv from-csv.parquet
npx parquet-tool parquet-to-csv from-csv.parquet roundtrip.csv
```

### Node.js

```js
const { csvToParquet, parquetToCsv } = require('parquet-tool');

csvToParquet('input.csv', 'from-csv.parquet');
parquetToCsv('from-csv.parquet', 'roundtrip.csv');
```

執行後：產生 from-csv.parquet 與 roundtrip.csv，保留 3 筆示例資料。

[原始碼](https://github.com/Lian0123/parquet-tool/blob/main/src/lib/csv.ts)

## JSON 與 Parquet 互轉

從 JSON 陣列建立 Parquet，再匯出純資料列 JSON。省略 rows-only 時，預設會包含 schema。

準備：下載 input.json 到工作資料夾。不需要先建立 data.parquet。

### CLI

```sh
npx parquet-tool json-to-parquet input.json from-json.parquet
npx parquet-tool parquet-to-json from-json.parquet roundtrip.json --rows-only
```

### Node.js

```js
const { jsonToParquet, parquetToJson } = require('parquet-tool');

jsonToParquet('input.json', 'from-json.parquet');
parquetToJson('from-json.parquet', 'roundtrip.json', {
  includeSchema: false,
});
```

執行後：產生 from-json.parquet 與只包含資料陣列的 roundtrip.json。

[原始碼](https://github.com/Lian0123/parquet-tool/blob/main/src/lib/json.ts)

## Apache Arrow IPC 互轉

將 Parquet 轉成 Arrow IPC 檔案，再轉回 Parquet。

準備：先執行「建立檔案」，準備 data.parquet。

### CLI

```sh
npx parquet-tool parquet-to-arrow data.parquet data.arrow
npx parquet-tool arrow-to-parquet data.arrow from-arrow.parquet
```

### Node.js

```js
const { parquetToArrow, arrowToParquet } = require('parquet-tool');

parquetToArrow('data.parquet', 'data.arrow');
arrowToParquet('data.arrow', 'from-arrow.parquet');
```

執行後：產生 data.arrow 與 from-arrow.parquet。

[原始碼](https://github.com/Lian0123/parquet-tool/blob/main/src/lib/arrow.ts)

## XML 與 Parquet 互轉

預設將 schema 和資料一起匯出，回讀時可還原原本的欄位型別。

準備：先執行「建立檔案」，準備 data.parquet。

### CLI

```sh
npx parquet-tool parquet-to-xml data.parquet data.xml
npx parquet-tool xml-to-parquet data.xml from-xml.parquet
```

### Node.js

```js
const { parquetToXml, xmlToParquet } = require('parquet-tool');

parquetToXml('data.parquet', 'data.xml');
xmlToParquet('data.xml', 'from-xml.parquet');
```

執行後：產生 data.xml 與 from-xml.parquet。

[原始碼](https://github.com/Lian0123/parquet-tool/blob/main/src/lib/xml.ts)

## 切分檔案，再合併回來

每個輸出檔案最多 2 筆資料，合併時預設檢查 schema 相容性。

準備：先重新執行「建立檔案」，讓 data.parquet 回到 3 筆資料。示例檔案輸出至目前工作資料夾。

### CLI

```sh
npx parquet-tool split data.parquet -n 2 -p demo
npx parquet-tool merge merged.parquet demo_part0000.parquet demo_part0001.parquet
npx parquet-tool validate merged.parquet
```

### Node.js

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

執行後：產生 2 個 demo_part 檔案，merged.parquet 合併後共有 3 筆資料。

[原始碼](https://github.com/Lian0123/parquet-tool/blob/main/src/lib/splitter.ts)

## 驗證 Parquet 檔案

檢查檔案結構與 row group。CLI 遇到無效檔案會回傳非零結束碼，可用於腳本與 CI。

準備：先執行「建立檔案」，準備 data.parquet。

### CLI

```sh
npx parquet-tool validate data.parquet
```

### Node.js

```js
const { validateParquetFile } = require('parquet-tool');

const report = validateParquetFile('data.parquet');
console.log(report.valid, report.issues);
if (!report.valid) {
  process.exitCode = 1;
}
```

執行後：有效的示例檔案顯示 Valid: yes；Node.js 顯示 true 與空的 issues。

[原始碼](https://github.com/Lian0123/parquet-tool/blob/main/src/lib/validate.ts)

## 只處理指定的欄位與 row group

Node.js API 可選擇欄位和 row group。CLI 可指定 row group，但沒有欄位選擇選項。索引從 0 開始。

準備：先執行「建立檔案」，使用 rowGroupSize: 2 產生 data.parquet。

### CLI

```sh
npx parquet-tool read data.parquet --row-group 0 --json
```

### Node.js

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

執行後：CLI 顯示第一個 row group 的完整資料；Node.js 顯示其中的 id、name 欄位。欄位投影在讀取 row group 後進行。

[原始碼](https://github.com/Lian0123/parquet-tool/blob/main/src/lib/reader.ts)

## 以 Node.js Buffer 傳遞檔案

讀取原始 Parquet 位元組，再寫入另一個檔案。這個操作不會將資料解碼成資料列。

準備：先執行「建立檔案」，準備 data.parquet。本功能使用 Node.js API。

### Node.js

```js
const { parquetToBuffer, bufferToParquet } = require('parquet-tool');

const bytes = parquetToBuffer('data.parquet', { validate: true });
bufferToParquet(bytes, 'restored.parquet', {
  validate: true,
  overwrite: true,
});
console.log('Bytes:', bytes.length);
```

執行後：建立經過驗證的 restored.parquet。overwrite: true 允許重跑時覆寫。

[原始碼](https://github.com/Lian0123/parquet-tool/blob/main/src/lib/buffer.ts)
