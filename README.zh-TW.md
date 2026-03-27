# parquet-tool

[![npm version](https://img.shields.io/npm/v/parquet-tool.svg)](https://www.npmjs.com/package/parquet-tool) [![license](https://img.shields.io/npm/l/parquet-tool.svg)](LICENSE) [![build status](https://img.shields.io/github/actions/workflow/status/Lian0123/parquet-tool/ci.yml?branch=main)](https://github.com/Lian0123/parquet-tool/actions)

以 TypeScript + C++ Native Addon 打造的 Parquet 處理工具包。
本專案不依賴現有的 npm parquet 套件；核心 Parquet 讀寫邏輯直接在此儲存庫中實作。

語言文件：

- 繁體中文：[docs/README.zh-TW.md](https://github.com/Lian0123/parquet-tool/blob/main/docs/README.zh-TW.md)
- 日文：[docs/README.ja.md](https://github.com/Lian0123/parquet-tool/blob/main/docs/README.ja.md)

## 功能

- 建立並讀取/寫入 Parquet 檔案
- Append 模式（在較早的需求中稱為 "apply"），可為既有檔案新增新的 row group
- 合併多個 Parquet 檔案，並檢查 schema 相容性
- 驗證 Parquet 結構、中繼資料與 row group
- CSV 與 Parquet 互轉
- Apache Arrow IPC 與 Parquet 互轉
- 將大型 Parquet 檔案切分為較小檔案
- 提供平行讀取/處理/寫入輔助工具
- CLI 與函式庫 API 都支援除錯模式
- CLI 指令：`info`、`read`、`write`、`append`、`split`、`merge`、`validate`、`csv-to-parquet`、`parquet-to-csv`、`arrow-to-parquet`、`parquet-to-arrow`
- 提供 Docker Compose viewer 以便快速檢查結果

## 快速開始

```bash
npm install parquet-tool
```

## 範例

儲存庫另外在 `examples/` 下提供可直接執行的腳本，並依工作流程分資料夾整理。

### 1. 基本寫入、讀取與 append

```ts
import { ParquetReader, ParquetWriter, Schema } from 'parquet-tool';

const schema = Schema.create({
  id: 'INT32',
  name: 'STRING',
  score: { type: 'DOUBLE', optional: true },
});

const writer = new ParquetWriter('output.parquet', schema);
writer.write([
  { id: 1, name: 'Alice', score: 98.5 },
  { id: 2, name: 'Bob' },
]);
writer.close();

const appender = ParquetWriter.openForAppend('output.parquet');
appender.write({ id: 3, name: 'Charlie', score: 75.0 });
appender.close();

const reader = ParquetReader.open('output.parquet');
const all = reader.readAll();
console.log(all.numRows, all.columns);
reader.close();
```

### 2. 合併與驗證

```ts
import { mergeParquetFiles, validateParquetFile } from 'parquet-tool';

mergeParquetFiles(['part-1.parquet', 'part-2.parquet'], 'merged.parquet');

const report = validateParquetFile('merged.parquet');
if (!report.valid) {
  console.error(report.issues);
}
```

### 3. CSV 與 Arrow 轉換

```ts
import {
  arrowToParquet,
  csvToParquet,
  parquetToArrow,
  parquetToCsv,
} from 'parquet-tool';

csvToParquet('input.csv', 'input.parquet');
parquetToCsv('input.parquet', 'roundtrip.csv');

parquetToArrow('input.parquet', 'input.arrow');
arrowToParquet('input.arrow', 'from-arrow.parquet');
```

### 4. 切檔與平行處理

```ts
import {
  parallelProcess,
  parallelRead,
  splitParquetFile,
} from 'parquet-tool';

const files = splitParquetFile('large.parquet', {
  maxRowsPerFile: 100_000,
  outputDir: './parts',
  prefix: 'large',
});
console.log(files);

const combined = await parallelRead('large.parquet', { concurrency: 4 });
console.log(combined.numRows);

const names = await parallelProcess(
  'large.parquet',
  (rows) => rows.map((row) => String(row.name ?? '')),
  { concurrency: 4 },
);
console.log(names.length);
```

### 5. 執行儲存庫內建範例

```bash
npx ts-node examples/basic-read-write/index.ts
npx ts-node examples/merge-and-validate/index.ts
npx ts-node examples/conversions/index.ts
npx ts-node examples/split-and-parallel/index.ts
npx ts-node examples/buffer-roundtrip/index.ts
```

可用的範例資料夾：

- `examples/basic-read-write/`
- `examples/merge-and-validate/`
- `examples/conversions/`
- `examples/split-and-parallel/`
- `examples/buffer-roundtrip/`

舊版的 `examples/example.ts` 入口仍可使用，並會轉送到基本範例。

## CLI 用法

```bash
# 中繼資料
npx parquet-tool info data.parquet

# 讀取資料列
npx parquet-tool read data.parquet --json
npx parquet-tool read data.parquet --limit 50

# 從 JSON 寫入
npx parquet-tool write out.parquet -i input.json -s "id:INT32,name:STRING"

# Append 資料列
npx parquet-tool append out.parquet -i more.json

# 切檔 / 合併
npx parquet-tool split large.parquet -n 10000 -o ./output
npx parquet-tool merge merged.parquet part1.parquet part2.parquet

# 驗證
npx parquet-tool validate merged.parquet

# CSV <-> Parquet
npx parquet-tool csv-to-parquet input.csv output.parquet
npx parquet-tool parquet-to-csv output.parquet output.csv

# Arrow <-> Parquet
npx parquet-tool arrow-to-parquet input.arrow output.parquet
npx parquet-tool parquet-to-arrow output.parquet output.arrow

# 除錯模式
npx parquet-tool --debug validate data.parquet
```

## Docker Viewer

```bash
mkdir -p data
cp your_file.parquet data/
docker-compose up --build
```

開啟 `http://localhost:8080`。

## 開發

```bash
npm install
npm run build:native
npm run build:ts
npm test
npm run clean
```

## 發佈

本專案使用 Commitizen + semantic-release。

```bash
npm run cz
npm run release
```

已設定的 semantic-release 外掛：

- `@semantic-release/commit-analyzer`
- `@semantic-release/release-notes-generator`
- `@semantic-release/changelog`
- `@semantic-release/npm`
- `@semantic-release/github`
- `@semantic-release/git`

分支策略：

- `main`：穩定版本發佈

## 支援型別

| Parquet 型別 | TypeScript 型別 | 說明 |
|---|---|---|
| BOOLEAN | boolean | 布林值 |
| INT32 | number | 32 位元整數 |
| INT64 | bigint | 64 位元整數 |
| FLOAT | number | 32 位元浮點數 |
| DOUBLE | number | 64 位元浮點數 |
| BYTE_ARRAY | string | UTF-8 字串 |

## 授權

MIT
