# parquet-tool

[![npm version](https://img.shields.io/npm/v/parquet-tool.svg)](https://www.npmjs.com/package/parquet-tool) [![license](https://img.shields.io/npm/l/parquet-tool.svg)](LICENSE) [![build status](https://img.shields.io/github/actions/workflow/status/Lian0123/parquet-tool/ci.yml?branch=master)](https://github.com/Lian0123/parquet-tool/actions)

使用 TypeScript + C++ Native Addon 實作的 Parquet 檔案處理工具。
**不依賴任何現有的 npm parquet 套件**，核心 Parquet 格式讀寫由自行實作的 C++ Addon 完成。

## 🔧 功能

- **建立 & 讀寫** Parquet 檔案（支援 BOOLEAN、INT32、INT64、FLOAT、DOUBLE、STRING 類型）
- **Append（Apply）模式** — 追加資料到現有檔案
- **多檔合併** — 合併多個 Parquet 檔案並檢查 schema 相容性
- **格式驗證** — 驗證 Parquet 檔案結構、row group 與欄位資料完整性
- **CSV 轉換** — 支援 CSV 與 Parquet 雙向轉換
- **Apache Arrow 轉換** — 支援 Arrow IPC 與 Parquet 雙向轉換
- **切檔** — 將大檔案拆分成多個小檔案
- **平行處理** — 平行讀取、處理、寫入
- **Debug 模式** — CLI `--debug` 與程式庫 API 可輸出診斷資訊
- **CLI 命令列工具** — `info`、`read`、`write`、`append`、`split`、`merge`
- **可做為 npm 套件引入** — TypeScript API
- **Docker Compose 查看器** — Web UI 驗證 Parquet 檔案內容

## 🚀 快速開始

### 安裝與建置

```bash
npm install
npm run build
```

### 做為程式庫使用

```typescript
import {
  arrowToParquet,
  configureDebugMode,
  csvToParquet,
  mergeParquetFiles,
  ParquetWriter,
  ParquetReader,
  Schema,
  splitParquetFile,
  parallelRead,
  parquetToArrow,
  validateParquetFile,
} from 'parquet-tool';

// 建立 Schema
const schema = Schema.create({
  id: 'INT32',
  name: 'STRING',
  score: { type: 'DOUBLE', optional: true },
});

// 寫入
const writer = new ParquetWriter('output.parquet', schema);
writer.write([
  { id: 1, name: 'Alice', score: 95.5 },
  { id: 2, name: 'Bob', score: null },
]);
writer.close();

// 讀取
const reader = ParquetReader.open('output.parquet');
const data = reader.readAll();
console.log(data.columns);
reader.close();

// Append（Apply）模式
const appender = ParquetWriter.openForAppend('output.parquet');
appender.write({ id: 3, name: 'Charlie', score: 88.0 });
appender.close();

// 驗證
const validation = validateParquetFile('output.parquet');
console.log(validation.valid, validation.issues);

// CSV -> Parquet
csvToParquet('input.csv', 'input.parquet');

// Parquet -> Arrow
parquetToArrow('output.parquet', 'output.arrow');

// Arrow -> Parquet
arrowToParquet('output.arrow', 'from-arrow.parquet');

// 合併多個 Parquet 檔案
mergeParquetFiles(['part-1.parquet', 'part-2.parquet'], 'merged.parquet');

// Debug 模式
configureDebugMode({ enabled: true });
```

範例程式位於 `examples/example.ts`，可以用以下命令執行：

```bash
ts-node examples/example.ts
```

此程式會建立 `examples/sample.parquet`，並讀回內容顯示於 console。

## 📝 提交與發行

本專案使用 [Commitizen](https://github.com/commitizen/cz-cli) 來產生符合
[cZ Conventional Changelog](https://www.conventionalcommits.org/) 格式的
提交訊息。執行：

```bash
npm run cz
```

然後根據提示輸入主題。團隊成員可透過此機制保持版本歷史一致。

自動化發行由 `semantic-release` 控制，會根據提交類型計算版本號，
生成更改日誌，並將 artefact 發佈到 npm。觸發發行請執行：

```bash
npm run release
```


本專案也提供 CI 發行流程腳本，可在持續整合系統中運行：

```bash
npm run ci          # install, lint, test, build
npm run ci:release  # run ci and then semantic-release
```

此外，GitHub Actions 配置位於 `.github/workflows/ci.yml`，
於推送到 `main` 分支時執行同樣的步驟並觸發自動發行（需設置
`NPM_TOKEN` secret）。

### CLI 使用

```bash
# 查看檔案資訊
npx parquet-tool info data.parquet

# 讀取內容
npx parquet-tool read data.parquet --json
npx parquet-tool read data.parquet -n 50

# 從 JSON 建立
npx parquet-tool write output.parquet -i input.json -s "id:INT32,name:STRING"

# 追加資料
npx parquet-tool append output.parquet -i more_data.json

# 切檔
npx parquet-tool split large.parquet -n 10000 -o ./output/

# 合併
npx parquet-tool merge merged.parquet file1.parquet file2.parquet

# 驗證
npx parquet-tool validate data.parquet

# CSV 與 Parquet 互轉
npx parquet-tool csv-to-parquet data.csv data.parquet
npx parquet-tool parquet-to-csv data.parquet data.csv

# Arrow 與 Parquet 互轉
npx parquet-tool arrow-to-parquet data.arrow data.parquet
npx parquet-tool parquet-to-arrow data.parquet data.arrow

# Debug 模式
npx parquet-tool --debug validate data.parquet
```

### Docker 查看器

```bash
# 將要查看的 parquet 檔案放到 ./data/ 目錄
mkdir -p data
cp your_file.parquet data/

# 啟動查看器
docker-compose up --build

# 瀏覽 http://localhost:8080
```

## 🛠 開發

```bash
# 安裝依賴
npm install

# 建置 C++ Addon
npm run build:native

# 建置 TypeScript
npm run build:ts

# 執行測試
npm test

# 清除建置產物
npm run clean
```

## 🏗 架構

```
src/
├── native/         C++ 原生 Addon
│   ├── thrift.h    Thrift Compact Protocol 實作
│   ├── parquet.h   Parquet 格式讀寫
│   └── addon.cpp   N-API 綁定
├── lib/            TypeScript 程式庫
│   ├── types.ts    型別定義
│   ├── schema.ts   Schema 建構器
│   ├── binding.ts  Native 綁定載入
│   ├── reader.ts   ParquetReader
│   ├── writer.ts   ParquetWriter
│   ├── splitter.ts 切檔功能
│   ├── parallel.ts 平行處理
│   └── index.ts    匯出
└── cli/
    └── index.ts    CLI 程式
```

## 支援型別

| Parquet Type | TypeScript Type | 說明 |
|---|---|---|
| BOOLEAN | boolean | 布林值 |
| INT32 | number | 32 位元整數 |
| INT64 | bigint | 64 位元整數 |
| FLOAT | number | 32 位元浮點數 |
| DOUBLE | number | 64 位元浮點數 |
| BYTE_ARRAY | string | UTF-8 字串 |

## License

MIT
