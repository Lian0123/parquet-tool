# parquet-tool

使用 TypeScript + C++ Native Addon 實作的 Parquet 檔案處理工具。
**不依賴任何現有的 npm parquet 套件**，核心 Parquet 格式讀寫由自行實作的 C++ Addon 完成。

## 功能

- **建立 & 讀寫** Parquet 檔案（支援 BOOLEAN、INT32、INT64、FLOAT、DOUBLE、STRING 類型）
- **Append（Apply）模式** — 追加資料到現有檔案
- **切檔** — 將大檔案拆分成多個小檔案
- **平行處理** — 平行讀取、處理、寫入
- **CLI 命令列工具** — `info`、`read`、`write`、`append`、`split`、`merge`
- **可做為 npm 套件引入** — TypeScript API
- **Docker Compose 查看器** — Web UI 驗證 Parquet 檔案內容

## 快速開始

### 安裝與建置

```bash
npm install
npm run build
```

### 做為程式庫使用

```typescript
import {
  ParquetWriter,
  ParquetReader,
  Schema,
  splitParquetFile,
  parallelRead,
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

// 切檔
const files = splitParquetFile('output.parquet', { maxRowsPerFile: 1000 });

// 平行讀取
const allData = await parallelRead('output.parquet', { concurrency: 4 });
```

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

## 開發

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

## 架構

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
