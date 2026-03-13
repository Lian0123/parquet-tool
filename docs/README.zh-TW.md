# parquet-tool 文件（繁體中文）

`parquet-tool` 是以 TypeScript + C++ Native Addon 實作的 Parquet 工具。
核心 Parquet 讀寫邏輯由本專案自行實作，未直接依賴現有 npm parquet 套件。

## 功能摘要

- 建立、讀取、寫入 Parquet
- Append（apply）模式
- 多檔合併與 schema 檢查
- 檔案驗證
- CSV 與 Parquet 互轉
- Arrow IPC 與 Parquet 互轉
- 切檔與平行處理
- CLI 與 npm library 兩種使用方式

## 快速範例

```ts
import { ParquetReader, ParquetWriter, Schema } from 'parquet-tool';

const schema = Schema.create({
  id: 'INT32',
  name: 'STRING',
});

const writer = new ParquetWriter('demo.parquet', schema);
writer.write({ id: 1, name: 'Alice' });
writer.close();

const reader = ParquetReader.open('demo.parquet');
console.log(reader.readAll());
reader.close();
```

## 主要英文文件

- 專案首頁與完整範例：`../README.md`
- Overview: `overview.md`
- Installation: `installation.md`
- API: `api.md`
- CLI: `cli.md`
- Examples: `examples.md`
