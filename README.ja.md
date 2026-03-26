# parquet-tool

[![npm version](https://img.shields.io/npm/v/parquet-tool.svg)](https://www.npmjs.com/package/parquet-tool) [![license](https://img.shields.io/npm/l/parquet-tool.svg)](LICENSE) [![build status](https://img.shields.io/github/actions/workflow/status/Lian0123/parquet-tool/ci.yml?branch=main)](https://github.com/Lian0123/parquet-tool/actions)

TypeScript + C++ Native Addon で構築された Parquet 処理ツールキットです。
既存の npm parquet パッケージには依存せず、Parquet のコア読み書きロジックをこのリポジトリ内で実装しています。

言語ドキュメント:

- 中国語（繁体字）: [docs/README.zh-TW.md](https://github.com/Lian0123/parquet-tool/blob/main/docs/README.zh-TW.md)
- 日本語: [docs/README.ja.md](https://github.com/Lian0123/parquet-tool/blob/main/docs/README.ja.md)

## 機能

- Parquet ファイルの作成と読み取り/書き込み
- Append モード（以前の要件では "apply" と呼称）による既存ファイルへの新規 row group 追加
- schema 互換性チェック付きの複数 Parquet ファイルのマージ
- Parquet 構造、メタデータ、row group の検証
- CSV と Parquet の相互変換
- Apache Arrow IPC と Parquet の相互変換
- 大きな Parquet ファイルの分割
- 並列読み取り/処理/書き込みヘルパー
- CLI とライブラリ API の両方でデバッグモードを利用可能
- CLI コマンド: `info`, `read`, `write`, `append`, `split`, `merge`, `validate`, `csv-to-parquet`, `parquet-to-csv`, `arrow-to-parquet`, `parquet-to-arrow`
- すばやく確認できる Docker Compose viewer

## クイックスタート

```bash
npm install parquet-tool
```

## 例

リポジトリには `examples/` 配下に、ワークフローごとに分けた実行可能なサンプルも用意されています。

### 1. 基本的な書き込み、読み取り、append

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

### 2. マージと検証

```ts
import { mergeParquetFiles, validateParquetFile } from 'parquet-tool';

mergeParquetFiles(['part-1.parquet', 'part-2.parquet'], 'merged.parquet');

const report = validateParquetFile('merged.parquet');
if (!report.valid) {
  console.error(report.issues);
}
```

### 3. CSV と Arrow の変換

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

### 4. 分割と並列処理

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

### 5. リポジトリ内サンプルを実行する

```bash
npx ts-node examples/basic-read-write/index.ts
npx ts-node examples/merge-and-validate/index.ts
npx ts-node examples/conversions/index.ts
npx ts-node examples/split-and-parallel/index.ts
npx ts-node examples/buffer-roundtrip/index.ts
```

利用可能なサンプルフォルダ:

- `examples/basic-read-write/`
- `examples/merge-and-validate/`
- `examples/conversions/`
- `examples/split-and-parallel/`
- `examples/buffer-roundtrip/`

従来の `examples/example.ts` エントリポイントも引き続き利用でき、基本サンプルへ委譲します。

## CLI の使い方

```bash
# メタデータ
npx parquet-tool info data.parquet

# 行の読み取り
npx parquet-tool read data.parquet --json
npx parquet-tool read data.parquet --limit 50

# JSON から書き込み
npx parquet-tool write out.parquet -i input.json -s "id:INT32,name:STRING"

# 行の append
npx parquet-tool append out.parquet -i more.json

# 分割 / マージ
npx parquet-tool split large.parquet -n 10000 -o ./output
npx parquet-tool merge merged.parquet part1.parquet part2.parquet

# 検証
npx parquet-tool validate merged.parquet

# CSV <-> Parquet
npx parquet-tool csv-to-parquet input.csv output.parquet
npx parquet-tool parquet-to-csv output.parquet output.csv

# Arrow <-> Parquet
npx parquet-tool arrow-to-parquet input.arrow output.parquet
npx parquet-tool parquet-to-arrow output.parquet output.arrow

# デバッグモード
npx parquet-tool --debug validate data.parquet
```

## Docker Viewer

```bash
mkdir -p data
cp your_file.parquet data/
docker-compose up --build
```

`http://localhost:8080` を開いてください。

## 開発

```bash
npm install
npm run build:native
npm run build:ts
npm test
npm run clean
```

## リリース

このプロジェクトは Commitizen + semantic-release を使用します。

```bash
npm run cz
npm run release
```

設定済みの semantic-release プラグイン:

- `@semantic-release/commit-analyzer`
- `@semantic-release/release-notes-generator`
- `@semantic-release/changelog`
- `@semantic-release/npm`
- `@semantic-release/github`
- `@semantic-release/git`

ブランチ戦略:

- `main`: 安定版リリース

## 対応型

| Parquet 型 | TypeScript 型 | 備考 |
|---|---|---|
| BOOLEAN | boolean | 真偽値 |
| INT32 | number | 32 ビット整数 |
| INT64 | bigint | 64 ビット整数 |
| FLOAT | number | 32 ビット浮動小数点 |
| DOUBLE | number | 64 ビット浮動小数点 |
| BYTE_ARRAY | string | UTF-8 文字列 |

## ライセンス

MIT
