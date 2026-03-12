# parquet-tool ドキュメント（日本語）

`parquet-tool` は TypeScript + C++ Native Addon で実装された Parquet ツールです。
Parquet のコア読み書き処理はこのリポジトリ内で実装されており、既存の npm parquet パッケージには依存していません。

## 主な機能

- Parquet の作成、読み取り、書き込み
- Append（apply）モード
- 複数ファイルのマージと schema 検証
- ファイル構造のバリデーション
- CSV と Parquet の相互変換
- Arrow IPC と Parquet の相互変換
- 分割保存と並列処理
- CLI と npm ライブラリの両方で利用可能

## クイックサンプル

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

## 英語ドキュメント

- プロジェクト概要と詳細例: `../README.md`
- Overview: `overview.md`
- Installation: `installation.md`
- API: `api.md`
- CLI: `cli.md`
- Examples: `examples.md`
