# Examples

Repository examples are organized into separate folders so each workflow can be
run independently.

## TypeScript examples

- `examples/basic-read-write/index.ts` - write, append, and read rows
- `examples/merge-and-validate/index.ts` - merge files and validate output
- `examples/conversions/index.ts` - CSV, Parquet, and Arrow conversions
- `examples/split-and-parallel/index.ts` - split files and use parallel helpers
- `examples/buffer-roundtrip/index.ts` - copy Parquet bytes through a buffer

Run any example from the repository root:

```bash
npx ts-node examples/basic-read-write/index.ts
npx ts-node examples/merge-and-validate/index.ts
npx ts-node examples/conversions/index.ts
npx ts-node examples/split-and-parallel/index.ts
npx ts-node examples/buffer-roundtrip/index.ts
```

The legacy `examples/example.ts` entry point remains available and delegates to
the basic read/write example.

## Docker viewer

A simple Flask web app lets you browse Parquet files mounted under `./data`.

```bash
# copy files to `data` folder
mkdir -p data
cp examples/sample.parquet data/

docker-compose up --build
```

Visit <http://localhost:8080> to see file listing, metadata, and row previews.
