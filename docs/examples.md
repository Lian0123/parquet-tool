# Examples

## TypeScript example

See `examples/example.ts` which demonstrates writing and reading a Parquet
file:

```bash
npx ts-node examples/example.ts
```

This generates `examples/sample.parquet` and prints its metadata and contents.

## Docker viewer

A simple Flask web app lets you browse Parquet files mounted under `./data`.

```bash
# copy files to `data` folder
mkdir -p data
cp examples/sample.parquet data/

docker-compose up --build
```

Visit <http://localhost:8080> to see file listing, metadata, and row previews.
