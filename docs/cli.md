# CLI Usage

The command-line tool `parquet-tool` is installed when building the project. It
can be invoked via `npx parquet-tool` or after global linking.

```bash
# show file information
parquet-tool info data.parquet

# read rows
parquet-tool read data.parquet --json
parquet-tool read data.parquet -n 50 --row-group 0

# write from JSON
parquet-tool write out.parquet -i input.json \
  -s "id:INT32,name:STRING" --row-group-size 10000

# append rows
parquet-tool append out.parquet -i more.json

# split file
parquet-tool split big.parquet -n 100000 -o ./out -p part

# merge files
parquet-tool merge merged.parquet a.parquet b.parquet
```

The CLI uses the same schema definitions as the library; specify schema with
`name:TYPE` pairs separated by commas.
