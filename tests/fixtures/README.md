# Parquet compatibility fixtures

`legacy-schema.parquet` was captured from this project's pre-fix native writer.
It contains `{id:1,name:"legacy"}` and `{id:2,name:null}` and guards legacy reading.

The `standard-*.parquet` files were produced independently with PyArrow 17.0.0:

```python
import pyarrow as pa, pyarrow.parquet as pq
table = pa.table({'id': pa.array([1, 2], type=pa.int32()),
                  'name': ['標準', None], 'active': [True, False]})
for name, options in [
    ('standard-plain', dict(compression=None, use_dictionary=False)),
    ('standard-snappy', dict(compression='snappy', use_dictionary=False)),
    ('standard-dictionary', dict(compression=None, use_dictionary=True)),
]:
    pq.write_table(table, name + '.parquet', version='1.0',
                   data_page_version='1.0', **options)
```

PLAIN/uncompressed is supported; Snappy and dictionary encodings must fail with
explicit unsupported-format errors. PyArrow is only a fixture-generation/QA tool,
not a production or test-install dependency.
