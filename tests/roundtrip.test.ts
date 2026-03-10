import * as path from 'path';
import * as fs from 'fs';
import { ParquetWriter, ParquetReader, Schema, ParquetType } from '../src/lib';

const TMP = path.join(__dirname, '..', 'tmp', 'roundtrip');

beforeAll(() => {
  if (!fs.existsSync(TMP)) fs.mkdirSync(TMP, { recursive: true });
});

afterAll(() => {
  fs.rmSync(TMP, { recursive: true, force: true });
});

describe('ParquetWriter & ParquetReader round-trip', () => {
  it('should write and read INT32 + STRING columns', () => {
    const file = path.join(TMP, 'roundtrip_basic.parquet');
    const schema = Schema.create({ id: 'INT32', name: 'STRING' });

    const writer = new ParquetWriter(file, schema);
    writer.write([
      { id: 1, name: 'Alice' },
      { id: 2, name: 'Bob' },
      { id: 3, name: 'Charlie' },
    ]);
    writer.close();

    const reader = ParquetReader.open(file);
    const meta = reader.getMetadata();
    expect(meta.numRows).toBe(3);
    expect(meta.numRowGroups).toBe(1);
    expect(meta.schema).toHaveLength(2);

    const data = reader.readAll();
    expect(data.numRows).toBe(3);
    expect(data.columns['id']).toEqual([1, 2, 3]);
    expect(data.columns['name']).toEqual(['Alice', 'Bob', 'Charlie']);
    reader.close();
  });

  it('should handle all basic types', () => {
    const file = path.join(TMP, 'roundtrip_types.parquet');
    const schema = new Schema()
      .addColumn('bool_col', ParquetType.BOOLEAN)
      .addColumn('int32_col', ParquetType.INT32)
      .addColumn('int64_col', ParquetType.INT64)
      .addColumn('float_col', ParquetType.FLOAT)
      .addColumn('double_col', ParquetType.DOUBLE)
      .addColumn('string_col', ParquetType.BYTE_ARRAY)
      .build();

    const writer = new ParquetWriter(file, schema);
    writer.write([
      {
        bool_col: true,
        int32_col: 42,
        int64_col: 123456789,
        float_col: 3.14,
        double_col: 2.718281828,
        string_col: 'hello',
      },
      {
        bool_col: false,
        int32_col: -1,
        int64_col: -987654321,
        float_col: -0.5,
        double_col: 0.0,
        string_col: '',
      },
    ]);
    writer.close();

    const reader = ParquetReader.open(file);
    const data = reader.readAll();
    expect(data.numRows).toBe(2);

    expect(data.columns['bool_col']).toEqual([true, false]);
    expect(data.columns['int32_col']).toEqual([42, -1]);
    // INT64 is returned as BigInt
    expect(data.columns['int64_col']).toEqual([
      BigInt(123456789),
      BigInt(-987654321),
    ]);
    // Float precision may differ
    expect(data.columns['float_col'][0]).toBeCloseTo(3.14, 1);
    expect(data.columns['float_col'][1]).toBeCloseTo(-0.5, 5);
    expect(data.columns['double_col']).toEqual([2.718281828, 0.0]);
    expect(data.columns['string_col']).toEqual(['hello', '']);
    reader.close();
  });

  it('should handle OPTIONAL columns with nulls', () => {
    const file = path.join(TMP, 'roundtrip_optional.parquet');
    const schema = Schema.create({
      id: 'INT32',
      label: { type: 'STRING', optional: true },
    });

    const writer = new ParquetWriter(file, schema);
    writer.write([
      { id: 1, label: 'foo' },
      { id: 2, label: null },
      { id: 3, label: 'bar' },
      { id: 4 },           // undefined → null
      { id: 5, label: 'baz' },
    ]);
    writer.close();

    const reader = ParquetReader.open(file);
    const data = reader.readAll();
    expect(data.numRows).toBe(5);
    expect(data.columns['id']).toEqual([1, 2, 3, 4, 5]);
    expect(data.columns['label']).toEqual(['foo', null, 'bar', null, 'baz']);
    reader.close();
  });

  it('should handle multiple row groups', () => {
    const file = path.join(TMP, 'roundtrip_rg.parquet');
    const schema = Schema.create({ value: 'INT32' });

    // Small row group size to force multiple groups
    const writer = new ParquetWriter(file, schema, { rowGroupSize: 3 });
    for (let i = 0; i < 10; i++) {
      writer.write({ value: i });
    }
    writer.close();

    const reader = ParquetReader.open(file);
    const meta = reader.getMetadata();
    expect(meta.numRows).toBe(10);
    // 10 rows / 3 per group → 4 groups (3+3+3+1)
    expect(meta.numRowGroups).toBe(4);

    const data = reader.readAll();
    expect(data.columns['value']).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    reader.close();
  });

  it('should iterate over rows', () => {
    const file = path.join(TMP, 'roundtrip_iter.parquet');
    const schema = Schema.create({ x: 'INT32' });
    const writer = new ParquetWriter(file, schema);
    writer.write([{ x: 10 }, { x: 20 }, { x: 30 }]);
    writer.close();

    const reader = ParquetReader.open(file);
    const rows = [...reader];
    expect(rows).toEqual([{ x: 10 }, { x: 20 }, { x: 30 }]);
    reader.close();
  });

  it('should report metadata correctly', () => {
    const file = path.join(TMP, 'roundtrip_meta.parquet');
    const schema = Schema.create({ a: 'INT32', b: 'DOUBLE' });
    const writer = new ParquetWriter(file, schema);
    writer.write([{ a: 1, b: 1.1 }]);
    writer.close();

    const meta = ParquetReader.readMetadata(file);
    expect(meta.version).toBe(2);
    expect(meta.numRows).toBe(1);
    expect(meta.schema).toHaveLength(2);
    expect(meta.schema[0].name).toBe('a');
    expect(meta.schema[0].type).toBe(ParquetType.INT32);
    expect(meta.schema[1].name).toBe('b');
    expect(meta.schema[1].type).toBe(ParquetType.DOUBLE);
  });
});
