import * as fs from 'fs';
import * as path from 'path';
import {
  bufferToParquet,
  parquetToBuffer,
  ParquetReader,
  ParquetWriter,
  Schema,
} from '../src/lib';

const TMP = path.join(__dirname, '..', 'tmp', 'buffer');

beforeAll(() => {
  if (!fs.existsSync(TMP)) fs.mkdirSync(TMP, { recursive: true });
});

afterAll(() => {
  fs.rmSync(TMP, { recursive: true, force: true });
});

describe('parquetToBuffer & bufferToParquet', () => {
  it('should round-trip a parquet file through Buffer', () => {
    const src = path.join(TMP, 'src.parquet');
    const dst = path.join(TMP, 'dst.parquet');

    const schema = Schema.create({ id: 'INT32', name: 'STRING' });
    const writer = new ParquetWriter(src, schema);
    writer.write([
      { id: 1, name: 'Alice' },
      { id: 2, name: 'Bob' },
    ]);
    writer.close();

    const buf = parquetToBuffer(src, { validate: true });
    bufferToParquet(buf, dst, { overwrite: false, validate: true });

    const reader = ParquetReader.open(dst);
    const data = reader.readAll();
    reader.close();

    expect(data.numRows).toBe(2);
    expect(data.columns['id']).toEqual([1, 2]);
    expect(data.columns['name']).toEqual(['Alice', 'Bob']);
  });

  it('should not overwrite by default', () => {
    const file = path.join(TMP, 'no_overwrite.parquet');
    fs.writeFileSync(file, Buffer.from('not parquet'));
    expect(() => bufferToParquet(Buffer.from('abc'), file)).toThrow();
  });

  it('should fail validation for non-parquet input when validate=true', () => {
    const file = path.join(TMP, 'not_parquet.bin');
    fs.writeFileSync(file, Buffer.from('not parquet'));
    expect(() => parquetToBuffer(file, { validate: true })).toThrow(
      /Invalid Parquet file/,
    );
  });
});
