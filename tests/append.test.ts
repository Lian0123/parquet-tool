import * as path from 'path';
import * as fs from 'fs';
import { ParquetWriter, ParquetReader, Schema } from '../src/lib';

const TMP = path.join(__dirname, '..', 'tmp', 'append');

beforeAll(() => {
  if (!fs.existsSync(TMP)) fs.mkdirSync(TMP, { recursive: true });
});
afterAll(() => {
  fs.rmSync(TMP, { recursive: true, force: true });
});

describe('Append (apply) mode', () => {
  it('should append rows to an existing file', () => {
    const file = path.join(TMP, 'append.parquet');
    const schema = Schema.create({ id: 'INT32', tag: 'STRING' });

    // Initial write
    const w1 = new ParquetWriter(file, schema);
    w1.write([
      { id: 1, tag: 'a' },
      { id: 2, tag: 'b' },
    ]);
    w1.close();

    // Append
    const w2 = ParquetWriter.openForAppend(file);
    w2.write([
      { id: 3, tag: 'c' },
      { id: 4, tag: 'd' },
    ]);
    w2.close();

    // Read and verify
    const reader = ParquetReader.open(file);
    const meta = reader.getMetadata();
    expect(meta.numRows).toBe(4);
    expect(meta.numRowGroups).toBe(2); // one from initial, one from append

    const data = reader.readAll();
    expect(data.columns['id']).toEqual([1, 2, 3, 4]);
    expect(data.columns['tag']).toEqual(['a', 'b', 'c', 'd']);
    reader.close();
  });

  it('should append multiple times', () => {
    const file = path.join(TMP, 'append_multi.parquet');
    const schema = Schema.create({ n: 'INT32' });

    const w1 = new ParquetWriter(file, schema);
    w1.write({ n: 1 });
    w1.close();

    for (let i = 2; i <= 5; i++) {
      const w = ParquetWriter.openForAppend(file);
      w.write({ n: i });
      w.close();
    }

    const reader = ParquetReader.open(file);
    const data = reader.readAll();
    expect(data.numRows).toBe(5);
    expect(data.columns['n']).toEqual([1, 2, 3, 4, 5]);
    reader.close();
  });
});
