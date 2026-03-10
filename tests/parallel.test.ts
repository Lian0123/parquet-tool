import * as path from 'path';
import * as fs from 'fs';
import {
  ParquetWriter,
  ParquetReader,
  Schema,
  parallelRead,
  parallelProcess,
  parallelWrite,
} from '../src/lib';

const TMP = path.join(__dirname, '..', 'tmp', 'parallel');

beforeAll(() => {
  if (!fs.existsSync(TMP)) fs.mkdirSync(TMP, { recursive: true });
});
afterAll(() => {
  fs.rmSync(TMP, { recursive: true, force: true });
});

describe('parallelRead', () => {
  it('should read all row groups in parallel', async () => {
    const file = path.join(TMP, 'parallel_read.parquet');
    const schema = Schema.create({ x: 'INT32' });

    // Write with small row groups to get multiple groups
    const writer = new ParquetWriter(file, schema, { rowGroupSize: 5 });
    for (let i = 0; i < 20; i++) writer.write({ x: i });
    writer.close();

    const data = await parallelRead(file, { concurrency: 3 });
    expect(data.numRows).toBe(20);
    expect(data.columns['x']).toEqual(Array.from({ length: 20 }, (_, i) => i));
  });
});

describe('parallelProcess', () => {
  it('should process row groups in parallel', async () => {
    const file = path.join(TMP, 'parallel_process.parquet');
    const schema = Schema.create({ v: 'INT32' });

    const writer = new ParquetWriter(file, schema, { rowGroupSize: 5 });
    for (let i = 0; i < 15; i++) writer.write({ v: i + 1 });
    writer.close();

    const sums = await parallelProcess<number>(
      file,
      (rows) => {
        const sum = rows.reduce((a, r) => a + (r.v as number), 0);
        return [sum];
      },
      { concurrency: 2 },
    );

    // 3 row groups → 3 sums
    // Group 0: 1+2+3+4+5=15, Group 1: 6+7+8+9+10=40, Group 2: 11+12+13+14+15=65
    expect(sums.sort((a, b) => a - b)).toEqual([15, 40, 65]);
  });
});

describe('parallelWrite', () => {
  it('should write data chunks in parallel then merge', async () => {
    const file = path.join(TMP, 'parallel_write.parquet');
    const schema = Schema.create({ k: 'INT32', s: 'STRING' });

    const chunks = [
      [
        { k: 1, s: 'a' },
        { k: 2, s: 'b' },
      ],
      [
        { k: 3, s: 'c' },
        { k: 4, s: 'd' },
      ],
      [
        { k: 5, s: 'e' },
      ],
    ];

    await parallelWrite(file, schema, chunks, {
      concurrency: 2,
      tempDir: TMP,
    });

    const reader = ParquetReader.open(file);
    const data = reader.readAll();
    expect(data.numRows).toBe(5);
    expect(data.columns['k']).toEqual([1, 2, 3, 4, 5]);
    expect(data.columns['s']).toEqual(['a', 'b', 'c', 'd', 'e']);
    reader.close();
  });
});
