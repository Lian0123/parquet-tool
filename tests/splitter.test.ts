import * as path from 'path';
import * as fs from 'fs';
import {
  ParquetWriter,
  ParquetReader,
  Schema,
  splitParquetFile,
} from '../src/lib';

const TMP = path.join(__dirname, '..', 'tmp', 'splitter');

beforeAll(() => {
  if (!fs.existsSync(TMP)) fs.mkdirSync(TMP, { recursive: true });
});
afterAll(() => {
  fs.rmSync(TMP, { recursive: true, force: true });
});

describe('splitParquetFile', () => {
  it('should split a file into smaller chunks', () => {
    const file = path.join(TMP, 'split_source.parquet');
    const schema = Schema.create({ idx: 'INT32', val: 'DOUBLE' });

    const writer = new ParquetWriter(file, schema);
    const N = 25;
    for (let i = 0; i < N; i++) {
      writer.write({ idx: i, val: i * 0.1 });
    }
    writer.close();

    const splitDir = path.join(TMP, 'split_out');
    if (!fs.existsSync(splitDir)) fs.mkdirSync(splitDir, { recursive: true });

    const files = splitParquetFile(file, {
      maxRowsPerFile: 10,
      outputDir: splitDir,
    });

    // 25 rows / 10 per file → 3 files
    expect(files.length).toBe(3);

    // Verify row counts
    let totalRows = 0;
    const allIdx: number[] = [];
    for (const f of files) {
      const reader = ParquetReader.open(f);
      const data = reader.readAll();
      totalRows += data.numRows;
      allIdx.push(...data.columns['idx'].map((value) => value as number));
      reader.close();
    }
    expect(totalRows).toBe(N);
    expect(allIdx).toEqual(Array.from({ length: N }, (_, i) => i));
  });

  it('should produce a single file when rows <= max', () => {
    const file = path.join(TMP, 'split_small.parquet');
    const schema = Schema.create({ v: 'INT32' });
    const writer = new ParquetWriter(file, schema);
    writer.write([{ v: 1 }, { v: 2 }]);
    writer.close();

    const files = splitParquetFile(file, {
      maxRowsPerFile: 100,
      outputDir: TMP,
      prefix: 'split_small',
    });
    expect(files.length).toBe(1);
  });
});
