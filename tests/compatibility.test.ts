import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { execFileSync } from 'child_process';
import { ParquetReader, ParquetWriter, Schema } from '../src/lib';

const fixture = (name: string) => path.join(__dirname, 'fixtures', name + '.parquet');
const standard = [{ id: 1, name: '標準', active: true }, { id: 2, name: null, active: false }];

it('reads standard independent PyArrow PLAIN data and legacy project schema data', () => {
  expect(ParquetReader.withReader(fixture('standard-plain'), r => [...r])).toEqual(standard);
  expect(ParquetReader.withReader(fixture('legacy-schema'), r => [...r])).toEqual([
    { id: 1, name: 'legacy' }, { id: 2, name: null },
  ]);
});

it.each(['standard-snappy', 'standard-dictionary'])('rejects unsupported encoding: %s', name => {
  expect(() => ParquetReader.withReader(fixture(name), r => r.readAll())).toThrow(/uncompressed|PLAIN/);
});

it('exchanges Unicode, nulls and booleans between native and project WASM engines', () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'parquet-interop-'));
  try {
    const nativeFile = path.join(directory, 'native.parquet');
    const wasmFile = path.join(directory, 'wasm.parquet');
    ParquetWriter.withWriter(nativeFile, Schema.create({
      id: 'INT32', name: { type: 'STRING', optional: true }, active: 'BOOLEAN',
    }), writer => writer.write(standard));
    execFileSync(process.execPath, ['--input-type=module', '-e', `
      import { readFileSync, writeFileSync } from 'node:fs';
      import assert from 'node:assert/strict';
      import { readProjectParquet, writeProjectParquet } from './site/project-engine.mjs';
      const bytes = readFileSync(process.argv[1]);
      assert.deepEqual(readProjectParquet(bytes), ${JSON.stringify(standard)});
      writeFileSync(process.argv[2], writeProjectParquet([
        { name: 'id', type: 'INT32', data: [1, 2] },
        { name: 'name', type: 'STRING', data: ['標準', null] },
        { name: 'active', type: 'BOOLEAN', data: [true, false] },
      ]));
    `, nativeFile, wasmFile], { cwd: path.join(__dirname, '..'), stdio: 'pipe' });
    expect(ParquetReader.withReader(wasmFile, r => [...r])).toEqual(standard);
  } finally { fs.rmSync(directory, { recursive: true, force: true }); }
});
