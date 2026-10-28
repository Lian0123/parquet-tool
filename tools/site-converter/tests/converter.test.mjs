import test from 'node:test';
import assert from 'node:assert/strict';
import { loadTable, exportTable, previewTable, LIMITS } from '../../../site/converter-core.mjs';
import { Papa } from '../../../site/vendor/converter-deps.mjs';
import { writeProjectParquet } from '../../../site/project-engine.mjs';

const encode = (text) => new TextEncoder().encode(text).buffer;
const json = (rows) => loadTable(encode(JSON.stringify(rows)), 'data.json');
const readOutput = (output) => new TextDecoder().decode(output.bytes);
const baseRows = [{ id: 1, name: '水木, Hana\n"Hello"', active: true, score: 1.5 }, { id: 2, name: null, active: false, score: null }];

for (const compression of ['none']) test(`JSON → project C++ ${compression} Parquet → CSV/JSON preserves scalar data`, async () => {
  const source = await json(baseRows);
  const output = exportTable(source, 'parquet', { compression });
  assert.equal(new TextDecoder().decode(output.bytes.slice(0, 4)), 'PAR1');
  const back = await loadTable(output.bytes.buffer, 'back.parquet');
  assert.deepEqual(back.rows, baseRows);
  assert.deepEqual(JSON.parse(readOutput(exportTable(back, 'json'))), baseRows);
  const csv = exportTable(back, 'csv');
  assert.deepEqual([...csv.bytes.slice(0, 3)], [239, 187, 191]);
  const fromCsv = await loadTable(csv.bytes.buffer, 'back.csv');
  assert.deepEqual(fromCsv.rows, baseRows);
});

test('CSV → JSON/Parquet handles quotes, newlines, BOM, nulls, leading zeros and unsafe integers', async () => {
  const source = '\uFEFFid;name;active;large;missing\r\n001;"林,\n\"\"小明\"\"";true;9007199254740993;\r\n002;Bob;false;9007199254740994;';
  const table = await loadTable(encode(source), 'data.CSV', { delimiter: ';' });
  assert.equal(table.rows[0].id, '001');
  assert.equal(table.rows[0].name, '林,\n"小明"');
  assert.equal(table.rows[0].large, '9007199254740993');
  assert.equal(table.rows[0].active, true);
  assert.equal(table.rows[0].missing, null);
  const output = exportTable(table, 'parquet');
  assert.deepEqual((await loadTable(output.bytes.buffer, 'back.parquet')).rows, table.rows);
  assert.deepEqual(JSON.parse(readOutput(exportTable(table, 'json'))), table.rows);
});

test('CSV inference can be disabled and tab delimiter detected', async () => {
  const table = await loadTable(encode('id\tactive\n123\ttrue'), 'data.csv', { infer: false });
  assert.deepEqual(table.rows, [{ id: '123', active: 'true' }]);
});

test('CSV exports optionally escape spreadsheet formula text', async () => {
  const table = await json([{ name: '=1+1', other: '@SUM(A1)', number: -2 }]);
  const safe = Papa.parse(readOutput(exportTable(table, 'csv')), { header: true }).data[0];
  assert.equal(safe.name, "'=1+1");
  assert.equal(safe.other, "'@SUM(A1)");
  assert.equal(safe.number, '-2');
  const raw = Papa.parse(readOutput(exportTable(table, 'csv', { escapeFormulae: false })), { header: true }).data[0];
  assert.equal(raw.name, '=1+1');
});

test('INT64 exports preserve exact digits as strings in JSON and preview', async () => {
  const buffer = writeProjectParquet([{ name: 'id', type: 'INT64', data: [9007199254740993n, null] }]);
  const table = await loadTable(buffer.buffer, 'int64.parquet');
  assert.equal(table.schema[0].type, 'INT64');
  assert.deepEqual(JSON.parse(readOutput(exportTable(table, 'json'))), [{ id: '9007199254740993' }, { id: null }]);
  assert.equal(previewTable(table).rows[0].id, '9007199254740993');
});

test('sparse JSON fields, all-null columns, mixed scalars and row wrapper', async () => {
  const table = await json({ rows: [{ a: 1, empty: null }, { b: true, a: 'two', empty: null }] });
  assert.deepEqual(table.rows, [{ a: '1', empty: null, b: null }, { a: 'two', empty: null, b: true }]);
  assert.deepEqual((await loadTable(exportTable(table, 'parquet').bytes.buffer, 'back.parquet')).rows, table.rows);
});

test('preview contains only ten rows, output retains every row and column', async () => {
  const table = await json(Array.from({ length: 25 }, (_, id) => ({ id, name: `User ${id}` })));
  assert.equal(previewTable(table).rows.length, 10);
  assert.equal(JSON.parse(readOutput(exportTable(table, 'json'))).length, 25);
});

for (const [label, text, filename, pattern] of [
  ['invalid JSON', '[{broken}]', 'data.json', /JSON 格式錯誤/],
  ['invalid Parquet', 'not parquet', 'data.parquet', /invalid parquet/],
  ['unsupported extension', 'test', 'data.xml', /請選擇/],
  ['empty input', '', 'data.csv', /空的/],
  ['no records', '[]', 'data.json', /找不到資料列/],
  ['scalar rows', '[1,2]', 'data.json', /物件陣列/],
  ['nested data', '[{"a":{"b":1}}]', 'data.json', /巢狀/],
  ['array field', '[{"a":[1]}]', 'data.json', /巢狀/],
  ['unsafe JSON integer', '[{"id":9007199254740993}]', 'data.json', /安全精度/],
  ['duplicate CSV header', 'id,id\n1,2', 'data.csv', /重複/],
  ['empty CSV header', 'id,\n1,2', 'data.csv', /空白/],
  ['ragged CSV', 'id,name\n1,Alice,extra', 'data.csv', /欄位數/],
  ['reserved name', '[{"__proto__":"x"}]', 'data.json', /欄位名稱/],
]) test(`rejects ${label} with actionable error`, async () => {
  await assert.rejects(loadTable(encode(text), filename), pattern);
});

test('rejects invalid UTF-8, files/row counts/column counts/cell counts beyond limits', async () => {
  await assert.rejects(loadTable(new Uint8Array([0xff, 0xff]).buffer, 'data.csv'), /UTF-8/);
  await assert.rejects(loadTable(new ArrayBuffer(LIMITS.bytes + 1), 'large.csv'), /20 MiB/);
  await assert.rejects(json(Array.from({ length: LIMITS.rows + 1 }, () => ({ id: 1 }))), /100,000/);
  await assert.rejects(json([Object.fromEntries(Array.from({ length: 201 }, (_, n) => [`c${n}`, n]))]), /200 個欄位/);
  await assert.rejects(json(Array.from({ length: 10001 }, () => Object.fromEntries(Array.from({ length: 200 }, (_, n) => [`c${n}`, 0])))), /2,000,000/);
});

test('project writer rejects oversized tables and unsupported compression', async () => {
  assert.throws(() => writeProjectParquet([{ name: 'id', type: 'INT32', data: Array(LIMITS.rows + 1).fill(1) }]), /limit/);
  const table = await json([{ id: 1 }]);
  assert.throws(() => exportTable(table, 'parquet', { compression: 'snappy' }), /uncompressed/);
});

test('project WASM reads independent standard and legacy fixtures and rejects unsupported encodings', async () => {
  const { readFile } = await import('node:fs/promises');
  const fixture = async (name) => {
    const bytes = await readFile(new URL(`../../../tests/fixtures/${name}.parquet`, import.meta.url));
    return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
  };
  assert.deepEqual((await loadTable(await fixture('standard-plain'), 'test.parquet')).rows,
    [{ id: 1, name: '標準', active: true }, { id: 2, name: null, active: false }]);
  assert.deepEqual((await loadTable(await fixture('legacy-schema'), 'test.parquet')).rows,
    [{ id: 1, name: 'legacy' }, { id: 2, name: null }]);
  for (const name of ['standard-snappy', 'standard-dictionary']) {
    await assert.rejects(loadTable(await fixture(name), 'test.parquet'), /uncompressed|PLAIN/);
  }
});
