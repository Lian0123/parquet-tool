import { Papa, parquetMetadata, parquetReadObjects, parquetWriteBuffer, compressors } from './vendor/converter-deps.mjs';
import { LIMITS, detectFormat } from './converter-shared.mjs';
export { LIMITS, detectFormat } from './converter-shared.mjs';
const forbiddenNames = new Set(['__proto__', 'prototype', 'constructor']);

function checkShape(rowCount, columnCount) {
  if (rowCount > LIMITS.rows) throw new Error('資料超過 100,000 列，請先切分檔案。');
  if (!columnCount || columnCount > LIMITS.columns) throw new Error('資料必須包含 1 至 200 個欄位。');
  if (rowCount * columnCount > LIMITS.cells) throw new Error('資料超過 2,000,000 個儲存格，請先切分檔案。');
}

function checkNames(names) {
  if (names.some((name) => !name.trim() || forbiddenNames.has(name))) {
    throw new Error('欄位名稱不能空白，也不能使用 __proto__、prototype 或 constructor。');
  }
  if (new Set(names).size !== names.length) throw new Error('欄位名稱重複，請先修正 CSV 標題列。');
}

export function inferType(values) {
  const present = values.filter((value) => value !== null);
  if (!present.length) return 'STRING';
  if (present.every((value) => typeof value === 'boolean')) return 'BOOLEAN';
  if (present.every((value) => typeof value === 'bigint' || (typeof value === 'number' && Number.isSafeInteger(value))) &&
      present.some((value) => typeof value === 'bigint')) return 'INT64';
  if (present.every((value) => typeof value === 'number')) {
    return present.every((value) => Number.isInteger(value) && value >= -2147483648 && value <= 2147483647) ? 'INT32' : 'DOUBLE';
  }
  return 'STRING';
}

function scalar(value, column) {
  if (value === null || value === undefined) return null;
  if (value instanceof Date) {
    if (!Number.isFinite(value.getTime())) throw new Error(`欄位「${column}」包含無效日期。`);
    return value.toISOString();
  }
  if (!['string', 'number', 'boolean', 'bigint'].includes(typeof value)) {
    throw new Error(`欄位「${column}」包含巢狀物件、陣列或二進位資料。目前僅支援平面資料表。`);
  }
  if (typeof value === 'number' && (!Number.isFinite(value) || (Number.isInteger(value) && !Number.isSafeInteger(value)))) {
    throw new Error(`欄位「${column}」的數值超出 JavaScript 安全精度。請改用帶引號的十進位字串。`);
  }
  if (typeof value === 'bigint' && (value < -(2n ** 63n) || value > 2n ** 63n - 1n)) {
    throw new Error(`欄位「${column}」超出 INT64 範圍。`);
  }
  return value;
}

function tableFromRows(rows, names) {
  if (!Array.isArray(rows) || rows.length === 0) throw new Error('找不到資料列。請提供至少一筆資料。');
  checkShape(rows.length, 1);
  if (rows.some((row) => !row || typeof row !== 'object' || Array.isArray(row))) {
    throw new Error('JSON 必須是物件陣列，例如 [{"id":1,"name":"Alice"}]。');
  }
  const columns = names || [...new Set(rows.flatMap((row) => Object.keys(row)))];
  checkShape(rows.length, columns.length);
  checkNames(columns);
  const normalized = rows.map((row) => Object.fromEntries(columns.map((name) => [name, scalar(Object.hasOwn(row, name) ? row[name] : null, name)])));
  const schema = columns.map((name) => {
    const values = normalized.map((row) => row[name]);
    const type = inferType(values);
    // A Parquet column has one type. Preserve heterogeneous scalar values as text.
    if (type === 'STRING') for (const row of normalized) if (row[name] !== null) row[name] = String(row[name]);
    if (type === 'INT64') for (const row of normalized) if (row[name] !== null) row[name] = BigInt(row[name]);
    return { name, type, nulls: values.filter((value) => value === null).length };
  });
  return { rows: normalized, schema };
}

function parseCsvValue(value, infer) {
  if (value === '') return null;
  if (!infer) return value;
  if (value === 'true') return true;
  if (value === 'false') return false;
  if (/^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?$/.test(value)) {
    const number = Number(value);
    if (Number.isFinite(number) && (!Number.isInteger(number) || Number.isSafeInteger(number))) return number;
  }
  return value;
}

export async function loadTable(buffer, filename, options = {}) {
  if (buffer.byteLength > LIMITS.bytes) throw new Error('檔案超過 20 MiB，請先切分後再轉換。');
  if (!buffer.byteLength) throw new Error('檔案是空的。');
  const format = detectFormat(filename);
  let table;
  if (format === 'parquet') {
    let metadata;
    try { metadata = parquetMetadata(buffer); } catch { throw new Error('無法讀取 Parquet：檔案可能已損毀或不是 Parquet 格式。'); }
    const names = metadata.schema.slice(1).filter((item) => item.num_children === undefined).map((item) => item.name);
    checkShape(Number(metadata.num_rows), names.length);
    checkNames(names);
    if (metadata.schema.slice(1).some((item) => item.num_children !== undefined)) throw new Error('目前僅支援平面 Parquet 資料表，無法轉換巢狀欄位。');
    const rows = await parquetReadObjects({ file: buffer, metadata, compressors });
    table = tableFromRows(rows, names);
  } else {
    let text;
    try { text = new TextDecoder('utf-8', { fatal: true }).decode(buffer).replace(/^\uFEFF/, ''); }
    catch { throw new Error('文字編碼無法辨識，請先將檔案另存為 UTF-8。'); }
    if (format === 'json') {
      let parsed;
      try { parsed = JSON.parse(text); } catch { throw new Error('JSON 格式錯誤，請檢查逗號、引號與括號。'); }
      table = tableFromRows(Array.isArray(parsed) ? parsed : parsed?.rows);
    } else {
      // Parse arrays first so duplicate headers cannot be silently renamed by a CSV parser.
      const parsed = Papa.parse(text, { delimiter: options.delimiter || '', skipEmptyLines: 'greedy', preview: LIMITS.rows + 2 });
      const errors = parsed.errors.filter((error) => error.code !== 'UndetectableDelimiter');
      if (errors.length) throw new Error(`CSV 格式錯誤：${errors[0].message}`);
      const [names, ...records] = parsed.data;
      if (!names) throw new Error('CSV 沒有標題列。');
      checkShape(records.length, names.length);
      checkNames(names);
      if (records.some((row) => row.length !== names.length)) throw new Error('CSV 每一列的欄位數必須與標題列一致，請確認分隔符號。');
      const rows = records.map((record) => Object.fromEntries(names.map((name, index) => [name, parseCsvValue(record[index], options.infer !== false)])));
      table = tableFromRows(rows, names);
    }
  }
  return { ...table, format };
}

export function previewTable(table) {
  return { format: table.format, rowCount: table.rows.length, schema: table.schema,
    rows: table.rows.slice(0, 10).map((row) => Object.fromEntries(Object.entries(row).map(([key, value]) =>
      [key, typeof value === 'bigint' ? value.toString() : value]))) };
}

export function exportTable(table, format, options = {}) {
  let bytes;
  const mime = { parquet: 'application/vnd.apache.parquet', json: 'application/json', csv: 'text/csv;charset=utf-8' }[format];
  if (!mime) throw new Error('不支援這個輸出格式。');
  if (format === 'parquet') {
    const buffer = parquetWriteBuffer({
      columnData: table.schema.map(({ name, type }) => ({ name, type, nullable: true, data: table.rows.map((row) => row[name]) })),
      codec: options.compression === 'none' ? 'UNCOMPRESSED' : 'SNAPPY',
      rowGroupSize: 10000,
      createdBy: 'parquet-tool browser converter (hyparquet-writer)',
    });
    bytes = new Uint8Array(buffer);
  } else if (format === 'json') {
    bytes = new TextEncoder().encode(JSON.stringify(table.rows, (_key, value) => typeof value === 'bigint' ? value.toString() : value, 2));
  } else {
    const text = Papa.unparse({ fields: table.schema.map((column) => column.name),
      data: table.rows.map((row) => table.schema.map(({ name }) => typeof row[name] === 'bigint' ? row[name].toString() : row[name])) },
    { delimiter: options.delimiter || ',', escapeFormulae: options.escapeFormulae !== false });
    bytes = new TextEncoder().encode('\uFEFF' + text);
  }
  return { bytes, mime, extension: format };
}
