import * as fs from 'fs';
import * as path from 'path';
import { tableFromArrays, tableToIPC } from 'apache-arrow';
import {
  arrowToParquet,
  configureDebugMode,
  csvToParquet,
  isDebugEnabled,
  jsonToParquet,
  mergeParquetFiles,
  parquetToArrow,
  parquetToCsv,
  parquetToJson,
  parquetToXml,
  ParquetReader,
  ParquetWriter,
  Schema,
  setDebugMode,
  validateParquetFile,
  xmlToParquet,
} from '../src/lib';

const TMP = path.join(__dirname, '..', 'tmp', 'features');

beforeAll(() => {
  if (!fs.existsSync(TMP)) fs.mkdirSync(TMP, { recursive: true });
});

afterAll(() => {
  fs.rmSync(TMP, { recursive: true, force: true });
});

describe('mergeParquetFiles', () => {
  it('should merge multiple parquet files via library API', () => {
    const schema = Schema.create({ id: 'INT32', name: 'STRING' });
    const left = path.join(TMP, 'merge_left.parquet');
    const right = path.join(TMP, 'merge_right.parquet');
    const output = path.join(TMP, 'merge_output.parquet');

    const writer1 = new ParquetWriter(left, schema);
    writer1.write([{ id: 1, name: 'a' }]);
    writer1.close();

    const writer2 = new ParquetWriter(right, schema);
    writer2.write([{ id: 2, name: 'b' }]);
    writer2.close();

    mergeParquetFiles([left, right], output);

    const reader = ParquetReader.open(output);
    const data = reader.readAll();
    expect(data.columns['id']).toEqual([1, 2]);
    expect(data.columns['name']).toEqual(['a', 'b']);
    reader.close();
  });
});

describe('validateParquetFile', () => {
  it('should validate a correct parquet file', () => {
    const schema = Schema.create({ value: 'INT32' });
    const file = path.join(TMP, 'validate_ok.parquet');
    const writer = new ParquetWriter(file, schema);
    writer.write([{ value: 1 }, { value: 2 }]);
    writer.close();

    const result = validateParquetFile(file);
    expect(result.valid).toBe(true);
    expect(result.issues).toHaveLength(0);
  });

  it('should report invalid files', () => {
    const file = path.join(TMP, 'validate_bad.parquet');
    fs.writeFileSync(file, 'not parquet', 'utf8');
    const result = validateParquetFile(file);
    expect(result.valid).toBe(false);
    expect(result.issues[0]?.code).toBe('READ_FAILURE');
  });
});

describe('CSV conversion', () => {
  it('should convert CSV to Parquet and back', () => {
    const csvPath = path.join(TMP, 'data.csv');
    const parquetPath = path.join(TMP, 'data.parquet');
    const csvOut = path.join(TMP, 'data_out.csv');

    fs.writeFileSync(csvPath, 'id,name,score\n1,Alice,95.5\n2,Bob,87\n', 'utf8');
    csvToParquet(csvPath, parquetPath);
    parquetToCsv(parquetPath, csvOut);

    const reader = ParquetReader.open(parquetPath);
    const data = reader.readAll();
    expect(data.columns['id']).toEqual([1, 2]);
    expect(data.columns['name']).toEqual(['Alice', 'Bob']);
    reader.close();

    const csvOutput = fs.readFileSync(csvOut, 'utf8');
    expect(csvOutput).toContain('Alice');
    expect(csvOutput).toContain('Bob');
  });
});

describe('Arrow conversion', () => {
  it('should convert between Arrow and Parquet', () => {
    const arrowPath = path.join(TMP, 'data.arrow');
    const parquetPath = path.join(TMP, 'data_arrow.parquet');
    const arrowOut = path.join(TMP, 'data_roundtrip.arrow');

    const table = tableFromArrays({
      id: [1, 2],
      name: ['Alice', 'Bob'],
      score: [95.5, 88.25],
    });
    fs.writeFileSync(arrowPath, Buffer.from(tableToIPC(table)));

    arrowToParquet(arrowPath, parquetPath);
    parquetToArrow(parquetPath, arrowOut);

    expect(fs.existsSync(parquetPath)).toBe(true);
    expect(fs.existsSync(arrowOut)).toBe(true);

    const reader = ParquetReader.open(parquetPath);
    const data = reader.readAll();
    expect(data.columns['id']).toEqual([1, 2]);
    expect(data.columns['name']).toEqual(['Alice', 'Bob']);
    reader.close();
  });
});

describe('JSON conversion', () => {
  it('should convert between JSON and Parquet with schema preservation', () => {
    const parquetPath = path.join(TMP, 'data_json.parquet');
    const jsonPath = path.join(TMP, 'data.json');
    const roundtripPath = path.join(TMP, 'data_json_roundtrip.parquet');

    const schema = Schema.create({
      id: 'INT32',
      name: 'STRING',
      visits: 'INT64',
      active: { type: 'BOOLEAN', optional: true },
    });
    const writer = new ParquetWriter(parquetPath, schema);
    writer.write([
      { id: 1, name: 'Alice', visits: BigInt(9007199254740991), active: true },
      { id: 2, name: 'Bob', visits: BigInt(42), active: null },
    ]);
    writer.close();

    parquetToJson(parquetPath, jsonPath);
    jsonToParquet(jsonPath, roundtripPath);

    const reader = ParquetReader.open(roundtripPath);
    const data = reader.readRows();
    expect(data).toEqual([
      { id: 1, name: 'Alice', visits: BigInt(9007199254740991), active: true },
      { id: 2, name: 'Bob', visits: BigInt(42), active: null },
    ]);
    reader.close();

    const json = JSON.parse(fs.readFileSync(jsonPath, 'utf8')) as { rows: Array<Record<string, unknown>> };
    expect(json.rows[0]?.visits).toBe('9007199254740991');
  });
});

describe('XML conversion', () => {
  it('should convert between XML and Parquet with schema preservation', () => {
    const parquetPath = path.join(TMP, 'data_xml.parquet');
    const xmlPath = path.join(TMP, 'data.xml');
    const roundtripPath = path.join(TMP, 'data_xml_roundtrip.parquet');

    const schema = Schema.create({
      id: 'INT32',
      name: 'STRING',
      score: { type: 'DOUBLE', optional: true },
      active: { type: 'BOOLEAN', optional: true },
    });
    const writer = new ParquetWriter(parquetPath, schema);
    writer.write([
      { id: 1, name: 'Alice', score: 95.5, active: true },
      { id: 2, name: 'Bob', score: null, active: false },
    ]);
    writer.close();

    parquetToXml(parquetPath, xmlPath);
    xmlToParquet(xmlPath, roundtripPath);

    const reader = ParquetReader.open(roundtripPath);
    const data = reader.readRows();
    expect(data).toEqual([
      { id: 1, name: 'Alice', score: 95.5, active: true },
      { id: 2, name: 'Bob', score: null, active: false },
    ]);
    reader.close();

    const xml = fs.readFileSync(xmlPath, 'utf8');
    expect(xml).toContain('<schema>');
    expect(xml).toContain('type="INT32"');
  });
});

describe('debug mode', () => {
  it('should enable debug mode programmatically', () => {
    const messages: string[] = [];
    configureDebugMode({
      enabled: true,
      logger: (message) => {
        messages.push(message);
      },
    });

    expect(isDebugEnabled()).toBe(true);
    setDebugMode(false);
    expect(isDebugEnabled()).toBe(false);
    expect(messages).toEqual([]);
  });
});