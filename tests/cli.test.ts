import * as path from 'path';
import * as fs from 'fs';
import { execSync } from 'child_process';

const TMP = path.join(__dirname, '..', 'tmp', 'cli');
const CLI = path.join(__dirname, '..', 'dist', 'cli', 'index.js');

function cli(args: string): string {
  return execSync(`node "${CLI}" ${args}`, {
    cwd: path.join(__dirname, '..'),
    encoding: 'utf-8',
    timeout: 15000,
  });
}

beforeAll(() => {
  if (!fs.existsSync(TMP)) fs.mkdirSync(TMP, { recursive: true });
});
afterAll(() => {
  fs.rmSync(TMP, { recursive: true, force: true });
});

describe('CLI', () => {
  const dataFile = path.join(TMP, 'cli_test.parquet');
  const jsonInput = path.join(TMP, 'cli_input.json');

  beforeAll(() => {
    // Prepare JSON input
    const rows = [
      { id: 1, name: 'Alice', score: 95.5 },
      { id: 2, name: 'Bob', score: 87.3 },
      { id: 3, name: 'Charlie', score: 91.0 },
    ];
    fs.writeFileSync(jsonInput, JSON.stringify(rows));
  });

  it('should write a parquet file from JSON', () => {
    const output = cli(
      `write "${dataFile}" -i "${jsonInput}" -s "id:INT32,name:STRING,score:DOUBLE"`,
    );
    expect(output).toContain('Wrote 3 rows');
    expect(fs.existsSync(dataFile)).toBe(true);
  });

  it('should show info about a parquet file', () => {
    const output = cli(`info "${dataFile}"`);
    expect(output).toContain('Rows');
    expect(output).toContain('3');
    expect(output).toContain('id');
    expect(output).toContain('INT32');
  });

  it('should read rows as JSON', () => {
    const output = cli(`read "${dataFile}" --json`);
    const rows = JSON.parse(output);
    expect(rows).toHaveLength(3);
    expect(rows[0].id).toBe(1);
    expect(rows[0].name).toBe('Alice');
  });

  it('should read rows with limit', () => {
    const output = cli(`read "${dataFile}" -n 2`);
    const lines = output.trim().split('\n');
    // header + separator + 2 data lines + "... more" line
    expect(lines.length).toBeGreaterThanOrEqual(4);
  });

  it('should append rows', () => {
    const appendJson = path.join(TMP, 'cli_append.json');
    fs.writeFileSync(
      appendJson,
      JSON.stringify([{ id: 4, name: 'Diana', score: 88.0 }]),
    );
    const output = cli(`append "${dataFile}" -i "${appendJson}"`);
    expect(output).toContain('Appended 1 rows');

    // Verify
    const info = cli(`info "${dataFile}"`);
    expect(info).toContain('4');
  });

  it('should split a file', () => {
    const splitDir = path.join(TMP, 'cli_split');
    if (!fs.existsSync(splitDir)) fs.mkdirSync(splitDir, { recursive: true });
    const output = cli(
      `split "${dataFile}" -n 2 -o "${splitDir}" -p "part"`,
    );
    expect(output).toContain('Split into');
  });

  it('should merge files', () => {
    const f1 = path.join(TMP, 'merge1.parquet');
    const f2 = path.join(TMP, 'merge2.parquet');
    const merged = path.join(TMP, 'merged.parquet');

    const j1 = path.join(TMP, 'merge1.json');
    const j2 = path.join(TMP, 'merge2.json');
    fs.writeFileSync(j1, JSON.stringify([{ v: 1 }, { v: 2 }]));
    fs.writeFileSync(j2, JSON.stringify([{ v: 3 }]));

    cli(`write "${f1}" -i "${j1}" -s "v:INT32"`);
    cli(`write "${f2}" -i "${j2}" -s "v:INT32"`);

    const output = cli(`merge "${merged}" "${f1}" "${f2}"`);
    expect(output).toContain('Merged 2 files');

    const info = cli(`info "${merged}"`);
    expect(info).toContain('3');
  });
});
