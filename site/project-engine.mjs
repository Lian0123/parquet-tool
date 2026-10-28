import createParquetModule from './vendor/project-parquet.mjs';
const options = {};
if (typeof process === 'object' && process.versions?.node) {
  const { readFile } = await import('node:fs/promises');
  options.wasmBinary = await readFile(new URL('./vendor/project-parquet.wasm', import.meta.url));
}
const engine = await createParquetModule(options);

export function readProjectParquet(bytes) {
  engine.FS.writeFile('/input.parquet', new Uint8Array(bytes));
  try {
    const result = engine.readRows();
    if (result.error) throw new Error(result.error);
    return result.rows;
  } finally { engine.FS.unlink('/input.parquet'); }
}

export function writeProjectParquet(columns) {
  try {
    const result = engine.writeColumns(columns);
    if (result.error) throw new Error(result.error);
    return engine.FS.readFile('/output.parquet');
  } finally {
    try { engine.FS.unlink('/output.parquet'); } catch { /* writer may have failed before creating the file */ }
  }
}
