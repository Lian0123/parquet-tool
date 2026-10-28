import { spawnSync } from 'node:child_process';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');
const output = path.join(root, 'site/vendor/project-parquet.mjs');
await mkdir(path.dirname(output), { recursive: true });
const compiler = process.env.EMCC || 'em++';
const result = spawnSync(compiler, [path.join(root, 'src/wasm/browser.cpp'), '-std=c++17', '-O2', '--bind',
  '-fexceptions', '-sDISABLE_EXCEPTION_CATCHING=0', '-sMODULARIZE=1', '-sEXPORT_ES6=1',
  '-sEXPORT_NAME=createParquetModule', '-sENVIRONMENT=web,worker,node', '-sALLOW_MEMORY_GROWTH=1',
  '-sMAXIMUM_MEMORY=268435456', '-sWASM_BIGINT=1', '-sFORCE_FILESYSTEM=1',
  '-sEXPORTED_RUNTIME_METHODS=["FS"]', '-o', output], { stdio: 'inherit' });
if (result.status !== 0) throw new Error('WebAssembly build failed. Install Emscripten 3.1.74 and set EMCC to em++.');
const files = ['src/wasm/browser.cpp', 'src/native/parquet.h', 'src/native/thrift.h', 'tools/site-converter/build-wasm.mjs'];
const sources = {};
for (const file of files) sources[file] = createHash('sha256').update(await readFile(path.join(root, file))).digest('hex');
await writeFile(path.join(root, 'site/vendor/project-parquet.sources.json'), JSON.stringify({ compiler: 'emscripten 3.1.74', sources }, null, 2) + '\n');
console.log('Compiled the project C++ Parquet engine to WebAssembly.');
