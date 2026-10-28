// Execute the actual browser module graph with delayed WASM delivery and no Node globals.
import vm from 'node:vm';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

const base = 'https://example.test/parquet-tool/site/';
const sourceRoot = new URL('../../../../site/', import.meta.url);
const messages = [];
const scope = { location: { href: base + 'converter-worker.mjs' }, postMessage: message => messages.push(message) };
let releaseWasm;
const gate = new Promise(resolve => { releaseWasm = resolve; });
let requestedWasm;
const requested = new Promise(resolve => { requestedWasm = resolve; });
const fail = process.argv.includes('--fail-wasm');
const context = vm.createContext({
  console, URL, TextEncoder, TextDecoder, WebAssembly, performance, setTimeout, clearTimeout,
  self: scope, WorkerGlobalScope: class {},
  fetch: async address => {
    assert.equal(String(address), base + 'vendor/project-parquet.wasm');
    requestedWasm();
    await gate;
    if (fail) throw new Error('Simulated unavailable WASM');
    return new Response(await readFile(new URL('vendor/project-parquet.wasm', sourceRoot)), { headers: { 'content-type': 'application/wasm' } });
  },
});
const modules = new Map();
async function load(address) {
  if (modules.has(address)) return modules.get(address);
  const module = new vm.SourceTextModule(await readFile(new URL(address.slice(base.length), sourceRoot), 'utf8'), {
    context, identifier: address,
    initializeImportMeta(meta) { meta.url = address; },
    async importModuleDynamically(name, referencing) {
      const dependency = await load(new URL(name, referencing.identifier).href);
      await dependency.evaluate();
      return dependency;
    },
  });
  modules.set(address, module);
  await module.link((name, referencing) => load(new URL(name, referencing.identifier).href));
  return module;
}
const entry = await load(base + 'converter-worker.mjs');
const evaluation = entry.evaluate();
await new Promise(resolve => setImmediate(resolve));
assert.equal(typeof scope.onmessage, 'function', 'Worker must register its handler before waiting for WASM.');
const original = [{ id: 1, name: '繁體中文・日本語', active: true }, { id: 2, name: null, active: false }];
const loadJob = scope.onmessage({ data: { action: 'load', file: new File([JSON.stringify(original)], 'input.json'), options: {} } });
await requested;
assert.equal(messages.some(message => message.action === 'loaded'), false);
releaseWasm();
await evaluation;
await loadJob;
if (fail) {
  assert.equal(messages.at(-1).action, 'error');
  assert.match(messages.at(-1).message, /引擎載入失敗/);
} else {
  assert.equal(messages.at(-1).action, 'loaded', JSON.stringify(messages));
  await scope.onmessage({ data: { action: 'convert', format: 'parquet', options: { compression: 'none' } } });
  assert.equal(messages.at(-1).action, 'converted', JSON.stringify(messages));
  const bytes = messages.at(-1).bytes;
  await scope.onmessage({ data: { action: 'load', file: new File([bytes], 'back.parquet'), options: {} } });
  assert.equal(messages.at(-1).action, 'loaded', JSON.stringify(messages));
  await scope.onmessage({ data: { action: 'convert', format: 'json', options: {} } });
  assert.deepEqual(JSON.parse(new TextDecoder().decode(messages.at(-1).bytes)), original);
}
console.log(fail ? 'WASM failure reported to the page.' : 'Early load message survived delayed browser WASM initialization and real conversion.');
