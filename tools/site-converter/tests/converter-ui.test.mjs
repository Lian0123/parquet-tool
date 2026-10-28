import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { JSDOM } from 'jsdom';
import esbuild from 'esbuild';
import { loadTable, exportTable, previewTable } from '../../../site/converter-core.mjs';

const pageUrl = 'https://example.github.io/parquet-tool/converter.html';
const html = await readFile(new URL('../../../site/converter.html', import.meta.url), 'utf8');
const i18n = await readFile(new URL('../../../site/i18n.js', import.meta.url), 'utf8');
const script = await esbuild.build({ entryPoints: [new URL('../../../site/converter.mjs', import.meta.url).pathname],
  bundle: true, format: 'iife', write: false, define: { 'import.meta.url': JSON.stringify(pageUrl) } });

function environment(url = pageUrl) {
  const dom = new JSDOM(html, { url, runScripts: 'outside-only' });
  const { window } = dom;
  const workers = [];
  const blobs = new Map();
  const revoked = [];
  class Worker {
    constructor(source, options) {
      assert.equal(String(source), new URL('./converter-worker.mjs', pageUrl).href);
      assert.equal(options.type, 'module');
      workers.push(this);
    }
    async postMessage(data) {
      await new Promise((resolve) => setTimeout(resolve, 2));
      if (this.terminated) return;
      try {
        if (data.action === 'load') {
          this.table = await loadTable(await data.file.arrayBuffer(), data.file.name, data.options);
          this.onmessage({ data: { action: 'loaded', preview: previewTable(this.table) } });
        } else {
          this.onmessage({ data: { action: 'converted', ...exportTable(this.table, data.format, data.options) } });
        }
      } catch (error) { this.onmessage({ data: { action: 'error', message: error.message } }); }
    }
    terminate() { this.terminated = true; }
  }
  window.Worker = Worker;
  window.File = File;
  window.Blob = Blob;
  window.URL.createObjectURL = (blob) => { const url = `blob:test-${blobs.size}`; blobs.set(url, blob); return url; };
  window.URL.revokeObjectURL = (url) => revoked.push(url);
  window.eval(i18n);
  window.eval(script.outputFiles[0].text);
  const element = (id) => window.document.getElementById(id);
  const change = (id, value) => {
    element(id).value = value;
    element(id).dispatchEvent(new window.Event('change'));
  };
  const select = (file) => {
    Object.defineProperty(element('file-input'), 'files', { value: [file], configurable: true });
    element('file-input').dispatchEvent(new window.Event('change'));
  };
  return { dom, window, element, workers, blobs, revoked, change, select };
}

async function until(condition) {
  for (let attempt = 0; attempt < 200; attempt++) {
    if (condition()) return;
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  assert.fail('Timed out waiting for converter state');
}

test('sample → preview → real Parquet download, then options invalidate stale download', async () => {
  const env = environment();
  try {
    const { element } = env;
    assert.equal(element('convert').disabled, true);
    element('sample').click();
    assert.equal(element('cancel').hidden, false);
    await until(() => !element('convert').disabled);
    assert.equal(element('preview-body').children.length, 3);
    element('convert').click();
    assert.equal(element('convert').disabled, true);
    await until(() => !element('result').hidden);
    const url = element('download').href;
    const table = await loadTable(await env.blobs.get(url).arrayBuffer(), 'result.parquet');
    assert.equal(table.rows.length, 3);
    assert.equal(element('download').download, 'example-converted.parquet');
    env.change('output-format', 'csv');
    assert.equal(element('result').hidden, true);
    assert.ok(env.revoked.includes(url));
    assert.equal(element('csv-options').hidden, false);
    element('convert').click();
    await until(() => !element('result').hidden);
    assert.match(await env.blobs.get(element('download').href).text(), /Alice/);
    element('clear').click();
    assert.equal(element('preview').hidden, true);
    assert.equal(element('convert').disabled, true);
    assert.equal(element('download').hasAttribute('href'), false);
  } finally { env.dom.window.close(); }
});

test('cancel terminates worker and ignores late results from replaced files', async () => {
  const env = environment();
  try {
    env.element('sample').click();
    const old = env.workers[0];
    env.element('cancel').click();
    assert.equal(old.terminated, true);
    old.onmessage({ data: { action: 'loaded', preview: { rows: [], schema: [], rowCount: 1, format: 'json' } } });
    assert.equal(env.element('preview').hidden, true);
    assert.equal(env.element('convert').disabled, true);
    env.element('sample').click();
    await until(() => !env.element('convert').disabled);
    assert.equal(env.element('preview-body').children.length, 3);
  } finally { env.dom.window.close(); }
});

test('invalid file disables conversion, selecting a valid file recovers, text stays inert', async () => {
  const env = environment();
  try {
    env.select(new File(['broken'], 'bad.json'));
    await until(() => !env.element('error').hidden);
    assert.match(env.element('error').textContent, /JSON 格式錯誤/);
    assert.equal(env.element('convert').disabled, true);
    const value = '<img src=x onerror=alert(1)>';
    env.select(new File([JSON.stringify([{ [value]: value }])], 'safe.json'));
    await until(() => !env.element('convert').disabled);
    assert.equal(env.element('error').hidden, true);
    assert.equal(env.element('preview').querySelector('img'), null);
    assert.equal(env.element('preview-body').textContent, value);
  } finally { env.dom.window.close(); }
});

test('Parquet input switches output away from Parquet and exports JSON', async () => {
  const env = environment();
  try {
    const table = await loadTable(new TextEncoder().encode('[{"id":1}]').buffer, 'in.json');
    const output = exportTable(table, 'parquet');
    env.select(new File([output.bytes], 'in.parquet'));
    await until(() => !env.element('convert').disabled);
    assert.equal(env.element('output-format').value, 'json');
    assert.equal(env.element('json-options').hidden, false);
    env.element('convert').click();
    await until(() => !env.element('result').hidden);
    assert.deepEqual(JSON.parse(await env.blobs.get(env.element('download').href).text()), [{ id: 1 }]);
  } finally { env.dom.window.close(); }
});

test('CSV option changes reload original file and update preview types', async () => {
  const env = environment();
  try {
    env.select(new File(['id,name\n1,Alice'], 'in.csv'));
    await until(() => !env.element('convert').disabled);
    assert.match(env.element('preview-head').textContent, /INT32/);
    env.element('infer-types').checked = false;
    env.element('infer-types').dispatchEvent(new env.window.Event('change'));
    await until(() => !env.element('convert').disabled);
    assert.doesNotMatch(env.element('preview-head').textContent, /INT32/);
  } finally { env.dom.window.close(); }
});

test('direct file URL explains HTTP requirement and disables input', () => {
  const env = environment('file:///tmp/site/converter.html');
  try {
    assert.equal(env.element('environment-error').hidden, false);
    assert.equal(env.element('sample').disabled, true);
    assert.equal(env.element('file-input').disabled, true);
  } finally { env.dom.window.close(); }
});

test('pagehide clears worker and ready state for a back/forward cache restore', async () => {
  const env = environment();
  try {
    env.element('sample').click();
    await until(() => !env.element('convert').disabled);
    env.window.dispatchEvent(new env.window.Event('pagehide'));
    assert.equal(env.workers[0].terminated, true);
    assert.equal(env.element('convert').disabled, true);
    assert.equal(env.element('preview').hidden, true);
  } finally { env.dom.window.close(); }
});
