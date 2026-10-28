import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';
import { JSDOM } from 'jsdom';
import { pagesMode } from '../pages-mode.mjs';
import { entryFiles, generateBranchEntries } from '../branch-entrypoints.mjs';
import { verifyDeployment } from '../verify-deployment.mjs';

const root = new URL('../../../', import.meta.url);
test('Pages mode supports both Actions and main root branch publishing', () => {
  assert.equal(pagesMode({ build_type: 'workflow' }), 'workflow');
  assert.equal(pagesMode({ build_type: 'legacy', source: { branch: 'main', path: '/' } }), 'branch');
  assert.equal(pagesMode({ source: { branch: 'main', path: '/' } }), 'branch');
  for (const config of [{}, { build_type: 'legacy', source: { branch: 'main', path: '/docs' } }, { build_type: 'legacy', source: { branch: 'other', path: '/' } }]) {
    assert.throws(() => pagesMode(config), /Unsupported Pages source/);
  }
});

test('all branch entry points reach real translated pages and retain query and recipe anchors', async () => {
  await generateBranchEntries({ check: true });
  for (const file of entryFiles) {
    const dom = new JSDOM(await readFile(new URL(file, root), 'utf8'));
    try {
      const document = dom.window.document;
      const target = document.querySelector('a').getAttribute('href');
      const url = new URL(target, new URL(file, root));
      assert.equal(url.href, new URL('site/' + file, root).href);
      const real = new JSDOM(await readFile(url, 'utf8'));
      assert.equal(real.window.document.documentElement.lang, document.documentElement.lang);
      real.window.close();
      let destination;
      vm.runInNewContext(document.querySelector('script').textContent, { location: { search: '?q=csv', hash: '#example-basic', replace: value => { destination = value; } } });
      assert.equal(destination, target + '?q=csv#example-basic');
      assert.equal(document.querySelector('meta[http-equiv="refresh"]').content, '0; url=' + target);
    } finally { dom.window.close(); }
  }
});

function publishedFiles(mode, mutation) {
  return async url => {
    const pathname = new URL(url).pathname.replace('/parquet-tool/', '') || 'index.html';
    const file = new URL((mode === 'workflow' ? 'site/' : '') + pathname, root);
    let bytes;
    try { bytes = await readFile(file); } catch { return new Response('missing', { status: 404 }); }
    const contentType = pathname.endsWith('.wasm') ? 'application/wasm' : /\.m?js$/.test(pathname) ? 'application/javascript' : pathname.endsWith('.html') ? 'text/html' : 'text/plain';
    return mutation?.(pathname, bytes, contentType) || new Response(bytes, { headers: { 'content-type': contentType } });
  };
}
for (const mode of ['workflow', 'branch']) test(`deployment verification follows ${mode} publishing and checks real engine bytes`, async () => {
  assert.match(await verifyDeployment('https://example.test/parquet-tool/', { fetcher: publishedFiles(mode) }), /nine deployed pages/);
});
test('deployment verification rejects stale pages, missing assets and HTML masquerading as WASM', async () => {
  for (const [mutation, pattern] of [
    [(file) => file === 'guide.html' ? new Response('<html>old README</html>', { headers: { 'content-type': 'text/html' } }) : null, /does not match/],
    [(file) => file === 'app.js' ? new Response('missing', { status: 404 }) : null, /HTTP 404/],
    [(file) => file.endsWith('.wasm') ? new Response('<html>wrong</html>', { headers: { 'content-type': 'text/html' } }) : null, /WASM MIME/],
    [(file) => file === 'converter-worker.mjs' ? new Response('worker', { headers: { 'content-type': 'text/plain' } }) : null, /JavaScript MIME/],
    [(file, bytes, type) => file === 'project-engine.mjs' ? new Response('old engine', { headers: { 'content-type': type } }) : null, /incorrect deployed asset/],
  ]) await assert.rejects(verifyDeployment('https://example.test/parquet-tool/', { fetcher: publishedFiles('workflow', mutation) }), pattern);
});

test('deployment verification checks the actual directory URL and HTML MIME', async () => {
  const published = publishedFiles('workflow');
  await assert.rejects(verifyDeployment('https://example.test/parquet-tool/', {
    fetcher: url => new URL(url).pathname === '/parquet-tool/'
      ? new Response('<html>old repository README</html>', { headers: { 'content-type': 'text/html' } })
      : published(url),
  }), /directory URL/);
  await assert.rejects(verifyDeployment('https://example.test/parquet-tool/', {
    fetcher: publishedFiles('workflow', (file, bytes) => file === 'guide.html'
      ? new Response(bytes, { headers: { 'content-type': 'text/plain' } }) : null),
  }), /HTML MIME/);
});
