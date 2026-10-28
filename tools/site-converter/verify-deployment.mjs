import { readFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { pathToFileURL } from 'node:url';
import { JSDOM } from 'jsdom';

const files = ['', 'en/', 'ja/'].flatMap(language => ['index.html', 'guide.html', 'converter.html'].map(page => language + page));
const digest = bytes => createHash('sha256').update(bytes).digest('hex');
export async function verifyDeployment(base, { fetcher = fetch } = {}) {
  const root = new URL(base.endsWith('/') ? base : base + '/');
  if (!['http:', 'https:'].includes(root.protocol)) throw new Error('An HTTP(S) deployment URL is required.');
  async function get(url) {
    const response = await fetcher(url, { cache: 'no-store', signal: AbortSignal.timeout(15000) });
    if (!response.ok) throw new Error(`${url}: HTTP ${response.status}`);
    return response;
  }
  async function readPage(address) {
    let url = new URL(address);
    async function htmlAt(target) {
      const response = await get(target);
      if (!/^text\/html(?:;|$)/i.test(response.headers.get('content-type') || '')) throw new Error(`Incorrect HTML MIME type: ${target}`);
      return response.text();
    }
    let html = await htmlAt(url);
    const dom = new JSDOM(html);
    const refresh = dom.window.document.querySelector('meta[http-equiv="refresh"]')?.content;
    dom.window.close();
    if (refresh) {
      const target = refresh.match(/^0;\s*url=(.+)$/i)?.[1];
      if (!target) throw new Error(`Invalid branch redirect at ${url}`);
      const redirected = new URL(target, url);
      if (redirected.origin !== root.origin || !redirected.pathname.startsWith(root.pathname)) throw new Error('Unexpected redirect destination.');
      url = redirected;
      html = await htmlAt(url);
    }
    return { url, html };
  }
  const home = await readPage(root);
  if (home.html !== await readFile(new URL('../../site/index.html', import.meta.url), 'utf8')) throw new Error(`${root}: directory URL does not serve this checkout's home page`);
  for (const file of files) {
    const { url, html } = await readPage(new URL(file, root));
    const expected = await readFile(new URL('../../site/' + file, import.meta.url), 'utf8');
    if (html !== expected) throw new Error(`${url}: deployed HTML does not match this checkout`);
    const dom = new JSDOM(html);
    try {
      for (const element of dom.window.document.querySelectorAll('script[src], link[rel="stylesheet"]')) {
        const asset = new URL(element.getAttribute('src') || element.getAttribute('href'), url);
        await (await get(asset)).arrayBuffer();
      }
    } finally { dom.window.close(); }
  }
  // Check the real engine, not only an HTTP 200 Jekyll/404 fallback page.
  const assetBase = new URL('./', home.url);
  for (const file of ['styles.css', 'app.js', 'i18n.js', 'examples.js', 'en/examples.js', 'ja/examples.js', 'converter-shared.mjs', 'converter.mjs', 'converter-worker.mjs', 'converter-core.mjs', 'project-engine.mjs', 'vendor/project-parquet.mjs', 'vendor/project-parquet.wasm', 'vendor/converter-deps.mjs']) {
    const response = await get(new URL(file, assetBase));
    if (/\.m?js$/.test(file) && !/^(application|text)\/javascript(?:;|$)/.test(response.headers.get('content-type') || '')) throw new Error(`Incorrect JavaScript MIME type: ${file}`);
    if (file.endsWith('.wasm') && !response.headers.get('content-type')?.includes('application/wasm')) throw new Error('Incorrect WASM MIME type.');
    if (digest(new Uint8Array(await response.arrayBuffer())) !== digest(await readFile(new URL('../../site/' + file, import.meta.url)))) throw new Error(`Stale or incorrect deployed asset: ${file}`);
  }
  return 'Verified nine deployed pages, script/style links and exact project engine assets.';
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const base = process.argv[2];
  if (!base) throw new Error('Usage: node verify-deployment.mjs https://owner.github.io/repository/');
  const attempts = Number(process.env.PAGES_VERIFY_ATTEMPTS || 30);
  if (!Number.isInteger(attempts) || attempts < 1 || attempts > 60) throw new Error('PAGES_VERIFY_ATTEMPTS must be an integer from 1 to 60.');
  for (let attempt = 1; attempt <= attempts; attempt++) {
    try { console.log(await verifyDeployment(base)); break; }
    catch (error) {
      if (attempt === attempts) throw error;
      console.log(`Waiting for Pages/CDN (${attempt}/${attempts}): ${error.message}`);
      await new Promise(resolve => setTimeout(resolve, 10000));
    }
  }
}
