import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile, access } from 'node:fs/promises';
import { JSDOM } from 'jsdom';
import vm from 'node:vm';
import { languages } from '../localize.mjs';
import { site, pages } from '../seo.config.mjs';

const root = new URL('../../../site/', import.meta.url);
for (const language of languages) for (const page of pages) test(`${language.id}/${page.file}: translated static content, language links and real local assets`, async () => {
  const file = new URL(language.directory + page.file, root);
  const dom = new JSDOM(await readFile(file, 'utf8'), { runScripts: 'outside-only' });
  try {
    const document = dom.window.document;
    assert.equal(document.documentElement.lang, language.id);
    assert.equal(document.querySelectorAll('.language-switch a').length, 3);
    assert.equal(document.querySelector('link[rel="canonical"]').href, new URL(language.directory + (page.file === 'index.html' ? '' : page.file), site.baseUrl).href);
    const links = [...document.querySelectorAll('link[hreflang]')];
    assert.deepEqual(links.map((link) => link.hreflang), ['zh-Hant', 'en', 'ja', 'x-default']);
    for (const node of document.querySelectorAll('[href], [src]')) {
      const href = node.getAttribute('href') || node.getAttribute('src');
      if (/^https?:|^#/.test(href)) continue;
      await access(new URL(href, file));
    }
    if (language.id === 'en') {
      const walker = document.createTreeWalker(document.body, 4); let node;
      while ((node = walker.nextNode())) if (!node.parentElement.closest('script,style,pre,code,.language-switch')) assert.doesNotMatch(node.textContent, /[\u3400-\u9fff]/);
    }
    dom.window.eval(await readFile(new URL('i18n.js', root), 'utf8'));
    assert.equal(typeof dom.window.siteTranslate, 'function');
    if (language.directory) assert.notEqual(dom.window.siteTranslate('JSON 格式錯誤，請檢查逗號、引號與括號。'), 'JSON 格式錯誤，請檢查逗號、引號與括號。');
    const schema = JSON.parse(document.querySelector('script[type="application/ld+json"]').textContent);
    assert.equal(schema['@graph'].find((item) => item['@type'] === 'WebPage').inLanguage, language.id);
    if (page.file === 'guide.html') assert.equal(document.querySelectorAll('.guide-recipes article').length, 12);
    if (page.file === 'index.html' && language.directory) {
      const context = { window: {} };
      vm.runInNewContext(await readFile(new URL('examples.js', file), 'utf8'), context);
      assert.equal(context.window.PARQUET_EXAMPLES.length, 12);
      assert.notEqual(context.window.PARQUET_EXAMPLES[0].title, '建立第一個 Parquet 檔案');
    }
  } finally { dom.window.close(); }
});
