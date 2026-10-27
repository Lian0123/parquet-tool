import test from 'node:test';
import assert from 'node:assert/strict';
import { access, readFile } from 'node:fs/promises';
import vm from 'node:vm';
import { JSDOM } from 'jsdom';
import { site, pages } from '../seo.config.mjs';
import { generateSeo } from '../seo.mjs';
import { LIMITS } from '../../../site/converter-shared.mjs';

const directory = new URL('../../../site/', import.meta.url);
const canonical = (file) => new URL(file === 'index.html' ? '' : file, site.baseUrl).href;
const readPage = async (file) => new JSDOM(await readFile(new URL(file, directory), 'utf8'));

for (const page of pages) test(`${page.file} has unique indexable metadata, crawlable social image, and truthful structured data`, async () => {
  const dom = await readPage(page.file);
  try {
    const document = dom.window.document;
    assert.equal(document.documentElement.lang, 'zh-Hant');
    assert.equal(document.querySelectorAll('h1').length, 1);
    assert.equal(document.querySelectorAll('title').length, 1);
    assert.equal(document.title, page.title);
    assert.equal(document.querySelectorAll('meta[name="description"]').length, 1);
    assert.equal(document.querySelector('meta[name="description"]').content, page.description);
    assert.equal(document.querySelector('link[rel="canonical"]').href, canonical(page.file));
    assert.match(document.querySelector('meta[name="robots"]').content, /index, follow/);
    assert.equal(document.querySelector('meta[property="og:url"]').content, canonical(page.file));
    for (const prefix of ['og', 'twitter']) {
      assert.equal(document.querySelector(`meta[${prefix === 'og' ? 'property' : 'name'}="${prefix}:title"]`).content, page.title);
      const image = document.querySelector(`meta[${prefix === 'og' ? 'property' : 'name'}="${prefix}:image"]`).content;
      assert.equal(image, new URL('social-card.png', site.baseUrl).href);
      const png = await readFile(new URL('social-card.png', directory));
      assert.deepEqual([...png.subarray(0, 8)], [137, 80, 78, 71, 13, 10, 26, 10]);
      assert.equal(png.readUInt32BE(16), 1200);
      assert.equal(png.readUInt32BE(20), 630);
    }
    const data = JSON.parse(document.querySelector('script[type="application/ld+json"]').textContent);
    assert.equal(data['@context'], 'https://schema.org');
    assert.equal(data['@graph'].find((item) => item['@type'] === 'WebPage').url, canonical(page.file));
    assert.ok(!JSON.stringify(data).includes('aggregateRating'), 'Do not invent ratings');
    const faq = data['@graph'].find((item) => item['@type'] === 'FAQPage');
    if (page.faqs.length) {
      const visible = [...document.querySelectorAll('#faq details')];
      assert.equal(visible.length, faq.mainEntity.length);
      for (const [index, question] of faq.mainEntity.entries()) {
        assert.equal(question.name, visible[index].querySelector('summary').textContent);
        assert.equal(question.acceptedAnswer.text, visible[index].querySelector('p').textContent);
        assert.equal(visible[index].closest('[hidden]'), null);
      }
    }
    if (page.file !== 'index.html') {
      const breadcrumb = data['@graph'].find((item) => item['@type'] === 'BreadcrumbList');
      assert.equal(breadcrumb.itemListElement[1].item, canonical(page.file));
      assert.ok(document.querySelector('nav[aria-label="麵包屑"]').textContent.includes(page.label));
    }
  } finally { dom.window.close(); }
});

test('all 12 recipes and exact runnable snippets are present without JavaScript in HTML and Markdown', async () => {
  const context = { window: {} };
  vm.runInNewContext(await readFile(new URL('examples.js', directory), 'utf8'), context);
  const dom = await readPage('guide.html');
  const markdown = await readFile(new URL('guide.md', directory), 'utf8');
  try {
    assert.equal(dom.window.document.querySelectorAll('.guide-recipes article').length, context.window.PARQUET_EXAMPLES.length);
    assert.equal(dom.window.document.querySelectorAll('script[src]').length, 0);
    for (const recipe of context.window.PARQUET_EXAMPLES) {
      const article = dom.window.document.getElementById(`recipe-${recipe.id}`);
      assert.equal(article.querySelector('h2').textContent, recipe.title);
      const snippets = [...article.querySelectorAll('pre code')].map((node) => node.textContent);
      assert.ok(snippets.includes(recipe.node));
      assert.ok(markdown.includes(recipe.node));
      if (recipe.cli) { assert.ok(snippets.includes(recipe.cli)); assert.ok(markdown.includes(recipe.cli)); }
    }
  } finally { dom.window.close(); }
});

test('sitemap lists only the three canonical HTML pages with consistent URLs', async () => {
  const xml = await readFile(new URL('sitemap.xml', directory), 'utf8');
  const dom = new JSDOM(xml, { contentType: 'application/xml' });
  try {
    assert.deepEqual([...dom.window.document.querySelectorAll('loc')].map((element) => element.textContent), pages.map((page) => canonical(page.file)));
    assert.equal(dom.window.document.documentElement.namespaceURI, 'http://www.sitemaps.org/schemas/sitemap/0.9');
    assert.equal(new Set(pages.map((page) => page.title)).size, pages.length);
    assert.equal(new Set(pages.map((page) => page.description)).size, pages.length);
  } finally { dom.window.close(); }
});

test('advertised converter limits match the real engine limits, optional LLM index links real pages', async () => {
  const dom = await readPage('converter.html');
  try {
    for (const token of [`${LIMITS.bytes / 1024 / 1024} MiB`, LIMITS.rows.toLocaleString('en-US'),
      `${LIMITS.columns} 欄`, LIMITS.cells.toLocaleString('en-US')]) assert.ok(dom.window.document.body.textContent.includes(token), token);
  } finally { dom.window.close(); }
  const llms = await readFile(new URL('llms.txt', directory), 'utf8');
  for (const match of llms.matchAll(/\]\((https:[^)]+)\)/g)) {
    if (match[1].startsWith(site.baseUrl)) await access(new URL(match[1].slice(site.baseUrl.length) || 'index.html', directory));
  }
  assert.match(llms, /does not.*guarantee/i);
});

test('generated SEO blocks and static documentation are current', async () => {
  await generateSeo({ check: true });
});
