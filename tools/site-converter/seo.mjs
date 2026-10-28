import { readFile, writeFile, mkdir } from 'node:fs/promises';
import { localizeOutputs } from './localize.mjs';
import { pathToFileURL } from 'node:url';
import vm from 'node:vm';
import { site, pages } from './seo.config.mjs';

const root = new URL('../../site/', import.meta.url);
const absolute = (file = '') => new URL(file === 'index.html' ? '' : file, site.baseUrl).href;
const escape = (value) => String(value).replace(/[&<>"']/g, (char) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[char]));
const json = (value) => JSON.stringify(value, null, 2).replaceAll('<', '\\u003c');
const context = { window: {} };
vm.runInNewContext(await readFile(new URL('examples.js', root), 'utf8'), context);
const recipes = context.window.PARQUET_EXAMPLES;

function head(page) {
  const url = absolute(page.file);
  const graph = [
    { '@type': 'WebSite', '@id': absolute('#website'), url: absolute(), name: site.name, inLanguage: site.language },
    { '@type': 'WebPage', '@id': `${url}#page`, url, name: page.title, description: page.description,
      inLanguage: site.language, isPartOf: { '@id': absolute('#website') },
      ...(page.file === 'converter.html' ? { mainEntity: { '@id': `${url}#application` } } : {}) },
  ];
  if (page.file === 'converter.html') graph.push({
    '@type': 'WebApplication', '@id': `${url}#application`, name: page.label, url,
    description: page.description, applicationCategory: 'DeveloperApplication', operatingSystem: 'Web',
    isAccessibleForFree: true, inLanguage: site.language,
    browserRequirements: 'Requires JavaScript modules and module Web Workers over HTTP(S).',
    softwareHelp: { '@type': 'CreativeWork', url: absolute('guide.html') },
    featureList: ['CSV、JSON、平面 Parquet 六向互轉', '本機處理，不上傳檔案', '資料預覽與下載', '單檔最多 20 MiB、100,000 列、200 欄、2,000,000 個儲存格'],
  });
  if (page.file !== 'index.html') graph.push({
    '@type': 'BreadcrumbList', '@id': `${url}#breadcrumb`, itemListElement: [
      { '@type': 'ListItem', position: 1, name: '首頁', item: absolute() },
      { '@type': 'ListItem', position: 2, name: page.label, item: url },
    ],
  });
  if (page.faqs.length) graph.push({
    '@type': 'FAQPage', '@id': `${url}#faq`, isPartOf: { '@id': `${url}#page` },
    mainEntity: page.faqs.map(([name, text]) => ({ '@type': 'Question', name, acceptedAnswer: { '@type': 'Answer', text } })),
  });
  if (page.file === 'guide.html') graph.push({
    '@type': 'TechArticle', '@id': `${url}#article`, headline: page.label, inLanguage: site.language,
    mainEntityOfPage: { '@id': `${url}#page` }, about: ['Parquet', 'Node.js', 'TypeScript', 'CLI'],
    hasPart: recipes.map((recipe) => ({ '@type': 'SoftwareSourceCode', name: recipe.title,
      url: `${url}#recipe-${recipe.id}`, programmingLanguage: 'JavaScript', codeRepository: site.repository })),
  });
  return `  <title>${escape(page.title)}</title>
  <meta name="description" content="${escape(page.description)}">
  <meta name="robots" content="index, follow, max-image-preview:large">
  <link rel="canonical" href="${url}">
  <link rel="sitemap" type="application/xml" href="./sitemap.xml">
  <meta property="og:type" content="${page.file === 'guide.html' ? 'article' : 'website'}">
  <meta property="og:site_name" content="${site.name}">
  <meta property="og:locale" content="zh_TW">
  <meta property="og:title" content="${escape(page.title)}">
  <meta property="og:description" content="${escape(page.description)}">
  <meta property="og:url" content="${url}">
  <meta property="og:image" content="${absolute('social-card.png')}">
  <meta property="og:image:width" content="1200">
  <meta property="og:image:height" content="630">
  <meta property="og:image:alt" content="parquet-tool：CSV、JSON、Parquet 瀏覽器轉換與 Node.js 使用範例">
  <meta name="twitter:card" content="summary_large_image">
  <meta name="twitter:title" content="${escape(page.title)}">
  <meta name="twitter:description" content="${escape(page.description)}">
  <meta name="twitter:image" content="${absolute('social-card.png')}">
  <meta name="twitter:image:alt" content="parquet-tool：CSV、JSON、Parquet 瀏覽器轉換與 Node.js 使用範例">
  <script type="application/ld+json">${json({ '@context': 'https://schema.org', '@graph': graph })}</script>`;
}

function faq(page) {
  return `<section id="faq" class="search-faq wrap" aria-labelledby="faq-title">
      <p class="eyebrow">QUESTIONS & ANSWERS</p><h2 id="faq-title">Parquet 轉換常見問題</h2>
      ${page.faqs.map(([question, answer]) => `<details><summary>${escape(question)}</summary><p>${escape(answer)}</p></details>`).join('\n      ')}
      <p class="content-review">內容檢核：<time datetime="${site.reviewed}">${site.reviewed}</time> · <a href="${site.repository}">原始碼與維護紀錄 ↗</a> · <a href="./guide.html">完整使用教學</a></p>
    </section>`;
}

function replaceBlock(html, name, content) {
  const expression = new RegExp(`<!-- ${name}:START -->[\\s\\S]*?<!-- ${name}:END -->`);
  if (!expression.test(html)) throw new Error(`Missing ${name} generation markers`);
  return html.replace(expression, () => `<!-- ${name}:START -->\n${content}\n  <!-- ${name}:END -->`);
}

function guide() {
  const page = pages.find((item) => item.file === 'guide.html');
  return `<!doctype html>
<html lang="zh-Hant">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="theme-color" content="#183f35">
${head(page)}
  <link rel="icon" href="./favicon.svg" type="image/svg+xml">
  <link rel="stylesheet" href="./styles.css">
  <link rel="stylesheet" href="./guide.css">
  <link rel="alternate" type="text/markdown" href="./guide.md" title="Markdown 完整教學">
</head>
<body>
  <a class="skip-link" href="#main">跳至主要內容</a>
  <header class="header"><a class="brand" href="./index.html"><span class="brand-mark" aria-hidden="true">▥</span>parquet<span class="brand-light">-tool</span><span class="brand-label">GUIDE</span></a><nav aria-label="主要導覽"><a href="./index.html#examples">互動範例</a><a href="./converter.html">轉換工具</a><a href="./guide.html" aria-current="page">完整教學</a></nav></header>
  <main id="main" class="wrap guide-main">
    <nav class="breadcrumbs" aria-label="麵包屑"><a href="./index.html">首頁</a><span aria-hidden="true">/</span><span>${page.label}</span></nav>
    <p class="eyebrow">PARQUET / NODE.JS / CLI</p><h1>Parquet 使用教學與完整範例</h1>
    <p class="guide-lead">這份教學整理 ${recipes.length} 個 parquet-tool 任務，提供前置需求、預期結果與可複製的 CLI／Node.js 程式碼。內容直接呈現在 HTML 中，不需 JavaScript 即可閱讀。</p>
    <p class="content-review">內容檢核：<time datetime="${site.reviewed}">${site.reviewed}</time> · <a href="${site.repository}">GitHub 原始碼 ↗</a> · <a href="./guide.md">Markdown 版本</a></p>
    <section class="guide-setup" aria-labelledby="setup-title"><h2 id="setup-title">開始前：安裝與準備資料</h2><p>需要 Node.js 18 以上。建立工作資料夾後執行以下指令，並下載 <a href="./samples/input.json" download>input.json</a> 與 <a href="./samples/input.csv" download>input.csv</a> 到同一資料夾。</p><pre><code>npm init -y
npm install parquet-tool</code></pre><p>CLI 指令直接在終端機執行。Node.js 程式碼請儲存為 <code>example.cjs</code>，再執行 <code>node example.cjs</code>。安裝原生模組所需工具鏈請見<a href="${site.repository}/blob/main/docs/installation.md">安裝文件</a>。</p><p>若只要轉換 CSV／JSON／Parquet，可使用<a href="./converter.html">瀏覽器轉換工具</a>，不必安裝 Node.js。網頁與 Node.js／CLI 共用本專案 C++ 核心；Parquet 支援無壓縮 PLAIN 資料頁 v1。混合型別、空值與容量限制詳見<a href="./converter.html#faq">轉換常見問題</a>。</p></section>
    <div class="guide-layout"><nav class="guide-toc" aria-label="教學目錄"><h2>任務目錄</h2>${recipes.map((recipe, index) => `<a href="#recipe-${recipe.id}"><span>${String(index + 1).padStart(2, '0')}</span>${escape(recipe.short)}</a>`).join('')}</nav><div class="guide-recipes">
      ${recipes.map((recipe) => `<article id="recipe-${recipe.id}"><h2>${escape(recipe.title)}</h2><p>${escape(recipe.description)}</p><p class="guide-prerequisite"><strong>準備：</strong>${escape(recipe.prerequisite)}</p>${recipe.cli ? `<h3>CLI 指令</h3><pre><code>${escape(recipe.cli)}</code></pre>` : ''}<h3>Node.js 程式碼</h3><pre><code>${escape(recipe.node)}</code></pre><p><strong>執行後：</strong>${escape(recipe.outcome)}</p><a class="guide-source" href="${site.repository}/blob/main/src/lib/${recipe.source}">查看此功能原始碼 ↗</a></article>`).join('\n      ')}
    </div></div>
    <section class="guide-setup"><h2>下一步</h2><p>查閱<a href="${site.repository}/blob/main/docs/api.md">完整 API 文件</a>、<a href="${site.repository}/blob/main/docs/cli.md">CLI 指令文件</a>或<a href="${site.repository}/tree/main/examples">可執行範例</a>。遇到錯誤可在<a href="${site.repository}/issues">GitHub Issues</a>回報。</p></section>
  </main>
  <footer class="wrap footer"><a class="brand" href="./index.html">parquet<span class="brand-light">-tool</span></a><span>MIT License · Node.js 與 CLI 使用教學</span><a href="./converter.html">直接轉換檔案 ↗</a></footer>
</body>
</html>
`;
}

function markdown() {
  return `# Parquet 使用教學與完整範例\n\n${absolute('guide.html')}\n\nNode.js 18+。先安裝 parquet-tool，下載 [input.json](${absolute('samples/input.json')}) 與 [input.csv](${absolute('samples/input.csv')}) 到工作資料夾。Node.js 程式碼另存為 example.cjs 後執行 node example.cjs。\n\n\`\`\`sh\nnpm init -y\nnpm install parquet-tool\n\`\`\`\n\n網頁與 Node.js／CLI 共用本專案 C++ 核心；Parquet 支援無壓縮 PLAIN 資料頁 v1。\n\n${recipes.map((recipe) => `## ${recipe.title}\n\n${recipe.description}\n\n準備：${recipe.prerequisite}\n\n${recipe.cli ? `### CLI\n\n\`\`\`sh\n${recipe.cli}\n\`\`\`\n\n` : ''}### Node.js\n\n\`\`\`js\n${recipe.node}\n\`\`\`\n\n執行後：${recipe.outcome}\n\n[原始碼](${site.repository}/blob/main/src/lib/${recipe.source})`).join('\n\n')}\n`;
}

export async function generateSeo({ check = false } = {}) {
  const base = new URL(site.baseUrl);
  if (base.protocol !== 'https:' || base.search || base.hash || !base.pathname.endsWith('/')) throw new Error('baseUrl must be a canonical HTTPS directory URL');
  const output = new Map();
  for (const page of pages.filter((item) => item.file !== 'guide.html')) {
    let html = await readFile(new URL(page.file, root), 'utf8');
    html = replaceBlock(html, 'SEO', head(page));
    html = replaceBlock(html, 'FAQ', faq(page));
    output.set(page.file, html);
  }
  output.set('guide.html', guide());
  output.set('guide.md', markdown());
  output.set('sitemap.xml', `<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n${pages.map((page) => `  <url><loc>${escape(absolute(page.file))}</loc></url>`).join('\n')}\n</urlset>\n`);
  output.set('llms.txt', `# parquet-tool\n\n> Parquet utilities, executable Node.js/CLI recipes, and a browser-local CSV/JSON/Parquet converter.\n\n## Pages\n\n- [Overview](${absolute()}): Traditional Chinese project overview and interactive recipes.\n- [Browser converter](${absolute('converter.html')}): CSV, JSON, flat Parquet conversion in a local module worker; no file uploads. Maximum 20 MiB, 100,000 rows, 200 columns, 2,000,000 cells.\n- [Full guide](${absolute('guide.html')}): All ${recipes.length} CLI/CommonJS recipes, prerequisites and outcomes, available without JavaScript.\n- [Markdown guide](${absolute('guide.md')}): Same recipes as plain Markdown.\n- [Source repository](${site.repository}): Public API, CLI, native implementation and issue tracker.\n\n## Accuracy notes\n\nThe browser compiles the same project C++ core as the Node addon to WebAssembly. Parquet support is limited to uncompressed PLAIN data page v1; unsupported codecs/encodings are rejected. The browser does not support nested/binary columns or preserve all source schema/metadata. INT64 exports to JSON/CSV as decimal strings. CLI recipes need Node.js 18+. This optional index does not grant crawling permissions or guarantee inclusion in AI search.\n`);
  for (const [file, content] of localizeOutputs(output, site, recipes)) {
    if (check) {
      let actual;
      try { actual = await readFile(new URL(file, root), 'utf8'); } catch { /* report missing output */ }
      if (actual !== content) throw new Error(`${file} is stale; run npm run seo --prefix tools/site-converter`);
    } else {
      await mkdir(new URL('./', new URL(file, root)), { recursive: true });
      await writeFile(new URL(file, root), content);
    }
  }
  console.log(`${check ? 'Verified' : 'Generated'} SEO metadata, visible FAQ, static guide, sitemap and optional LLM index.`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  await generateSeo({ check: process.argv.includes('--check') });
}
