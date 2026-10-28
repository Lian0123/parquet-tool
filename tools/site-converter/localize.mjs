import { JSDOM } from 'jsdom';
import path from 'node:path';
import { catalog } from './translations.mjs';
import { runtimeEntries } from './translations-runtime.mjs';

export const languages = [
  { id: 'zh-Hant', directory: '', name: '繁體中文', og: 'zh_TW' },
  { id: 'en', directory: 'en/', name: 'English', og: 'en_US' },
  { id: 'ja', directory: 'ja/', name: '日本語', og: 'ja_JP' },
];
const pageFiles = ['index.html', 'converter.html', 'guide.html'];
const isChinese = (text) => /[\u3400-\u9fff]/.test(text);
const missing = new Set();

export function translated(text, language, strict = false) {
  if (language.id === 'zh-Hant') return text;
  const key = text.trim();
  const result = catalog(language.id)[key];
  if (result !== undefined) return text.replace(key, () => result);
  if (strict && isChinese(text)) missing.add(key);
  return text;
}

export function localizedRecipes(recipes, language) {
  return recipes.map((recipe) => ({ ...recipe,
    ...Object.fromEntries(['title', 'short', 'description', 'prerequisite', 'outcome'].map((key) => [key, translated(recipe[key], language, true)])),
  }));
}

export function localizeOutputs(source, site, recipes) {
  missing.clear();
  const outputs = new Map(source);
  const absolute = (language, page) => new URL(language.directory + (page === 'index.html' ? '' : page), site.baseUrl).href;
  const localizedUrl = (value, language) => {
    if (!value.startsWith(site.baseUrl)) return value;
    const suffix = value.slice(site.baseUrl.length);
    const [file] = suffix.split('#');
    if (file === '' || [...pageFiles, 'guide.md'].includes(file)) return site.baseUrl + language.directory + suffix;
    return value;
  };
  for (const language of languages) {
    for (const page of pageFiles) {
      const dom = new JSDOM(source.get(page));
      const document = dom.window.document;
      for (const element of document.querySelectorAll('#locale-data, #site-i18n, .language-switch, link[hreflang]')) element.remove();
      document.documentElement.lang = language.id;
      const walker = document.createTreeWalker(document.documentElement, 4);
      let node;
      while ((node = walker.nextNode())) {
        if (!node.parentElement.closest('script,style,pre,code')) node.textContent = translated(node.textContent, language, true);
      }
      for (const element of document.querySelectorAll('*')) {
        for (const attribute of ['title', 'placeholder', 'aria-label', 'alt', 'content']) {
          if (!element.hasAttribute(attribute)) continue;
          let value = translated(element.getAttribute(attribute), language, true);
          if (attribute === 'content') value = localizedUrl(value, language);
          element.setAttribute(attribute, value);
        }
        if (element.matches('meta[property="og:locale"]')) element.content = language.og;
        for (const attribute of ['href', 'src']) {
          if (!element.hasAttribute(attribute)) continue;
          const value = element.getAttribute(attribute);
          if (value.startsWith(site.baseUrl)) { element.setAttribute(attribute, localizedUrl(value, language)); continue; }
          if (!language.directory || !value.startsWith('./')) continue;
          const filename = value.slice(2).split('#')[0];
          element.setAttribute(attribute, (pageFiles.includes(filename) || filename === 'guide.md' || filename === 'examples.js') ? value : '../' + value.slice(2));
        }
      }
      for (const script of document.querySelectorAll('script[type="application/ld+json"]')) {
        const visit = (value, key) => {
          if (key === 'inLanguage') return language.id;
          if (typeof value === 'string') return localizedUrl(translated(value, language, true), language);
          if (Array.isArray(value)) return value.map((item) => visit(item));
          if (value && typeof value === 'object') return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, visit(item, key)]));
          return value;
        };
        script.textContent = JSON.stringify(visit(JSON.parse(script.textContent)), null, 2).replaceAll('<', '\\u003c');
      }
      for (const variant of languages) {
        const link = document.createElement('link'); link.rel = 'alternate'; link.hreflang = variant.id; link.href = absolute(variant, page); document.head.append(link);
      }
      const fallback = document.createElement('link'); fallback.rel = 'alternate'; fallback.hreflang = 'x-default'; fallback.href = absolute(languages[0], page); document.head.append(fallback);
      const navigation = document.createElement('nav'); navigation.className = 'language-switch wrap'; navigation.setAttribute('aria-label', 'Language');
      for (const variant of languages) {
        const link = document.createElement('a');
        const relative = path.posix.relative(language.directory || '.', variant.directory + page);
        link.href = relative.startsWith('.') ? relative : './' + relative;
        link.hreflang = variant.id; link.lang = variant.id; link.textContent = variant.name;
        if (variant.id === language.id) link.setAttribute('aria-current', 'page');
        navigation.append(link);
      }
      document.querySelector('header').after(navigation);
      const data = document.createElement('script'); data.id = 'locale-data'; data.type = 'application/json';
      const runtimeKeys = new Set([...runtimeEntries.map((entry) => entry[0]), '基本讀寫', '格式轉換', '資料處理', '準備：', '在工作資料夾的終端機執行。', '欄式儲存，適合資料分析與資料交換。', '選擇檔案後，即可查看預覽並開始轉換。']);
      const messages = language.directory ? Object.fromEntries(Object.entries(catalog(language.id)).filter(([key]) => runtimeKeys.has(key))) : {
        'Invalid Parquet file. Check that it is complete and uses a supported format.': 'Parquet 檔案無效，請確認檔案完整且使用支援的格式。',
        'nested and repeated Parquet columns are not supported': '不支援巢狀或重複的 Parquet 欄位。',
        'binary and non-UTF8 logical types are not supported by the project engine': '不支援二進位或 UTF-8 以外的 logical types。',
        'Multiple data pages per column are not supported by the project engine': '目前不支援每欄包含多個資料頁的 Parquet。',
        'unsupported Parquet physical type': '不支援這個 Parquet 實體型別。',
        'Only uncompressed Parquet is supported by the project engine': '本專案核心目前只支援無壓縮 Parquet。',
        'Only PLAIN encoding and data page v1 are supported by the project engine': '本專案核心目前只支援 PLAIN 編碼與資料頁 v1。',
        'browser table limit exceeded': '資料超過瀏覽器處理上限：100,000 列、200 欄或 2,000,000 個儲存格。',
      };
      data.textContent = JSON.stringify({ language: language.id, messages }).replaceAll('<', '\\u003c');
      const helper = document.createElement('script'); helper.id = 'site-i18n'; helper.src = language.directory ? '../i18n.js' : './i18n.js';
      document.head.append(data, helper);
      while (document.body.lastChild?.nodeType === 3 && !document.body.lastChild.textContent.trim()) document.body.lastChild.remove();
      document.body.append(document.createTextNode('\n'));
      outputs.set(language.directory + page, dom.serialize() + '\n');
      dom.window.close();
    }
    if (language.directory) {
      const localized = localizedRecipes(recipes, language);
      outputs.set(language.directory + 'examples.js', `window.PARQUET_EXAMPLES = ${JSON.stringify(localized, null, 2)};\n`);
      const title = translated('Parquet 使用教學與完整範例', language, true);
      const intro = language.id === 'ja' ? 'Node.js 18 以降。npm install parquet-tool を実行し、input.json と input.csv を同じフォルダーに保存してください。コードは example.cjs として保存し、node example.cjs で実行します。' : 'Requires Node.js 18+. Run npm install parquet-tool and save input.json and input.csv in the same directory. Save code as example.cjs and run node example.cjs.';
      outputs.set(language.directory + 'guide.md', `# ${title}\n\n${absolute(language, 'guide.html')}\n\n${intro}\n\n${localized.map((recipe) => `## ${recipe.title}\n\n${recipe.description}\n\n${recipe.prerequisite}\n\n${recipe.cli ? `\`\`\`sh\n${recipe.cli}\n\`\`\`\n\n` : ''}\`\`\`js\n${recipe.node}\n\`\`\`\n\n${recipe.outcome}`).join('\n\n')}\n`);
    }
  }
  const alternatives = (page) => languages.map((language) => `<xhtml:link rel="alternate" hreflang="${language.id}" href="${absolute(language, page)}"/>`).join('');
  outputs.set('sitemap.xml', `<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" xmlns:xhtml="http://www.w3.org/1999/xhtml">\n${languages.flatMap((language) => pageFiles.map((page) => `  <url><loc>${absolute(language, page)}</loc>${alternatives(page)}</url>`)).join('\n')}\n</urlset>\n`);
  outputs.set('llms.txt', source.get('llms.txt') + `\n## Language versions\n\n${languages.map((language) => `- [${language.name} guide](${absolute(language, 'guide.html')})`).join('\n')}\n`);
  if (missing.size) throw new Error(`Missing translations: ${JSON.stringify([...missing], null, 2)}`);
  return outputs;
}
