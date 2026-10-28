import { readFile, writeFile, mkdir } from 'node:fs/promises';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

const root = new URL('../../', import.meta.url);
export const entryFiles = ['', 'en/', 'ja/'].flatMap(language =>
  ['index.html', 'guide.html', 'converter.html'].map(page => language + page));

export function branchEntry(file) {
  const target = path.posix.relative(path.posix.dirname(file), 'site/' + file);
  const language = file.startsWith('en/') ? 'en' : file.startsWith('ja/') ? 'ja' : 'zh-Hant';
  return `<!doctype html>
<html lang="${language}">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="0; url=${target}">
  <title>parquet-tool</title>
  <script>location.replace(${JSON.stringify(target)} + location.search + location.hash);</script>
</head>
<body><a href="${target}">繁體中文 · English · 日本語 — parquet-tool</a></body>
</html>
`;
}

export async function generateBranchEntries({ check = false } = {}) {
  for (const file of entryFiles) {
    const url = new URL(file, root);
    const expected = branchEntry(file);
    if (check) {
      if (await readFile(url, 'utf8').catch(() => '') !== expected) throw new Error(`Stale branch Pages entry: ${file}`);
    } else {
      await mkdir(new URL('./', url), { recursive: true });
      await writeFile(url, expected);
    }
  }
  console.log(`${check ? 'Verified' : 'Generated'} nine branch Pages entry points.`);
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) await generateBranchEntries({ check: process.argv.includes('--check') });
