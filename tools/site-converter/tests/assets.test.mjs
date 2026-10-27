import test from 'node:test';
import assert from 'node:assert/strict';
import { access, readFile } from 'node:fs/promises';
import { JSDOM } from 'jsdom';

for (const page of ['index.html', 'converter.html', 'guide.html']) test(`${page} assets and navigation work beneath the GitHub Pages project prefix`, async () => {
  const file = new URL(`../../../site/${page}`, import.meta.url);
  const dom = new JSDOM(await readFile(file, 'utf8'));
  try {
    const ids = new Set();
    for (const element of dom.window.document.querySelectorAll('[id]')) {
      assert.ok(!ids.has(element.id), `Duplicate ID ${element.id}`);
      ids.add(element.id);
    }
    for (const element of dom.window.document.querySelectorAll('[src], [href]')) {
      const target = element.getAttribute('src') || element.getAttribute('href');
      if (/^[a-z]+:/i.test(target)) continue;
      assert.ok(!target.startsWith('/'), `Absolute asset path ${target}`);
      if (target.startsWith('#')) {
        if (target !== '#' && !target.startsWith('#example-')) assert.ok(ids.has(target.slice(1)), `Missing anchor ${target}`);
      } else {
        const resolved = new URL(target, file);
        await access(resolved);
        if (resolved.hash) {
          const destination = new JSDOM(await readFile(resolved, 'utf8'));
          try { assert.ok(destination.window.document.getElementById(resolved.hash.slice(1))); }
          finally { destination.window.close(); }
        }
      }
    }
  } finally { dom.window.close(); }
});
