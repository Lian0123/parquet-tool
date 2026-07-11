'use strict';

const fs = require('fs');
const path = require('path');

const root = path.resolve(__dirname, '..');
const docsRoot = path.join(root, 'docs', 'ai-maintenance');
const entry = path.join(docsRoot, 'entry.md');
const maxAgeDays = 120;
const errors = [];

function walk(dir) {
  return fs.readdirSync(dir, { withFileTypes: true }).flatMap((item) => {
    const full = path.join(dir, item.name);
    return item.isDirectory() ? walk(full) : [full];
  });
}

const files = walk(docsRoot).filter((file) => file.endsWith('.md'));
const reachable = new Set([entry]);
const queue = [entry];
const linkPattern = /\[[^\]]+\]\(([^)]+\.md)(?:#[^)]+)?\)/g;

function inspect(file, discoverLinks) {
  const content = fs.readFileSync(file, 'utf8');
  const firstLine = content.split(/\r?\n/, 1)[0];
  const metadata = firstLine.match(/^<!-- AI-DOC: owner=([^;]+); verified=(\d{4}-\d{2}-\d{2}); sources=(.+) -->$/);
  const label = path.relative(root, file);

  if (!metadata) {
  errors.push(`${label}: missing or invalid AI-DOC metadata`);
  } else {
    const verified = new Date(`${metadata[2]}T00:00:00Z`);
    const age = (Date.now() - verified.getTime()) / 86400000;
    if (Number.isNaN(verified.getTime()) || age < -1 || age > maxAgeDays) {
      errors.push(`${label}: invalid or stale verified date (>${maxAgeDays} days)`);
    }
    for (const source of metadata[3].split(',').map((value) => value.trim())) {
      if (!source || !fs.existsSync(path.resolve(root, source))) {
        errors.push(`${label}: source does not exist: ${source || '(empty)'}`);
      }
    }
  }

  for (const match of content.matchAll(linkPattern)) {
    const target = path.resolve(path.dirname(file), decodeURI(match[1]));
    if (!target.startsWith(docsRoot + path.sep) || !fs.existsSync(target)) {
      errors.push(`${label}: broken link or link outside maintenance docs: ${match[1]}`);
    } else if (discoverLinks && !reachable.has(target)) {
      reachable.add(target);
      queue.push(target);
    }
  }
}

for (const file of files) inspect(file, false);
while (queue.length) inspect(queue.shift(), true);
for (const file of files) {
  if (!reachable.has(file)) errors.push(`${path.relative(root, file)}: unreachable from entry`);
}

if (errors.length) {
  console.error(`AI documentation check failed (${errors.length})\n- ${errors.join('\n- ')}`);
  process.exit(1);
}
console.log(`AI documentation check passed: ${files.length} files reachable from entry.`);
