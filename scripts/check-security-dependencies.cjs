'use strict';
const fs = require('node:fs');
const path = require('node:path');
const root = path.resolve(__dirname, '..');
const safe = (version) => {
  const [major, minor, patch] = version.split('.').map(Number);
  return major > 10 || (major === 10 && (minor > 7 || (minor === 7 && patch >= 4)));
};
let found = 0;
for (const file of ['package-lock.json', 'examples/package-lock.json', 'tools/site-converter/package-lock.json']) {
  const lock = JSON.parse(fs.readFileSync(path.join(root, file), 'utf8'));
  for (const [key, entry] of Object.entries(lock.packages || {})) {
    const selector = key.endsWith('/postcss-selector-parser');
    if (!key.endsWith('/ip-address') && !selector) continue;
    const installed = path.resolve(root, path.dirname(file), key, 'package.json');
    if (!fs.existsSync(installed)) throw new Error(`Missing ${installed}; run npm ci first.`);
    const actual = JSON.parse(fs.readFileSync(installed, 'utf8')).version;
    if (selector) {
      const atLeastFixed = version => {
        const [major, minor, patch] = version.split('.').map(Number);
        return major > 7 || (major === 7 && (minor > 1 || (minor === 1 && patch >= 6)));
      };
      if (!atLeastFixed(entry.version) || !atLeastFixed(actual) || actual !== entry.version) {
        throw new Error(`Vulnerable or mismatched postcss-selector-parser at ${file}: ${key}, lock=${entry.version}, installed=${actual}`);
      }
      console.log(`Verified locked and installed postcss-selector-parser ${actual}.`);
      found++;
      continue;
    }
    if (!safe(actual)) throw new Error(`Vulnerable installed ${key}@${actual}; run the postinstall patch.`);
    if (!safe(entry.version)) {
      if (file !== 'package-lock.json' || key !== 'node_modules/npm/node_modules/ip-address' || !entry.inBundle) {
        throw new Error(`Unmitigated vulnerable lock entry: ${file}: ${key}@${entry.version}`);
      }
      console.log(`Upstream npm bundle locks ${entry.version}; postinstall mitigation verified ${actual}. Dependabot may still report the upstream bundle.`);
    }
    found++;
  }
}
console.log(`Verified ${found} dependency copies: ip-address >=10.7.4 (installed), postcss-selector-parser >=7.1.6 (locked and installed).`);
