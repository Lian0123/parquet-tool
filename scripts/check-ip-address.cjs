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
    if (!key.endsWith('/ip-address')) continue;
    const installed = path.resolve(root, path.dirname(file), key, 'package.json');
    if (!fs.existsSync(installed)) throw new Error(`Missing ${installed}; run npm ci first.`);
    const actual = JSON.parse(fs.readFileSync(installed, 'utf8')).version;
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
console.log(`Verified ${found} installed ip-address copies are >=10.7.4.`);
