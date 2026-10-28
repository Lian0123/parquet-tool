'use strict';

// npm ships its own locked dependency bundle, which npm overrides do not replace.
// Until npm ships a patched bundle, copy the official patched development dependency.
const fs = require('node:fs');
const path = require('node:path');
const root = path.resolve(__dirname, '..');
const source = path.join(root, 'node_modules/ip-address');
const target = path.join(root, 'node_modules/npm/node_modules/ip-address');
const safe = (version) => {
  const [major, minor, patch] = version.split('.').map(Number);
  return major > 10 || (major === 10 && (minor > 7 || (minor === 7 && patch >= 4)));
};
if (!fs.existsSync(path.join(target, 'package.json'))) process.exit(0); // npm is a dev-only dependency.
const fixed = JSON.parse(fs.readFileSync(path.join(source, 'package.json'), 'utf8'));
if (fixed.name !== 'ip-address' || !safe(fixed.version)) throw new Error('A patched ip-address >=10.7.4 is required.');
const current = JSON.parse(fs.readFileSync(path.join(target, 'package.json'), 'utf8'));
if (!safe(current.version)) {
  fs.rmSync(target, { recursive: true, force: true });
  fs.cpSync(source, target, { recursive: true });
  console.log(`Replaced npm bundled ip-address ${current.version} with official ${fixed.version}.`);
}
