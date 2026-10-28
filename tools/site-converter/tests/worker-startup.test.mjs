import test from 'node:test';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

for (const fail of [false, true]) test(fail ? 'worker reports asynchronous WASM initialization failure' : 'worker receives the first load before delayed WASM initialization finishes', () => {
  execFileSync(process.execPath, ['--experimental-vm-modules', fileURLToPath(new URL('./fixtures/worker-startup.mjs', import.meta.url)), ...(fail ? ['--fail-wasm'] : [])], { timeout: 15000, stdio: 'pipe' });
});
