'use strict';

/**
 * Install hook for parquet-tool.
 *
 * 1. First tries to load an existing pre-built binary (bundled in the npm
 *    package under `prebuilds/`) via node-gyp-build.  If found, nothing
 *    needs to be compiled and we exit immediately.
 *
 * 2. If no pre-built binary matches the current platform/arch (e.g. when
 *    cloning the repository for development), the script falls back to
 *    compiling the native addon from source using cmake-js (devDependency).
 *
 * For end-users installing from npm, step 1 always succeeds because the
 * package bundles pre-built binaries for all major platforms:
 *   linux-x64, darwin-x64, darwin-arm64, win32-x64
 */

const path = require('path');
const { spawnSync } = require('child_process');

const pkg = path.join(__dirname, '..');

// ── 1. Try to load an existing binary ───────────────────────────────────────
try {
  require('node-gyp-build')(pkg);
  // A matching binary was found – nothing to compile.
  process.exit(0);
} catch (_err) {
  // No prebuild or build/Release binary available.  Fall through to compile.
}

// ── 2. Compile from source via cmake-js (available in devDependencies) ──────
// When npm runs lifecycle scripts it adds node_modules/.bin to PATH,
// so cmake-js is available in dev/ci installs even though it is a devDep.
const cmakeJsBin = process.platform === 'win32' ? 'cmake-js.cmd' : 'cmake-js';
const result = spawnSync(cmakeJsBin, ['compile'], {
  stdio: 'inherit',
  cwd: pkg,
  shell: true,
});

if (result.status === 0) {
  process.exit(0);
}

// ── Compilation failed ───────────────────────────────────────────────────────
console.error(
  '\n[parquet-tool] ⚠  Native addon not found and source compilation failed.\n' +
    'Pre-built binaries are provided for: linux-x64, darwin-x64, darwin-arm64, win32-x64\n' +
    'To compile from source you need: cmake ≥ 3.15, and a C++17 compiler.\n' +
    'Run: npm run build:native\n',
);
process.exit(1);
