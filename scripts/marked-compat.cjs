// Ensure marked deprecation warnings do not appear during semantic-release.
// semantic-release dependencies may still call marked with legacy defaults.
try {
  const { marked } = require('marked');
  marked.setOptions({
    mangle: false,
    headerIds: false,
  });
} catch (_err) {
  // Keep release flow running even if marked is not resolvable.
}

const originalWarn = console.warn;
console.warn = (...args) => {
  const first = String(args[0] ?? '');
  if (
    first.startsWith('marked(): mangle parameter is enabled by default') ||
    first.startsWith('marked(): headerIds and headerPrefix parameters enabled by default')
  ) {
    return;
  }
  originalWarn(...args);
};
