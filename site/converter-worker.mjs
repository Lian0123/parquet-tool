// Install the handler synchronously: a static import awaits WASM before this runs
// and can lose the initial postMessage while module evaluation is suspended.
let corePromise;
function core() {
  return corePromise ??= import('./converter-core.mjs').catch(() => {
    throw new Error('本地轉換引擎載入失敗，請重新整理頁面後再試。');
  });
}

let table;
self.onmessage = async ({ data }) => {
  try {
    const { loadTable, previewTable, exportTable } = await core();
    if (data.action === 'load') {
      self.postMessage({ action: 'reading' });
      table = undefined;
      const buffer = await data.file.arrayBuffer();
      table = await loadTable(buffer, data.file.name, data.options);
      self.postMessage({ action: 'loaded', preview: previewTable(table) });
    } else if (data.action === 'convert') {
      if (!table) throw new Error('請先選擇檔案。');
      const result = exportTable(table, data.format, data.options);
      self.postMessage({ action: 'converted', ...result }, [result.bytes.buffer]);
    }
  } catch (error) {
    self.postMessage({ action: 'error', message: error instanceof Error ? error.message : String(error) });
  }
};
