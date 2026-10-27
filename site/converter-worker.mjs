import { loadTable, previewTable, exportTable } from './converter-core.mjs';

let table;
self.onmessage = async ({ data }) => {
  try {
    if (data.action === 'load') {
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
