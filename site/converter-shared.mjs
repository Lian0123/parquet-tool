export const LIMITS = Object.freeze({ bytes: 20 * 1024 * 1024, rows: 100000, columns: 200, cells: 2000000 });

export function detectFormat(name) {
  const extension = name.split('.').at(-1).toLowerCase();
  if (!['csv', 'json', 'parquet'].includes(extension)) throw new Error('請選擇 .csv、.json 或 .parquet 檔案。');
  return extension;
}
