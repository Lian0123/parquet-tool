/* Runnable CommonJS recipes. Keep API names in sync with src/lib/index.ts. */
window.PARQUET_EXAMPLES = [
  {
    id: 'write', title: '建立第一個 Parquet 檔案', short: '建立檔案', category: 'basics',
    description: '明確定義 schema，將 JSON 資料寫入 Parquet。rowGroupSize 設為 2，方便後續觀察 row group。',
    prerequisite: '先下載 input.json 到工作資料夾。重跑會覆寫 data.parquet。',
    outcome: '產生包含 3 筆資料、2 個 row group 的 data.parquet。', source: 'writer.ts',
    cli: 'npx parquet-tool write data.parquet -i input.json -s "id:INT32,name:STRING,active:BOOLEAN" --row-group-size 2',
    node: `const { readFileSync } = require('node:fs');
const { Schema, ParquetWriter } = require('parquet-tool');

const rows = JSON.parse(readFileSync('input.json', 'utf8'));
const schema = Schema.create({
  id: 'INT32', name: 'STRING', active: 'BOOLEAN',
});

ParquetWriter.withWriter('data.parquet', schema,
  { rowGroupSize: 2 }, (writer) => {
    writer.write(rows);
  }); // 自動關閉 writer，完成檔案寫入`
  },
  {
    id: 'read', title: '讀取資料列', short: '讀取資料', category: 'basics',
    description: 'CLI 輸出 JSON；Node.js 使用 iterator 逐筆處理，避免一次合併整份檔案。',
    prerequisite: '先執行「建立檔案」，準備 data.parquet。',
    outcome: '在終端機看到 id、name 與 active 欄位的資料。', source: 'reader.ts',
    cli: 'npx parquet-tool read data.parquet --json --limit 20',
    node: `const { ParquetReader } = require('parquet-tool');

ParquetReader.withReader('data.parquet', (reader) => {
  for (const row of reader.iterateRows()) {
    console.log(row);
  }
}); // 自動關閉 reader`
  },
  {
    id: 'metadata', title: '查看 schema 與中繼資料', short: '查看中繼資料', category: 'basics',
    description: '確認欄位型別、資料筆數及 row group 數量，先了解檔案結構。',
    prerequisite: '先執行「建立檔案」，準備 data.parquet。',
    outcome: '顯示檔案的 schema、numRows、numRowGroups 等中繼資料。', source: 'reader.ts',
    cli: 'npx parquet-tool info data.parquet',
    node: `const { ParquetReader } = require('parquet-tool');

const metadata = ParquetReader.readMetadata('data.parquet');
console.log('Rows:', metadata.numRows);
console.log('Row groups:', metadata.numRowGroups);
console.table(metadata.schema);`
  },
  {
    id: 'append', title: '為既有檔案新增資料', short: '新增資料', category: 'basics',
    description: '沿用原有 schema，追加新的 row group。每次執行都會再新增一筆資料。',
    prerequisite: '先執行「建立檔案」。CLI 先用 Node.js 產生 more.json；此範例會修改 data.parquet。',
    outcome: '首次追加後，原始 3 筆資料會增加為 4 筆。', source: 'writer.ts',
    cli: `node -e "require('node:fs').writeFileSync('more.json', JSON.stringify([{id:4,name:'Dora',active:true}]))"
npx parquet-tool append data.parquet -i more.json
npx parquet-tool info data.parquet`,
    node: `const { ParquetWriter, ParquetReader } = require('parquet-tool');

ParquetWriter.withAppender('data.parquet', (writer) => {
  writer.write({ id: 4, name: 'Dora', active: true });
});
console.log(ParquetReader.readMetadata('data.parquet').numRows);`
  },
  {
    id: 'csv', title: 'CSV 與 Parquet 互轉', short: 'CSV 互轉', category: 'conversion',
    description: '匯入有標題列的 CSV，自動推斷欄位型別，再匯出成 CSV。',
    prerequisite: '下載 input.csv 到工作資料夾。不需要先建立 data.parquet。',
    outcome: '產生 from-csv.parquet 與 roundtrip.csv，保留 3 筆示例資料。', source: 'csv.ts',
    cli: `npx parquet-tool csv-to-parquet input.csv from-csv.parquet
npx parquet-tool parquet-to-csv from-csv.parquet roundtrip.csv`,
    node: `const { csvToParquet, parquetToCsv } = require('parquet-tool');

csvToParquet('input.csv', 'from-csv.parquet');
parquetToCsv('from-csv.parquet', 'roundtrip.csv');`
  },
  {
    id: 'json', title: 'JSON 與 Parquet 互轉', short: 'JSON 互轉', category: 'conversion',
    description: '從 JSON 陣列建立 Parquet，再匯出純資料列 JSON。省略 rows-only 時，預設會包含 schema。',
    prerequisite: '下載 input.json 到工作資料夾。不需要先建立 data.parquet。',
    outcome: '產生 from-json.parquet 與只包含資料陣列的 roundtrip.json。', source: 'json.ts',
    cli: `npx parquet-tool json-to-parquet input.json from-json.parquet
npx parquet-tool parquet-to-json from-json.parquet roundtrip.json --rows-only`,
    node: `const { jsonToParquet, parquetToJson } = require('parquet-tool');

jsonToParquet('input.json', 'from-json.parquet');
parquetToJson('from-json.parquet', 'roundtrip.json', {
  includeSchema: false,
});`
  },
  {
    id: 'arrow', title: 'Apache Arrow IPC 互轉', short: 'Arrow 互轉', category: 'conversion',
    description: '將 Parquet 轉成 Arrow IPC 檔案，再轉回 Parquet。',
    prerequisite: '先執行「建立檔案」，準備 data.parquet。',
    outcome: '產生 data.arrow 與 from-arrow.parquet。', source: 'arrow.ts',
    cli: `npx parquet-tool parquet-to-arrow data.parquet data.arrow
npx parquet-tool arrow-to-parquet data.arrow from-arrow.parquet`,
    node: `const { parquetToArrow, arrowToParquet } = require('parquet-tool');

parquetToArrow('data.parquet', 'data.arrow');
arrowToParquet('data.arrow', 'from-arrow.parquet');`
  },
  {
    id: 'xml', title: 'XML 與 Parquet 互轉', short: 'XML 互轉', category: 'conversion',
    description: '預設將 schema 和資料一起匯出，回讀時可還原原本的欄位型別。',
    prerequisite: '先執行「建立檔案」，準備 data.parquet。',
    outcome: '產生 data.xml 與 from-xml.parquet。', source: 'xml.ts',
    cli: `npx parquet-tool parquet-to-xml data.parquet data.xml
npx parquet-tool xml-to-parquet data.xml from-xml.parquet`,
    node: `const { parquetToXml, xmlToParquet } = require('parquet-tool');

parquetToXml('data.parquet', 'data.xml');
xmlToParquet('data.xml', 'from-xml.parquet');`
  },
  {
    id: 'split-merge', title: '切分檔案，再合併回來', short: '切分與合併', category: 'processing',
    description: '每個輸出檔案最多 2 筆資料，合併時預設檢查 schema 相容性。',
    prerequisite: '先重新執行「建立檔案」，讓 data.parquet 回到 3 筆資料。示例檔案輸出至目前工作資料夾。',
    outcome: '產生 2 個 demo_part 檔案，merged.parquet 合併後共有 3 筆資料。', source: 'splitter.ts',
    cli: `npx parquet-tool split data.parquet -n 2 -p demo
npx parquet-tool merge merged.parquet demo_part0000.parquet demo_part0001.parquet
npx parquet-tool validate merged.parquet`,
    node: `const {
  splitParquetFile, mergeParquetFiles, validateParquetFile,
} = require('parquet-tool');

const files = splitParquetFile('data.parquet', {
  maxRowsPerFile: 2,
  outputDir: '.',
  prefix: 'demo',
});
mergeParquetFiles(files, 'merged.parquet');
console.log(validateParquetFile('merged.parquet').valid);`
  },
  {
    id: 'validate', title: '驗證 Parquet 檔案', short: '驗證檔案', category: 'processing',
    description: '檢查檔案結構與 row group。CLI 遇到無效檔案會回傳非零結束碼，可用於腳本與 CI。',
    prerequisite: '先執行「建立檔案」，準備 data.parquet。',
    outcome: '有效的示例檔案顯示 Valid: yes；Node.js 顯示 true 與空的 issues。', source: 'validate.ts',
    cli: 'npx parquet-tool validate data.parquet',
    node: `const { validateParquetFile } = require('parquet-tool');

const report = validateParquetFile('data.parquet');
console.log(report.valid, report.issues);
if (!report.valid) {
  process.exitCode = 1;
}`
  },
  {
    id: 'projection', title: '只處理指定的欄位與 row group', short: '指定欄位與群組', category: 'processing',
    description: 'Node.js API 可選擇欄位和 row group。CLI 可指定 row group，但沒有欄位選擇選項。索引從 0 開始。',
    prerequisite: '先執行「建立檔案」，使用 rowGroupSize: 2 產生 data.parquet。',
    outcome: 'CLI 顯示第一個 row group 的完整資料；Node.js 顯示其中的 id、name 欄位。欄位投影在讀取 row group 後進行。', source: 'reader.ts',
    cli: 'npx parquet-tool read data.parquet --row-group 0 --json',
    node: `const { ParquetReader } = require('parquet-tool');

ParquetReader.withReader('data.parquet', (reader) => {
  for (const row of reader.iterateRows({
    columns: ['id', 'name'],
    rowGroups: [0],
  })) {
    console.log(row);
  }
});`
  },
  {
    id: 'buffer', title: '以 Node.js Buffer 傳遞檔案', short: 'Buffer 往返', category: 'processing',
    description: '讀取原始 Parquet 位元組，再寫入另一個檔案。這個操作不會將資料解碼成資料列。',
    prerequisite: '先執行「建立檔案」，準備 data.parquet。本功能使用 Node.js API。',
    outcome: '建立經過驗證的 restored.parquet。overwrite: true 允許重跑時覆寫。', source: 'buffer.ts',
    node: `const { parquetToBuffer, bufferToParquet } = require('parquet-tool');

const bytes = parquetToBuffer('data.parquet', { validate: true });
bufferToParquet(bytes, 'restored.parquet', {
  validate: true,
  overwrite: true,
});
console.log('Bytes:', bytes.length);`
  }
];
