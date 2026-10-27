export const site = {
  baseUrl: 'https://lian0123.github.io/parquet-tool/',
  name: 'parquet-tool',
  language: 'zh-Hant',
  reviewed: '2026-10-27',
  repository: 'https://github.com/Lian0123/parquet-tool',
};

export const pages = [
  {
    file: 'index.html',
    title: 'Parquet 工具與使用範例：Node.js、CLI、網頁轉換 | parquet-tool',
    description: 'parquet-tool 提供 Parquet 讀寫、格式轉換與資料處理範例。瀏覽 12 個 Node.js／CLI 任務，或直接在瀏覽器互轉 CSV、JSON、Parquet，不需上傳檔案。',
    label: 'Parquet 工具與使用範例',
    faqs: [
      ['parquet-tool 是什麼？', 'parquet-tool 是 Node.js 18 以上可用的 TypeScript 函式庫與 CLI，核心讀寫使用 C++ 原生模組。這個網站另提供獨立的瀏覽器轉換工具，以及 12 個讀寫、轉換與資料處理範例。'],
      ['網頁版可以轉換哪些格式？', '網頁版支援 CSV、JSON、平面 Parquet 三種格式互轉，共六個方向。Arrow IPC 與 XML 轉換請使用 Node.js／CLI 範例。'],
      ['使用網頁轉換工具需要安裝 Node.js 嗎？', '不需要。使用支援 JavaScript 模組與 Web Worker 的現代瀏覽器，開啟轉換頁即可。教學中的 Node.js 與 CLI 程式碼則需要在本機安裝 Node.js 18 以上及 parquet-tool。'],
    ],
  },
  {
    file: 'converter.html',
    title: 'Parquet 線上轉換工具：CSV、JSON 互轉，檔案不上傳 | parquet-tool',
    description: '免費在瀏覽器互轉 CSV、JSON、Parquet，支援拖放選檔、資料預覽與結果下載。檔案只在本機處理，不上傳伺服器。支援單檔 20 MiB、最多 100,000 列的平面資料表。',
    label: 'CSV、JSON、Parquet 線上轉換工具',
    faqs: [
      ['如何將 CSV 轉成 Parquet？', '選擇或拖入含標題列的 UTF-8 CSV，確認資料預覽，將輸出格式設為 Parquet，再按「開始轉換」與「下載檔案」。可選擇 Snappy 或不壓縮。'],
      ['如何將 Parquet 轉成 JSON 或 CSV？', '選擇平面 Parquet 檔案，預覽資料後，將輸出格式設為 JSON 或 CSV，再開始轉換並下載。CSV 可選擇逗號、分號或 Tab 分隔；JSON 輸出為物件陣列。'],
      ['檔案會上傳到伺服器嗎？', '不會。檔案讀取、資料預覽與轉換都在你的瀏覽器 Web Worker 中執行。轉換引擎隨網站載入，不透過第三方轉換服務傳送檔案。'],
      ['支援多大的檔案？', '單檔最多 20 MiB，最多 100,000 列、200 欄與 2,000,000 個儲存格。目前支援平面資料表，暫不支援巢狀或二進位欄位。壓縮檔案解碼後會使用更多記憶體，可隨時取消處理。'],
      ['大整數、空值與混合型別如何處理？', 'Parquet INT64 在 JSON／CSV 輸出為十進位字串。JSON 超出安全精度的數值會被拒絕，請改用帶引號的字串。CSV 空欄位與 JSON 缺少的欄位轉為 null；混合型別欄位以文字保存，日期轉為 ISO 文字。'],
      ['網頁版的 Parquet 可以直接與目前 CLI 互用嗎？', '目前 C++ CLI 與網頁使用的標準 Parquet 引擎存在互通限制，請透過 CSV 或 JSON 在兩者之間交換資料。網頁版不保留原檔的完整 schema、metadata 或所有 logical types。'],
    ],
  },
  {
    file: 'guide.html',
    title: 'Parquet 使用教學：12 個 Node.js 與 CLI 完整範例 | parquet-tool',
    description: '完整 Parquet 使用教學：建立、讀取、追加、中繼資料、CSV／JSON／Arrow／XML 互轉、切分合併、驗證、欄位投影與 Buffer。附前置需求及可複製的 Node.js、CLI 程式碼。',
    label: 'Node.js 與 CLI 完整教學',
    faqs: [],
  },
];
