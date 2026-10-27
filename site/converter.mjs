import { LIMITS, detectFormat } from './converter-shared.mjs';

const byId = (id) => document.getElementById(id);
const format = byId('output-format');
const input = byId('file-input');
const status = byId('status');
const error = byId('error');
let worker;
let currentFile;
let ready = false;
let phase = 'idle';
let resultUrl;
let generation = 0;

function size(bytes) {
  return bytes < 1024 ? `${bytes} B` : bytes < 1024 * 1024 ? `${(bytes / 1024).toFixed(1)} KiB` : `${(bytes / 1024 / 1024).toFixed(2)} MiB`;
}

function delimiter(id) { return byId(id).value === 'tab' ? '\t' : byId(id).value; }

function clearResult() {
  if (resultUrl) URL.revokeObjectURL(resultUrl);
  resultUrl = undefined;
  byId('download').removeAttribute('href');
  byId('result').hidden = true;
}

function showError(message) {
  error.textContent = message;
  error.hidden = false;
}

function setPhase(value, message) {
  phase = value;
  const busy = value === 'loading' || value === 'converting';
  byId('convert').disabled = busy || !ready;
  byId('cancel').hidden = !busy;
  for (const id of ['output-format', 'compression', 'output-delimiter', 'escape-formulae', 'input-delimiter', 'infer-types']) byId(id).disabled = busy;
  status.classList.toggle('busy', busy);
  byId('preview').setAttribute('aria-busy', String(busy));
  status.textContent = message;
}

function reset(message = '選擇檔案後，即可查看預覽並開始轉換。') {
  generation++;
  worker?.terminate();
  worker = undefined;
  currentFile = undefined;
  ready = false;
  clearResult();
  input.value = '';
  byId('preview').hidden = true;
  byId('file-info').hidden = true;
  byId('preview-head').replaceChildren();
  byId('preview-body').replaceChildren();
  error.hidden = true;
  for (const option of format.options) option.disabled = false;
  setPhase('idle', message);
}

function updateFormat() {
  clearResult();
  byId('parquet-options').hidden = format.value !== 'parquet';
  byId('csv-options').hidden = format.value !== 'csv';
  byId('json-options').hidden = format.value !== 'json';
  const descriptions = { parquet: '欄式儲存，適合資料分析與資料交換。', csv: '通用文字表格，可匯入試算表或其他資料工具。', json: '易讀的物件陣列，適合程式與 API 資料交換。' };
  byId('format-description').textContent = descriptions[format.value];
  if (ready) setPhase('ready', '資料已就緒，選擇格式後按「開始轉換」。');
}

function renderPreview(preview) {
  byId('preview-head').replaceChildren();
  byId('preview-body').replaceChildren();
  const header = document.createElement('tr');
  for (const column of preview.schema) {
    const cell = document.createElement('th');
    cell.scope = 'col';
    cell.textContent = column.name;
    const type = document.createElement('span');
    type.className = 'column-type';
    type.textContent = `${column.type} · ${column.nulls.toLocaleString()} null`;
    cell.append(type);
    header.append(cell);
  }
  byId('preview-head').append(header);
  for (const row of preview.rows) {
    const tr = document.createElement('tr');
    for (const column of preview.schema) {
      const td = document.createElement('td');
      const value = row[column.name];
      const text = value === null ? 'null' : String(value);
      td.textContent = text.length > 500 ? `${text.slice(0, 500)}…` : text;
      if (value === null) td.className = 'null-value';
      tr.append(td);
    }
    byId('preview-body').append(tr);
  }
  byId('table-stats').textContent = `${preview.rowCount.toLocaleString()} 列 × ${preview.schema.length} 欄 · ${preview.format.toUpperCase()}`;
  byId('preview').hidden = false;
}

function load(file) {
  if (input.disabled) return;
  reset();
  try {
    if (!file) return;
    const sourceFormat = detectFormat(file.name);
    if (file.size > LIMITS.bytes) throw new Error('檔案超過 20 MiB，請先切分後再轉換。');
    currentFile = file;
    byId('filename').textContent = file.name;
    byId('filesize').textContent = `${size(file.size)} · ${sourceFormat.toUpperCase()}`;
    byId('file-info').hidden = false;
    if (format.value === sourceFormat) format.value = sourceFormat === 'parquet' ? 'json' : 'parquet';
    for (const option of format.options) option.disabled = option.value === sourceFormat;
    updateFormat();
    setPhase('loading', '正在本機讀取與辨識資料…');
    const job = generation;
    worker = new Worker(new URL('./converter-worker.mjs', import.meta.url), { type: 'module' });
    worker.onmessage = ({ data }) => {
      if (job !== generation) return;
      if (data.action === 'loaded') {
        ready = true;
        renderPreview(data.preview);
        setPhase('ready', '資料已就緒，預覽確認後即可轉換。');
      } else if (data.action === 'converted') {
        clearResult();
        resultUrl = URL.createObjectURL(new Blob([data.bytes], { type: data.mime }));
        const filename = `${currentFile.name.replace(/\.[^.]+$/, '')}-converted.${data.extension}`;
        byId('download').href = resultUrl;
        byId('download').download = filename;
        byId('result-info').textContent = `${filename} · ${size(data.bytes.byteLength)}`;
        byId('result').hidden = false;
        setPhase('ready', '轉換完成。按「下載檔案」儲存結果。');
      } else if (data.action === 'error') {
        if (phase === 'loading') ready = false;
        setPhase('error', '處理未完成，請檢查下方錯誤訊息。');
        showError(data.message);
      }
    };
    worker.onerror = (event) => {
      if (job !== generation) return;
      event.preventDefault();
      worker.terminate();
      ready = false;
      setPhase('error', '轉換引擎無法執行。');
      showError('請重新選擇檔案再試一次，並確認使用支援 JavaScript 模組與 Web Worker 的新版瀏覽器。');
    };
    worker.postMessage({ action: 'load', file, options: { delimiter: delimiter('input-delimiter'), infer: byId('infer-types').checked } });
  } catch (failure) {
    ready = false;
    setPhase('error', '無法讀取檔案。');
    showError(failure.message);
  }
}

input.addEventListener('change', () => load(input.files[0]));
byId('clear').addEventListener('click', () => reset());
byId('cancel').addEventListener('click', () => reset('已取消處理並清除檔案。'));
byId('sample').addEventListener('click', () => load(new File([JSON.stringify([
  { id: 1, name: 'Alice', active: true }, { id: 2, name: 'Bob', active: false }, { id: 3, name: 'Carol', active: true },
])], 'example.json', { type: 'application/json' })));
format.addEventListener('change', updateFormat);
for (const id of ['compression', 'output-delimiter', 'escape-formulae']) byId(id).addEventListener('change', () => {
  clearResult();
  error.hidden = true;
  if (ready) setPhase('ready', '選項已更新，請重新轉換。');
});
for (const id of ['input-delimiter', 'infer-types']) byId(id).addEventListener('change', () => {
  if (currentFile && detectFormat(currentFile.name) === 'csv') load(currentFile);
});
byId('convert').addEventListener('click', () => {
  if (!ready || phase === 'converting') return;
  clearResult();
  error.hidden = true;
  setPhase('converting', '正在瀏覽器背景轉換，請稍候…');
  worker.postMessage({ action: 'convert', format: format.value, options: {
    compression: byId('compression').value, delimiter: delimiter('output-delimiter'), escapeFormulae: byId('escape-formulae').checked,
  } });
});

const dropZone = byId('drop-zone');
let dragDepth = 0;
for (const type of ['dragenter', 'dragover']) dropZone.addEventListener(type, (event) => {
  event.preventDefault();
  if (type === 'dragenter') dragDepth++;
  dropZone.classList.add('dragover');
});
dropZone.addEventListener('dragleave', () => {
  dragDepth--;
  if (dragDepth <= 0) dropZone.classList.remove('dragover');
});
dropZone.addEventListener('drop', (event) => {
  event.preventDefault();
  dragDepth = 0;
  dropZone.classList.remove('dragover');
  if (event.dataTransfer.files.length !== 1) { showError('請一次選擇一個檔案。'); return; }
  load(event.dataTransfer.files[0]);
});
// Prevent dropping a file outside the target from navigating away from the tool.
window.addEventListener('dragover', (event) => event.preventDefault());
window.addEventListener('drop', (event) => event.preventDefault());
// Reset state as well as resources: a back/forward cache restore must not reuse a terminated worker.
window.addEventListener('pagehide', () => reset());
if (location.protocol === 'file:' || !('Worker' in window)) {
  byId('environment-error').hidden = false;
  byId('environment-error').textContent = '請透過 GitHub Pages 或本機 HTTP 伺服器開啟此頁，並使用支援 Web Worker 的新版瀏覽器。直接開啟 HTML 檔案無法執行轉換。';
  input.disabled = true;
  byId('sample').disabled = true;
}
