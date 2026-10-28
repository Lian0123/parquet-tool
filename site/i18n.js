'use strict';
(() => {
  const data = JSON.parse(document.getElementById('locale-data')?.textContent || '{}');
  const messages = data.messages || {};
  window.siteLocale = data.language || document.documentElement.lang;
  window.siteTranslate = (value) => {
    if (messages[value]) return messages[value];
    if (/^(invalid parquet|invalid Parquet|truncated Parquet|Parquet column count mismatch|thrift:)/i.test(value)) return messages['Invalid Parquet file. Check that it is complete and uses a supported format.'] || 'Invalid Parquet file. Check that it is complete and uses a supported format.';
    const column = /^欄位「(.*)」(.+)$/.exec(value);
    if (column && messages[column[2]]) return (window.siteLocale === 'ja' ? `列「${column[1]}」` : `Column “${column[1]}”`) + messages[column[2]];
    if (value.startsWith('CSV 格式錯誤：') && messages['CSV 格式錯誤：']) return messages['CSV 格式錯誤：'] + value.slice(8);
    return value;
  };
})();
