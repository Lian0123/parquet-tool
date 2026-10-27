'use strict';

(() => {
  const recipes = window.PARQUET_EXAMPLES;
  const categories = { basics: '基本讀寫', conversion: '格式轉換', processing: '資料處理' };
  const byId = (id) => document.getElementById(id);
  const search = byId('search');
  const category = byId('category');
  const list = byId('recipe-list');
  const tabs = [...document.querySelectorAll('[data-language]')];
  let selected = recipes[0];
  let language = 'cli';
  let toastTimer;

  function showCode() {
    if (!selected[language]) language = 'node';
    for (const tab of tabs) {
      const active = tab.dataset.language === language;
      tab.disabled = !selected[tab.dataset.language];
      tab.setAttribute('aria-selected', String(active));
      tab.tabIndex = active ? 0 : -1;
    }
    byId('code-panel').setAttribute('aria-labelledby', `tab-${language}`);
    byId('recipe-code').textContent = selected[language];
    byId('code-hint').textContent = language === 'cli'
      ? '在工作資料夾的終端機執行。'
      : '儲存為 example.cjs，再執行 node example.cjs。';
    const source = language === 'cli' ? 'src/cli/index.ts' : `src/lib/${selected.source}`;
    byId('source-link').href = `https://github.com/Lian0123/parquet-tool/blob/main/${source}`;
  }

  function selectRecipe(recipe, updateHash = true) {
    selected = recipe;
    byId('recipe-category').textContent = categories[recipe.category];
    byId('recipe-title').textContent = recipe.title;
    byId('recipe-description').textContent = recipe.description;
    byId('prerequisite').textContent = `準備：${recipe.prerequisite}`;
    byId('recipe-outcome').textContent = recipe.outcome;
    byId('permalink').href = `#example-${recipe.id}`;
    for (const button of list.querySelectorAll('button')) {
      button.setAttribute('aria-current', String(button.dataset.recipe === recipe.id));
    }
    showCode();
    if (updateHash) {
      // replaceState keeps recipe changes from adding a scroll jump or history entry.
      try { history.replaceState(null, '', `#example-${recipe.id}`); } catch { /* file:// preview */ }
    }
  }

  function renderList() {
    const query = search.value.trim().toLocaleLowerCase();
    const visible = recipes.filter((recipe) => {
      const matchesCategory = category.value === 'all' || category.value === recipe.category;
      const text = [recipe.title, recipe.short, recipe.description, recipe.cli, recipe.node,
        categories[recipe.category]].join(' ').toLocaleLowerCase();
      return matchesCategory && text.includes(query);
    });
    list.replaceChildren();
    byId('result-count').textContent = `${visible.length} / ${recipes.length} 個範例`;
    byId('empty-state').hidden = visible.length !== 0;
    byId('recipe-detail').hidden = visible.length === 0;
    for (const recipe of visible) {
      const button = document.createElement('button');
      button.type = 'button';
      button.className = 'recipe-item';
      button.dataset.recipe = recipe.id;
      const number = document.createElement('span');
      number.className = 'recipe-number';
      number.textContent = String(recipes.indexOf(recipe) + 1).padStart(2, '0');
      number.setAttribute('aria-hidden', 'true');
      const label = document.createElement('span');
      label.textContent = recipe.short;
      const arrow = document.createElement('span');
      arrow.className = 'recipe-chevron';
      arrow.textContent = '↗';
      arrow.setAttribute('aria-hidden', 'true');
      button.append(number, label, arrow);
      button.addEventListener('click', () => selectRecipe(recipe));
      list.append(button);
    }
    if (visible.length) {
      selectRecipe(visible.find((recipe) => recipe.id === selected.id) || visible[0], false);
    }
  }

  function readHash() {
    const recipe = recipes.find((item) => `#example-${item.id}` === location.hash);
    if (!recipe) return;
    selected = recipe;
    search.value = '';
    category.value = 'all';
    renderList();
    byId('examples').scrollIntoView({ behavior: 'instant', block: 'start' });
  }

  for (const tab of tabs) {
    tab.addEventListener('click', () => {
      language = tab.dataset.language;
      showCode();
    });
    tab.addEventListener('keydown', (event) => {
      const enabled = tabs.filter((item) => !item.disabled);
      const index = enabled.indexOf(tab);
      let next;
      if (event.key === 'ArrowRight') next = enabled[(index + 1) % enabled.length];
      if (event.key === 'ArrowLeft') next = enabled[(index + enabled.length - 1) % enabled.length];
      if (event.key === 'Home') next = enabled[0];
      if (event.key === 'End') next = enabled[enabled.length - 1];
      if (next) {
        event.preventDefault();
        next.click();
        next.focus();
      }
    });
  }
  search.addEventListener('input', renderList);
  category.addEventListener('change', renderList);
  window.addEventListener('hashchange', readHash);
  byId('copy').hidden = false;
  byId('copy').addEventListener('click', async () => {
    clearTimeout(toastTimer);
    try {
      await navigator.clipboard.writeText(selected[language]);
      byId('copy-status').textContent = '已複製程式碼';
    } catch {
      const selection = window.getSelection();
      const range = document.createRange();
      range.selectNodeContents(byId('recipe-code'));
      selection.removeAllRanges();
      selection.addRange(range);
      byId('copy-status').textContent = '請按 Ctrl+C 或 ⌘C 複製已選取的程式碼';
    }
    toastTimer = setTimeout(() => { byId('copy-status').textContent = ''; }, 5000);
  });
  renderList();
  readHash();
})();
