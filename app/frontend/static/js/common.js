/* ============================================================
   Common utilities
   ============================================================ */

// Format number with thousand separator
function fmt(n, decimals = 0) {
  if (n === null || n === undefined) return '-';
  if (typeof n === 'string') n = parseFloat(n);
  if (isNaN(n)) return '-';
  return n.toLocaleString('zh-CN', { maximumFractionDigits: decimals, minimumFractionDigits: decimals });
}

function fmtCNY(n) { return '¥' + fmt(n, 2); }
function fmtPct(n) { return fmt(n, 1) + '%'; }
function fmtInt(n) { return fmt(n, 0); }

function escapeHtml(s) {
  if (s === null || s === undefined) return '';
  return String(s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

function debounce(fn, delay = 300) {
  let t = null;
  return function (...args) {
    clearTimeout(t);
    t = setTimeout(() => fn.apply(this, args), delay);
  };
}

// Render empty state
function renderEmpty(el, text = '暂无数据') {
  el.innerHTML = `<div class="empty"><div style="text-align:center"><div class="icon">📭</div><div>${escapeHtml(text)}</div></div></div>`;
}

// Render error state
function renderError(el, msg) {
  el.innerHTML = `<div class="empty"><div style="text-align:center;color:#ff6666"><div class="icon">⚠️</div><div>${escapeHtml(msg)}</div></div></div>`;
}

// Render loading
function renderLoading(el) {
  el.innerHTML = '<div class="empty"><span class="loading"></span>加载中...</div>';
}

// Header / nav (called by each page)
function renderHeader(active) {
  const links = [
    { key: 'overview',  href: 'index.html',    label: '总览大屏' },
    { key: 'analysis',  href: 'analysis.html', label: '数据分析' },
    { key: 'predict',   href: 'predict.html',  label: '模型预测' },
    { key: 'manage',    href: 'manage.html',   label: '业务管理' },
  ];
  return `
    <div class="app-header">
      <div class="logo">🎡 <span class="accent">智能景区</span>大数据平台</div>
      <nav>
        ${links.map(l => `<a href="${l.href}" class="${l.key === active ? 'active' : ''}">${l.label}</a>`).join('')}
      </nav>
      <div class="status" id="status-bar">
        <span><span class="dot ok"></span>MySQL</span>
        <span><span class="dot" id="dot-hive"></span>Hive</span>
        <span><span class="dot" id="dot-hbase"></span>HBase</span>
      </div>
    </div>
  `;
}

function renderFooter() {
  return `<div class="app-footer">© 2026 Smart Scenic BigData Platform · P2 Frontend</div>`;
}

// Update health dots
async function refreshHealth() {
  try {
    const r = await API.health();
    const hiveDot = document.getElementById('dot-hive');
    if (hiveDot) {
      hiveDot.classList.remove('ok', 'err');
      hiveDot.classList.add(r.data?.hive ? 'ok' : 'err');
    }
  } catch (e) {
    console.warn('health check fail', e);
  }
}

// Pagination
function renderPagination(el, page, pageSize, total, onChange) {
  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  el.innerHTML = `
    <button class="btn" ${page <= 1 ? 'disabled' : ''} data-pg="${page - 1}">上一页</button>
    <span class="info">第 ${page} / ${totalPages} 页 · 共 ${fmtInt(total)} 条</span>
    <button class="btn" ${page >= totalPages ? 'disabled' : ''} data-pg="${page + 1}">下一页</button>
  `;
  el.querySelectorAll('button[data-pg]').forEach(btn => {
    btn.addEventListener('click', () => onChange(parseInt(btn.dataset.pg)));
  });
}

// Generic table render
function renderTable(el, columns, rows) {
  if (!rows || rows.length === 0) { renderEmpty(el); return; }
  el.innerHTML = `
    <table>
      <thead><tr>${columns.map(c => `<th>${c.title}</th>`).join('')}</tr></thead>
      <tbody>
        ${rows.map(r => `<tr>${columns.map(c =>
          `<td>${c.render ? c.render(r) : escapeHtml(r[c.key])}</td>`
        ).join('')}</tr>`).join('')}
      </tbody>
    </table>
  `;
}

// Init a page (call after DOMContentLoaded)
function initPage(active) {
  document.body.insertAdjacentHTML('afterbegin', renderHeader(active));
  document.body.insertAdjacentHTML('beforeend', renderFooter());
  refreshHealth();
  setInterval(refreshHealth, 30000);
}
