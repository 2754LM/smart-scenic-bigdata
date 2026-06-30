/* ============================================================
   manage.html (业务管理) logic
   - 4 tabs: 景点 / 游客 / 消费 / 游玩 + 系统管理
   - 分页 + 过滤
   ============================================================ */

let currentTab = 'attractions';
let visitorPage = 1;
let consumptionPage = 1;
let visitsPage = 1;
let realtimeAutoRefresh = null;

document.addEventListener('DOMContentLoaded', () => {
  initPage('manage');
  document.querySelectorAll('#tab-bar button').forEach(btn => {
    btn.addEventListener('click', () => switchTab(btn.dataset.tab));
  });
  switchTab('attractions');

  // search handlers
  document.getElementById('v-search').addEventListener('click', () => { visitorPage = 1; loadVisitors(); });
  document.getElementById('c-search').addEventListener('click', () => { consumptionPage = 1; loadConsumption(); });
  document.getElementById('vr-search').addEventListener('click', () => { visitsPage = 1; loadVisits(); });
  document.getElementById('hb-search').addEventListener('click', loadHBase);
  document.getElementById('hb2-search')?.addEventListener('click', loadHBase2);
});

function switchTab(tab) {
  currentTab = tab;
  document.querySelectorAll('.tab-content').forEach(el => el.style.display = 'none');
  const target = document.getElementById(`tab-${tab}`);
  if (target) target.style.display = '';
  document.querySelectorAll('#tab-bar button').forEach(b => {
    if (b.dataset.tab === tab) b.classList.add('btn-primary');
    else b.classList.remove('btn-primary');
  });
  // lazy load
  if (tab === 'attractions') loadAttractions();
  if (tab === 'visitors')    loadVisitors();
  if (tab === 'consumption') loadConsumption();
  if (tab === 'visits')      loadVisits();
  if (tab === 'system' && window.loadAdmin) window.loadAdmin();
}

// ---- 景点 ----
async function loadAttractions() {
  const el = document.getElementById('table-attractions');
  renderLoading(el);
  try {
    const r = await API.attractions();
    renderTable(el, [
      { title: 'ID', key: '景点ID' },
      { title: '名称', key: '景点名称' },
      { title: '类型', key: '类型', render: r => `<span class="badge badge-blue">${escapeHtml(r.类型)}</span>` },
      { title: '位置', key: '位置' },
      { title: '开放时间', key: '开放时间' },
    ], r.data);
  } catch (e) { renderError(el, '加载失败'); }
}

// ---- 游客 ----
async function loadVisitors() {
  const el = document.getElementById('table-visitors');
  renderLoading(el);
  const params = { page: visitorPage, page_size: 30 };
  const gender = document.getElementById('v-gender').value;
  const minA = document.getElementById('v-min-age').value;
  const maxA = document.getElementById('v-max-age').value;
  if (gender) params.gender = gender;
  if (minA)   params.min_age = minA;
  if (maxA)   params.max_age = maxA;
  try {
    const r = await API.visitors(params);
    renderTable(el, [
      { title: 'ID', key: '游客ID' },
      { title: '姓名', key: '姓名' },
      { title: '性别', key: '性别' },
      { title: '年龄', key: '年龄' },
      { title: '地区', key: '地区' },
    ], r.items);
    renderPagination(document.getElementById('pagination-visitors'),
      r.page, r.page_size, r.total,
      (p) => { visitorPage = p; loadVisitors(); });
  } catch (e) { renderError(el, '加载失败'); }
}

// ---- 消费 ----
async function loadConsumption() {
  const el = document.getElementById('table-consumption');
  renderLoading(el);
  const params = { page: consumptionPage, page_size: 30 };
  const s = document.getElementById('c-start').value;
  const e = document.getElementById('c-end').value;
  const v = document.getElementById('c-visitor-id').value;
  const a = document.getElementById('c-attraction-id').value;
  if (s) params.start_date = s;
  if (e) params.end_date = e;
  if (v) params.visitor_id = v;
  if (a) params.attraction_id = a;
  try {
    const r = await API.consumption(params);
    renderTable(el, [
      { title: 'ID', key: '消费ID' },
      { title: '时间', key: '时间' },
      { title: '游客ID', key: '游客ID' },
      { title: '景点ID', key: '景点ID' },
      { title: '金额', key: '消费金额', render: r => fmtCNY(r.消费金额) },
    ], r.items);
    renderPagination(document.getElementById('pagination-consumption'),
      r.page, r.page_size, r.total,
      (p) => { consumptionPage = p; loadConsumption(); });
  } catch (e) { renderError(el, '加载失败'); }
}

// ---- 游玩 ----
async function loadVisits() {
  const el = document.getElementById('table-visits');
  renderLoading(el);
  const params = { page: visitsPage, page_size: 30 };
  const s = document.getElementById('vr-start').value;
  const e = document.getElementById('vr-end').value;
  const v = document.getElementById('vr-visitor-id').value;
  const a = document.getElementById('vr-attraction-id').value;
  if (s) params.start_date = s;
  if (e) params.end_date = e;
  if (v) params.visitor_id = v;
  if (a) params.attraction_id = a;
  try {
    const r = await API.visits(params);
    renderTable(el, [
      { title: 'ID', key: '记录ID' },
      { title: '时间', key: '时间' },
      { title: '游客ID', key: '游客ID' },
      { title: '景点ID', key: '景点ID' },
      { title: '时长(h)', key: '游玩时长', render: r => fmt(r.游玩时长, 2) },
    ], r.items);
    renderPagination(document.getElementById('pagination-visits'),
      r.page, r.page_size, r.total,
      (p) => { visitsPage = p; loadVisits(); });
  } catch (e) { renderError(el, '加载失败'); }
}

// ---- 实时流已迁移到 realtime.html ----
