/* ============================================================
   manage.html (业务管理) logic
   - 5 tabs: 景点 / 游客 / 消费 / 游玩 / HBase实时
   - 分页 + 过滤
   ============================================================ */

let currentTab = 'attractions';
let visitorPage = 1;
let consumptionPage = 1;
let visitsPage = 1;

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

  // Kafka 实时流
  const pr = document.getElementById('kafka-publish-review');
  if (pr) pr.addEventListener('click', publishReview);
  const pe = document.getElementById('kafka-publish-event');
  if (pe) pe.addEventListener('click', publishEvent);
  const pt = document.getElementById('kafka-task-trigger');
  if (pt) pt.addEventListener('click', triggerKafkaTask);
  const rb = document.getElementById('kafka-refresh');
  if (rb) rb.addEventListener('click', refreshKafkaAll);

  refreshKafkaAll();
});

function switchTab(tab) {
  currentTab = tab;
  document.querySelectorAll('.tab-content').forEach(el => el.style.display = 'none');
  document.getElementById(`tab-${tab}`).style.display = '';
  document.querySelectorAll('#tab-bar button').forEach(b => {
    if (b.dataset.tab === tab) b.classList.add('btn-primary');
    else b.classList.remove('btn-primary');
  });
  // lazy load
  if (tab === 'attractions') loadAttractions();
  if (tab === 'visitors')    loadVisitors();
  if (tab === 'consumption') loadConsumption();
  if (tab === 'visits')      loadVisits();
  if (tab === 'kafka')       refreshKafkaAll();
  if (tab === 'hbase')       loadHBase();
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

// ---- HBase ----
async function loadHBase() {
  const el = document.getElementById('hbase-result');
  const type = document.getElementById('hb-type').value;
  const id = +document.getElementById('hb-id').value;
  renderLoading(el);
  try {
    let r;
    if (type === 'visitor') {
      if (!id) { renderError(el, '请输入游客ID'); return; }
      r = await API.visitorProfile(id);
    } else if (type === 'attraction') {
      if (!id) { renderError(el, '请输入景点ID'); return; }
      r = await API.attractionStat(id);
    } else {
      r = await API.visitRecent(20);
    }
    if (!r.data) { renderEmpty(el, '暂无数据'); return; }
    el.innerHTML = `<pre class="text-mono" style="white-space: pre-wrap; word-break: break-all; font-size: 12px">${escapeHtml(JSON.stringify(r.data, null, 2))}</pre>`;
  } catch (e) { renderError(el, '查询失败: ' + e.message); }
}

// ---- Kafka 实时流 ----
async function publishReview() {
  const body = {
    visitor_id:   document.getElementById('kafka-vid').value,
    attraction_id: document.getElementById('kafka-aid').value,
    rating:       parseInt(document.getElementById('kafka-rating').value),
    comment:      document.getElementById('kafka-comment').value,
  };
  const div = document.getElementById('kafka-task-result');
  renderLoading(div);
  try {
    const r = await API.publishReview(body);
    div.innerHTML = `<span class="text-success">✓ 评论已发布 (via ${r.via || 'kafka'})</span> · ${escapeHtml(JSON.stringify(r.kafka_meta || {}))}`;
    refreshKafkaAll();
  } catch (e) {
    renderError(div, '发布失败: ' + e.message);
  }
}

async function publishEvent() {
  const body = {
    visitor_id:   document.getElementById('kafka-ev-vid').value,
    attraction_id: document.getElementById('kafka-ev-aid').value,
    event_type:   document.getElementById('kafka-ev-type').value,
  };
  const div = document.getElementById('kafka-task-result');
  renderLoading(div);
  try {
    const r = await API.publishEvent(body);
    div.innerHTML = `<span class="text-success">✓ 事件已发布 (via ${r.via || 'kafka'})</span> · ${escapeHtml(JSON.stringify(r.kafka_meta || {}))}`;
    refreshKafkaAll();
  } catch (e) {
    renderError(div, '发布失败: ' + e.message);
  }
}

async function triggerKafkaTask() {
  const taskType = document.getElementById('kafka-task-type').value;
  const count = parseInt(document.getElementById('kafka-task-count').value);
  const aidRaw = document.getElementById('kafka-task-aid').value;
  const body = {
    task_type: taskType,
    count: count,
    attraction_id: aidRaw ? parseInt(aidRaw) : null,
  };
  const div = document.getElementById('kafka-task-result');
  renderLoading(div);
  try {
    const r = await fetch(window.API_BASE + '/api/realtime/task/trigger', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body),
    }).then(r => r.json());
    div.innerHTML = `<span class="text-success">⚡ ${r.events_published} 个事件已发布到 Kafka</span><br>
      <span style="color:#9ca3af;font-size:12px">${escapeHtml(r.kafka_status || '')}</span>`;
    setTimeout(refreshKafkaAll, 2000);
  } catch (e) {
    renderError(div, '触发失败: ' + e.message);
  }
}

async function refreshKafkaAll() {
  // status
  try {
    const r = await API.kafkaStatus();
    const el = document.getElementById('kafka-status');
    if (el) el.textContent = JSON.stringify(r, null, 2);
  } catch (e) { console.error(e); }
  // hbase data
  try {
    const r = await API.visitRecent(20);
    const div = document.getElementById('kafka-hbase-data');
    if (div && r.data && r.data.length) {
      let html = '<table style="width:100%; border-collapse:collapse; margin-top:10px">';
      html += '<tr><th>row_key</th><th>visitor</th><th>attraction</th><th>action</th><th>ts</th></tr>';
      r.data.forEach(d => {
        html += '<tr><td>' + escapeHtml(d.row_key || '') + '</td>';
        html += '<td>' + escapeHtml(d.visitor_id || '') + '</td>';
        html += '<td>' + escapeHtml(d.scenic_id || d.attraction_id || '') + '</td>';
        html += '<td>' + escapeHtml(d.action || d.event_type || '') + '</td>';
        html += '<td>' + escapeHtml(d.ts || '') + '</td></tr>';
      });
      html += '</table>';
      div.innerHTML = html;
    } else if (div) {
      div.innerHTML = '<p style="color:#9ca3af">暂无数据</p>';
    }
  } catch (e) { console.error(e); }
}
