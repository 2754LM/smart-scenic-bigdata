/* ============================================================
   predict.html (模型预测) logic
   - 3 prediction types with different forms
   - Model comparison tables
   ============================================================ */

document.addEventListener('DOMContentLoaded', async () => {
  initPage('predict');

  const typeSel = document.getElementById('pred-type');
  typeSel.addEventListener('change', switchForm);
  switchForm();

  document.getElementById('btn-predict').addEventListener('click', doPredict);
  await Promise.all([loadRegressionTable(), loadClassificationTable(), loadClusteringTable()]);
});

function switchForm() {
  const t = document.getElementById('pred-type').value;
  document.getElementById('form-consumption').style.display = t === 'consumption_amount' ? '' : 'none';
  document.getElementById('form-visitor').style.display     = t === 'daily_visitor' ? '' : 'none';
  document.getElementById('form-highvalue').style.display   = t === 'high_value_visitor' ? '' : 'none';
}

async function doPredict() {
  const t = document.getElementById('pred-type').value;
  const el = document.getElementById('pred-result');
  let features = {};
  if (t === 'consumption_amount') {
    features = {
      类型:     document.getElementById('c-type').value,
      月份:     +document.getElementById('c-month').value,
      星期:     +document.getElementById('c-weekday').value,
      小时:     +document.getElementById('c-hour').value,
      是否周末: +document.getElementById('c-weekend').value,
      是否节假日: +document.getElementById('c-holiday').value,
    };
  } else if (t === 'daily_visitor') {
    features = {
      month:    +document.getElementById('v-month').value,
      weekday:  +document.getElementById('v-weekday').value,
      dayofyear:+document.getElementById('v-dayofyear').value,
      is_weekend: +document.getElementById('v-weekend').value,
      is_holiday: +document.getElementById('v-holiday').value,
    };
  } else {
    features = {
      年龄:    +document.getElementById('h-age').value,
      性别:    document.getElementById('h-gender').value,
      偏好类型: document.getElementById('h-pref').value,
      游玩次数: +document.getElementById('h-visits').value,
      平均消费: +document.getElementById('h-avg-consume').value,
    };
  }
  renderLoading(el);
  try {
    const r = await API.predict(t, features);
    const d = r.data;
    el.innerHTML = `
      <div class="result-card">
        <div style="font-size: 13px; color: #6c7a96; margin-bottom: 8px">预测结果</div>
        <div class="value">
          ${d.type === 'consumption_amount' ? fmtCNY(d.prediction) :
            d.type === 'daily_visitor' ? fmtInt(d.prediction) + ' 人' :
            (d.label || (d.prediction ? '高价值' : '普通'))}
        </div>
        ${d.probability ? `<div style="margin-top:8px;color:#b0c4de">置信概率: <span class="text-accent">${fmtPct(d.probability * 100)}</span></div>` : ''}
        <div style="margin-top: 8px; font-size: 12px; color: #6c7a96">
          模型: ${escapeHtml(d.model)} · 时间: ${escapeHtml(d.timestamp || '')}
        </div>
      </div>
    `;
  } catch (e) { renderError(el, '预测失败: ' + e.message); }
}

async function loadRegressionTable() {
  const el = document.getElementById('table-regression');
  renderLoading(el);
  try {
    const r = await API.predictRegression();
    if (!r.data) { renderEmpty(el, r.message || '暂无报告'); return; }
    const results = r.data.results || [];
    const grouped = {};
    results.forEach(x => { grouped[x.task] = grouped[x.task] || []; grouped[x.task].push(x); });
    let html = '';
    for (const [task, rows] of Object.entries(grouped)) {
      html += `<div style="margin-bottom:12px"><b>${escapeHtml(task)}</b></div>`;
      html += `
        <table>
          <thead><tr><th>模型</th><th>RMSE</th><th>MAE</th><th>R²</th></tr></thead>
          <tbody>
            ${rows.map(x => `<tr>
              <td>${escapeHtml(x.model)}</td>
              <td>${fmt(x.rmse, 4)}</td>
              <td>${fmt(x.mae, 4)}</td>
              <td>${fmt(x.r2, 4)}</td>
            </tr>`).join('')}
          </tbody>
        </table>
      `;
    }
    el.innerHTML = html || '<div class="empty">暂无数据</div>';
  } catch (e) { renderError(el, '加载失败'); }
}

async function loadClassificationTable() {
  const el = document.getElementById('table-classification');
  renderLoading(el);
  try {
    const r = await API.predictClassification();
    if (!r.data) { renderEmpty(el, r.message || '暂无报告'); return; }
    const rows = r.data.results || [];
    el.innerHTML = `
      <table>
        <thead><tr><th>模型</th><th>Accuracy</th><th>Precision</th><th>Recall</th><th>F1</th><th>AUC</th></tr></thead>
        <tbody>
          ${rows.map(x => `<tr>
            <td>${escapeHtml(x.model)}</td>
            <td>${fmt(x.accuracy, 4)}</td>
            <td>${fmt(x.precision, 4)}</td>
            <td>${fmt(x.recall, 4)}</td>
            <td>${fmt(x.f1, 4)}</td>
            <td>${fmt(x.auc, 4)}</td>
          </tr>`).join('')}
        </tbody>
      </table>
    `;
  } catch (e) { renderError(el, '加载失败'); }
}

async function loadClusteringTable() {
  const el = document.getElementById('table-clustering');
  renderLoading(el);
  try {
    const r = await API.predictClustering();
    if (!r.data) { renderEmpty(el, r.message || '暂无报告'); return; }
    const stats = r.data.cluster_stats || [];
    el.innerHTML = `
      <table>
        <thead><tr>
          <th>Cluster</th><th>人数</th><th>平均年龄</th>
          <th>平均总消费</th><th>平均单次消费</th><th>平均游玩次数</th><th>平均时长(h)</th>
        </tr></thead>
        <tbody>
          ${stats.map(s => `<tr>
            <td><span class="badge badge-purple">#${s.cluster}</span></td>
            <td>${fmtInt(s.n)}</td>
            <td>${fmt(s.avg_age, 1)}</td>
            <td>${fmt(s.avg_total_consume, 1)}</td>
            <td>${fmt(s.avg_per_consume, 1)}</td>
            <td>${fmt(s.avg_visit_count, 1)}</td>
            <td>${fmt(s.avg_duration_h, 2)}</td>
          </tr>`).join('')}
        </tbody>
      </table>
    `;
  } catch (e) { renderError(el, '加载失败'); }
}
