/* ============================================================
   predict.html (模型预测) logic
   - 4 prediction types
   - Model comparison tables
   - 真实 vs 预测 daily-compare line chart (echarts)
   ============================================================ */

document.addEventListener('DOMContentLoaded', async () => {
  initPage('predict');

  const typeSel = document.getElementById('pred-type');
  typeSel.addEventListener('change', switchForm);
  switchForm();

  document.getElementById('btn-predict').addEventListener('click', doPredict);
  document.getElementById('btn-compare').addEventListener('click', loadCompareChart);

  await Promise.all([
    loadRegressionTable(),
    loadClassificationTable(),
    loadClusteringTable(),
    loadCompareChart(),
  ]);
});

function switchForm() {
  // 所有任务都用同一组 6 个特征，无需切换表单
}

async function doPredict() {
  const t = document.getElementById('pred-type').value;
  const el = document.getElementById('pred-result');
  let features = {};
  if (t === 'consumption_amount') {
    // 直接用 Spark 训练的 6 个特征（不要再从其他字段推导）
    features = {
      age:                +document.getElementById('c-age').value,
      purchase_count:     +document.getElementById('c-pc').value,
      avg_amount:         +document.getElementById('c-avg').value,
      visit_count:        +document.getElementById('c-vc').value,
      avg_duration:       +document.getElementById('c-dur').value,
      unique_attractions: +document.getElementById('c-ua').value,
    };
  } else if (t === 'daily_visitor') {
    features = {
      age:                +document.getElementById('c-age').value,
      purchase_count:     +document.getElementById('c-pc').value,
      avg_amount:         +document.getElementById('c-avg').value,
      visit_count:        +document.getElementById('c-vc').value,
      avg_duration:       +document.getElementById('c-dur').value,
      unique_attractions: +document.getElementById('c-ua').value,
    };
  } else if (t === 'high_value_visitor') {
    features = {
      age:                +document.getElementById('c-age').value,
      purchase_count:     +document.getElementById('c-pc').value,
      avg_amount:         +document.getElementById('c-avg').value,
      visit_count:        +document.getElementById('c-vc').value,
      avg_duration:       +document.getElementById('c-dur').value,
      unique_attractions: +document.getElementById('c-ua').value,
    };
  } else if (t === 'cluster') {
    features = {
      age:                +document.getElementById('c-age').value,
      purchase_count:     +document.getElementById('c-pc').value,
      avg_amount:         +document.getElementById('c-avg').value,
      visit_count:        +document.getElementById('c-vc').value,
      avg_duration:       +document.getElementById('c-dur').value,
      unique_attractions: +document.getElementById('c-ua').value,
    };
  }
  renderLoading(el);
  try {
    const r = await API.predict(t, features);
    const d = r.data;
    let valText;
    if (d.type === 'consumption_amount') valText = fmtCNY(d.prediction);
    else if (d.type === 'daily_visitor') valText = fmtInt(d.prediction) + ' 人';
    else if (d.type === 'cluster') valText = `${d.label || '聚类' + d.cluster} (#${d.cluster})`;
    else valText = d.label || (d.prediction > 0.5 ? '高价值' : '普通');

    el.innerHTML = `
      <div class="result-card">
        <div style="font-size: 13px; color: #b0c4de; margin-bottom: 8px">预测结果</div>
        <div class="value">${valText}</div>
        ${d.probability != null ? `<div style="margin-top:8px;color:#b0c4de">置信概率: <span class="text-accent">${fmtPct(d.probability * 100)}</span></div>` : ''}
        ${d.tip ? `<div style="margin-top:8px;color:#9bc995;font-size:13px">💡 ${escapeHtml(d.tip)}</div>` : ''}
        <div style="margin-top: 8px; font-size: 12px; color: #b0c4de">
          模型: ${escapeHtml(d.model || '')} · 引擎: ${escapeHtml(d.engine || '')} · ${d.elapsed_ms != null ? d.elapsed_ms + 'ms' : ''}
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
    const results = (r.data && r.data.results) || [];
    if (!results.length) { renderEmpty(el, '暂无报告'); return; }
    el.innerHTML = `
      <table>
        <thead><tr><th>模型</th><th>RMSE</th><th>R²</th></tr></thead>
        <tbody>
          ${results.map(x => `<tr>
            <td>${escapeHtml(x.model)}</td>
            <td>${fmt(x.rmse, 4)}</td>
            <td>${fmt(x.r2, 4)}</td>
          </tr>`).join('')}
        </tbody>
      </table>
    `;
  } catch (e) { renderError(el, '加载失败'); }
}

async function loadClassificationTable() {
  const el = document.getElementById('table-classification');
  renderLoading(el);
  try {
    const r = await API.predictClassification();
    const rows = (r.data && r.data.results) || [];
    if (!rows.length) { renderEmpty(el, '暂无报告'); return; }
    el.innerHTML = `
      <table>
        <thead><tr><th>模型</th><th>Accuracy</th><th>F1</th></tr></thead>
        <tbody>
          ${rows.map(x => `<tr>
            <td>${escapeHtml(x.model)}</td>
            <td>${fmt(x.accuracy, 4)}</td>
            <td>${fmt(x.f1, 4)}</td>
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
    const stats = (r.data && r.data.cluster_stats) || [];
    if (!stats.length) { renderEmpty(el, '暂无聚类'); return; }
    const profiles = [
      '低频低消费游客 - 推送优惠券',
      '高频中消费游客 - 推荐热门景点',
      '中频高消费游客 - 推荐 VIP',
      '高频高消费游客 - 专属管家服务',
    ];
    el.innerHTML = `
      <table>
        <thead><tr>
          <th>Cluster</th><th>人群特征</th><th>人数</th><th>平均年龄</th>
          <th>平均总消费</th><th>平均游玩次数</th><th>平均时长(h)</th>
        </tr></thead>
        <tbody>
          ${stats.map(s => `<tr>
            <td><span class="badge badge-purple">#${s.cluster}</span></td>
            <td style="color:#9bc995">${profiles[s.cluster] || '聚类' + s.cluster}</td>
            <td>${fmtInt(s.n)}</td>
            <td>${fmt(s.avg_age, 1)}</td>
            <td>${fmt(s.avg_total_consume, 0)}</td>
            <td>${fmt(s.avg_visit_count, 1)}</td>
            <td>${fmt(s.avg_duration_h, 2)}</td>
          </tr>`).join('')}
        </tbody>
      </table>
    `;
  } catch (e) { renderError(el, '加载失败'); }
}

async function loadCompareChart() {
  const el = document.getElementById('chart-compare');
  const infoEl = document.getElementById('cmp-info');
  const start = document.getElementById('cmp-start').value;
  const end   = document.getElementById('cmp-end').value;
  const split = document.getElementById('cmp-split').value;

  renderLoading(el);
  try {
    const r = await fetch(`${window.API_BASE || 'http://localhost:8000'}/api/analysis/daily-compare?start=${start}&end=${end}&split_date=${split}`).then(r => r.json());
    if (r.error) { renderError(el, r.error); return; }
    const data = r.results || [];
    if (!data.length) { renderEmpty(el, '暂无数据'); return; }

    infoEl.textContent = `训练 ${r.train_days} 天 + 测试 ${r.test_days} 天 = ${r.total_days} 天`;

    // 真实 vs 预测 (echarts line)
    const dates = data.map(d => d.date);
    const actual = data.map(d => +d.actual_amount);
    const predicted = data.map(d => +d.predicted_amount);

    // split mark line
    const splitIdx = data.findIndex(d => d.date >= split);
    const markLine = { silent: true, symbol: 'none',
      lineStyle: { color: '#ffaa00', type: 'dashed', width: 2 },
      label: { formatter: `训练|测试分割: ${split}`, color: '#ffaa00' },
      data: [{ xAxis: split }]
    };

    const chart = echarts.init(el);
    chart.setOption({
      backgroundColor: 'transparent',
      title: { text: '每日消费金额: 真实 vs 预测 (sklearn Ridge 模型)',
        textStyle: { color: '#e0e6ed', fontSize: 13 }, left: 10, top: 5 },
      tooltip: { trigger: 'axis',
        backgroundColor: '#131a3a', borderColor: '#2a3b6e',
        textStyle: { color: '#e0e6ed' } },
      legend: { data: ['真实值', '预测值'], textStyle: { color: '#b0c4de' }, top: 5, right: 10 },
      grid: { left: 60, right: 20, top: 60, bottom: 60 },
      xAxis: { type: 'category', data: dates,
        axisLabel: { color: '#b0c4de', rotate: 30, fontSize: 10 },
        axisLine: { lineStyle: { color: '#2a3b6e' } } },
      yAxis: { type: 'value', name: '消费金额 (元)',
        nameTextStyle: { color: '#b0c4de' },
        axisLabel: { color: '#b0c4de' },
        splitLine: { lineStyle: { color: '#1e2a4a' } } },
      dataZoom: [
        { type: 'inside', start: 0, end: 100 },
        { type: 'slider', start: 0, end: 100,
          textStyle: { color: '#b0c4de' },
          borderColor: '#2a3b6e',
          fillerStyle: { color: 'rgba(30, 144, 255, 0.2)' },
          handleStyle: { color: '#00d4ff' } }
      ],
      series: [
        { name: '真实值', type: 'line', data: actual, smooth: false,
          lineStyle: { color: '#00d4ff', width: 2 },
          itemStyle: { color: '#00d4ff' },
          markLine: markLine
        },
        { name: '预测值', type: 'line', data: predicted, smooth: false,
          lineStyle: { color: '#ffaa00', width: 2, type: 'dashed' },
          itemStyle: { color: '#ffaa00' }
        }
      ]
    });

    // 简单统计: 测试集 RMSE
    if (splitIdx > 0) {
      const testData = data.slice(splitIdx);
      let sse = 0, n = 0;
      for (const d of testData) {
        const diff = d.actual_amount - d.predicted_amount;
        sse += diff * diff;
        n++;
      }
      const test_rmse = Math.sqrt(sse / n);
      infoEl.textContent = `训练 ${r.train_days} 天 · 测试 ${r.test_days} 天 · 测试集 RMSE = ${test_rmse.toFixed(2)}`;
    }

    window.addEventListener('resize', () => chart.resize());
  } catch (e) { renderError(el, '加载失败: ' + e.message); }
}