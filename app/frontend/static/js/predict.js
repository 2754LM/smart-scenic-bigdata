/* ============================================================
   predict.html (智能预测中心) logic
   - 4 scenario cards: 客流预测 / 智能推荐 / 路线规划 / 游客画像
   - 模型对比 + 真实 vs 预测 daily-compare
   ============================================================ */

document.addEventListener('DOMContentLoaded', async () => {
  initPage('predict');

  // 推荐下拉
  await loadAttractionOptions();
  // 客流预测（默认加载）
  await loadForecast();
  // 多日预测
  await loadMultiDay();
  // 模型对比
  await Promise.all([
    loadRegressionTable(),
    loadClassificationTable(),
    loadClusteringTable(),
    loadCompareChart(),
  ]);

  document.getElementById('btn-recommend').addEventListener('click', loadRecommend);
  document.getElementById('btn-route').addEventListener('click', loadRoute);
  document.getElementById('btn-profile').addEventListener('click', loadProfile);
  document.getElementById('btn-compare').addEventListener('click', loadCompareChart);
  document.getElementById('btn-multi').addEventListener('click', loadMultiDay);
});

// ============================================================
// 1. 客流预测
// ============================================================
async function loadAttractionOptions() {
  const sel = document.getElementById('rec-from');
  try {
    const r = await API.attractions();
    (r.data || []).forEach(a => {
      const opt = document.createElement('option');
      opt.value = a.景点ID;
      opt.textContent = `${a.景点ID} - ${a.景点名称} (${a.类型 || ''})`;
      sel.appendChild(opt);
    });
  } catch (e) { /* ignore */ }
}

async function loadForecast() {
  const el = document.getElementById('chart-forecast');
  const listEl = document.getElementById('forecast-list');
  renderLoading(el);
  listEl.innerHTML = '<span class="text-muted">加载中...</span>';
  try {
    const r = await API.tourismAttractionForecast();
    const data = r.forecasts || [];
    if (!data.length) { renderEmpty(el, '暂无数据'); return; }

    // 主图：实际 vs 预测 (bar+line)
    const names = data.map(d => d.景点名称);
    const yesterday = data.map(d => d.昨日游客);
    const predicted = data.map(d => d.预测明日);
    const avg7 = data.map(d => d.近7天日均);

    const chart = echarts.init(el);
    chart.setOption({
      backgroundColor: 'transparent',
      tooltip: {
        trigger: 'axis',
        backgroundColor: 'rgba(20,30,60,0.95)', borderColor: '#00d4ff',
        textStyle: { color: '#fff' },
        axisPointer: { type: 'shadow' },
      },
      legend: { data: ['昨日实际', '预测明日', '近7日均'], textStyle: { color: '#b0c4de' }, top: 0 },
      grid: { left: 50, right: 20, top: 40, bottom: 50 },
      xAxis: { type: 'category', data: names, axisLabel: { color: '#b0c4de', fontSize: 10, rotate: 25 } },
      yAxis: { type: 'value', axisLabel: { color: '#6c7a96' }, splitLine: { lineStyle: { color: '#1e2a4a' } } },
      series: [
        { name: '昨日实际', type: 'bar', data: yesterday,
          itemStyle: { color: 'rgba(168, 85, 247, 0.6)', borderColor: '#a855f7', borderWidth: 1 },
          barWidth: '25%' },
        { name: '近7日均', type: 'line', data: avg7,
          lineStyle: { color: '#6c7a96', type: 'dotted' }, itemStyle: { color: '#6c7a96' } },
        { name: '预测明日', type: 'bar', data: predicted,
          itemStyle: { color: 'rgba(0, 212, 255, 0.85)' },
          barWidth: '25%',
          label: { show: true, position: 'top', color: '#00d4ff', fontSize: 11, formatter: '{c}' } },
      ],
    });
    window.addEventListener('resize', () => chart.resize());

    // 列表
    listEl.innerHTML = data.map(d => {
      const change = d.变化;
      const changeColor = change > 0 ? '#10b981' : (change < 0 ? '#ef4444' : '#9ca3af');
      const arrow = change > 0 ? '↑' : (change < 0 ? '↓' : '·');
      return `<div style="display:flex; justify-content:space-between; padding:6px 0; border-bottom: 1px dashed #2a3b6e">
        <div style="flex:1">
          <span class="tag tag-blue">${d.景点名称}</span>
          <span style="color:#9ca3af; font-size:11px">${d.类型 || ''}</span>
        </div>
        <div style="text-align:right; min-width:90px">
          <div style="color:#fff">${d.预测明日} 人</div>
          <div style="color:${changeColor}; font-size:11px">${arrow} ${Math.abs(change)}%</div>
        </div>
      </div>`;
    }).join('');
  } catch (e) { renderError(el, '加载失败: ' + e.message); }
}

// ============================================================
// 1.5 多日客流预测
// ============================================================
async function loadMultiDay() {
  const el = document.getElementById('chart-multi-total');
  const listEl = document.getElementById('multi-list');
  const days = parseInt(document.getElementById('multi-days').value);
  renderLoading(el);
  listEl.innerHTML = '<span class="text-muted">加载中...</span>';
  try {
    const r = await API.tourismMultiDayForecast(days);
    if (r.error) { renderError(el, r.error); return; }
    const total = r.总客流 || [];
    const items = r.景点预测 || [];
    if (!total.length) { renderEmpty(el, '暂无数据'); return; }

    // 主图：每日总客流
    const chart = echarts.init(el);
    chart.setOption({
      backgroundColor: 'transparent',
      title: { text: `未来 ${days} 天全景区总客流预测`, textStyle: { color: '#e0e6ed', fontSize: 13 }, left: 10, top: 5 },
      tooltip: { trigger: 'axis', backgroundColor: 'rgba(20,30,60,0.95)', borderColor: '#00d4ff', textStyle: { color: '#fff' } },
      grid: { left: 50, right: 50, top: 50, bottom: 60 },
      xAxis: {
        type: 'category', data: total.map(d => d.date.substring(5)),
        axisLabel: { color: '#b0c4de', fontSize: 10, rotate: 30 },
        axisLine: { lineStyle: { color: '#2a3b6e' } },
      },
      yAxis: { type: 'value', axisLabel: { color: '#6c7a96' }, splitLine: { lineStyle: { color: '#1e2a4a' } } },
      series: [{
        name: '总客流', type: 'bar', data: total.map(d => d.total_visitors),
        itemStyle: {
          color: (params) => total[params.dataIndex]?.is_weekend ? '#a855f7' : '#00d4ff',
          borderRadius: [4, 4, 0, 0],
        },
        label: { show: true, position: 'top', color: '#b0c4de', fontSize: 10, formatter: '{c}' },
      }],
    });
    window.addEventListener('resize', () => chart.resize());

    // 列表
    listEl.innerHTML = items.map(f => {
      const totalN = f[`未来${days}天总计`];
      const avgN = f[`未来${days}天日均`];
      return `<div style="display:flex; justify-content:space-between; padding:6px 0; border-bottom: 1px dashed #2a3b6e">
        <div style="flex:1">
          <span class="tag tag-blue">${escapeHtml(f.景点名称)}</span>
          <span style="color:#9ca3af; font-size:11px">${f.类型 || ''}</span>
        </div>
        <div style="text-align:right; min-width:90px">
          <div style="color:#fff">${fmtInt(totalN)} 人</div>
          <div style="color:#9ca3af; font-size:11px">日均 ${fmt(avgN, 1)}</div>
        </div>
      </div>`;
    }).join('');
  } catch (e) { renderError(el, '加载失败: ' + e.message); }
}

// ============================================================
// 2. 智能推荐
// ============================================================
async function loadRecommend() {
  const aid = document.getElementById('rec-from').value;
  const el = document.getElementById('rec-result');
  if (!aid) { renderError(el, '请先选择景点'); return; }
  renderLoading(el);
  try {
    const r = await API.tourismAttractionRecommend(aid);
    const recs = r.recommendations || [];
    if (!recs.length) { renderEmpty(el, '没有足够的游玩序列数据'); return; }

    // 当前景点名
    const fromName = document.getElementById('rec-from').selectedOptions[0]?.text.split(' - ')[1] || aid;

    el.innerHTML = `
      <div class="result-block">
        <div style="font-size:12px;color:#9ca3af">游玩了 <span class="text-accent">${escapeHtml(fromName)}</span> 后, 接下来最常去:</div>
        <div style="margin-top:12px; display:flex; flex-wrap:wrap; gap:8px">
          ${recs.map((r, i) => {
            const colors = ['tag-blue', 'tag-purple', 'tag-green', 'tag-yellow', 'tag-red'];
            return `<div style="background: rgba(0,212,255,0.05); border:1px solid #2a3b6e; border-radius:6px; padding:10px 14px; min-width:140px">
              <div style="font-size:11px;color:#9ca3af">#${i+1} 推荐</div>
              <div style="font-size:15px;color:#fff;margin:4px 0">${escapeHtml(r.景点名称)}</div>
              <div><span class="tag ${colors[i % 5]}">${r.类型 || ''}</span></div>
              <div style="font-size:11px;color:#9ca3af;margin-top:4px">频次: ${r.频次} · 概率: ${(r.概率*100).toFixed(1)}%</div>
            </div>`;
          }).join('')}
        </div>
        <div style="margin-top:10px; font-size:11px; color:#6c7a96">基于 ${r.total_pairs} 个游玩序列样本</div>
      </div>
    `;
  } catch (e) { renderError(el, '加载失败: ' + e.message); }
}

// ============================================================
// 3. 路线规划
// ============================================================
async function loadRoute() {
  const el = document.getElementById('route-result');
  const type = document.getElementById('route-type').value;
  const budget = +document.getElementById('route-budget').value;
  const hours = +document.getElementById('route-hours').value;
  renderLoading(el);
  try {
    const r = await API.tourismRouteRecommend({ type, budget, hours });
    if (r.error) { renderError(el, r.error); return; }
    const route = r.route || [];
    if (!route.length) { renderEmpty(el, '没有符合条件的景点'); return; }

    el.innerHTML = `
      <div class="result-block">
        <div style="display:flex; gap:20px; font-size:12px; color:#9ca3af; margin-bottom: 10px">
          <span>类型: <span class="text-accent">${escapeHtml(r.type)}</span></span>
          <span>预算: <span class="text-accent">¥${r.budget}</span></span>
          <span>剩余: <span class="text-accent">¥${r.remaining_budget}</span></span>
          <span>总时长: <span class="text-accent">${r.total_hours}h</span></span>
        </div>
        <div style="font-size:12px;color:#9ca3af;margin-bottom:8px">📍 推荐路线（按热度排序）:</div>
        <div style="position:relative; padding-left: 20px">
          ${route.map((p, i) => `
            <div style="position:relative; padding: 8px 0 8px 20px; border-left: 2px solid #2a3b6e; margin-left: 6px">
              <div style="position:absolute; left:-9px; top:10px; width:16px; height:16px; border-radius:50%; background:#00d4ff; color:#000; font-size:10px; line-height:16px; text-align:center; font-weight:bold">${i+1}</div>
              <div style="color:#fff; font-size:14px">${escapeHtml(p.景点名称)}</div>
              <div style="font-size:11px; color:#9ca3af; margin-top:2px">
                <span class="tag tag-blue">${p.类型}</span>
                <span>${p.开放时间}</span>
                <span style="color:#10b981">· ¥${p.预计消费}</span>
                <span style="color:#f59e0b">· ${p.建议游玩时长}h</span>
              </div>
            </div>
          `).join('')}
        </div>
      </div>
    `;
  } catch (e) { renderError(el, '加载失败: ' + e.message); }
}

// ============================================================
// 4. 游客画像
// ============================================================
async function loadProfile() {
  const vid = document.getElementById('profile-vid').value.trim();
  const el = document.getElementById('profile-result');
  if (!vid) { renderError(el, '请输入游客 ID'); return; }
  renderLoading(el);
  try {
    const r = await API.tourismVisitorProfile(vid);
    if (r.detail) { renderError(el, r.detail); return; }
    const v = r.visitor;
    const b = r.behavior;
    const p = r.preferences || [];
    const m = r.ml_predictions;

    const profileTags = {
      0: { class: 'tag-blue',   text: '低频低消费 · 推送优惠券' },
      1: { class: 'tag-green',  text: '高频中消费 · 推荐热门' },
      2: { class: 'tag-purple', text: '中频高消费 · 推荐 VIP' },
      3: { class: 'tag-red',    text: '高频高消费 · 专属管家' },
    };
    const clusterTag = profileTags[m.群体归类] || { class: 'tag-blue', text: m.群体标签 || '-' };

    el.innerHTML = `
      <div style="display:grid; grid-template-columns: 1fr 1fr 1fr; gap: 14px">
        <!-- 基本信息 -->
        <div class="profile-box">
          <div style="color:#00d4ff; font-size:12px; margin-bottom:8px">👤 基本信息</div>
          <div class="profile-row"><span class="k">姓名</span><span class="v">${escapeHtml(v.姓名 || '-')}</span></div>
          <div class="profile-row"><span class="k">性别</span><span class="v">${v.性别 || '-'}</span></div>
          <div class="profile-row"><span class="k">年龄</span><span class="v">${v.年龄 || '-'}</span></div>
          <div class="profile-row"><span class="k">地区</span><span class="v">${escapeHtml(v.地区 || '-')}</span></div>
        </div>

        <!-- 行为统计 -->
        <div class="profile-box">
          <div style="color:#a855f7; font-size:12px; margin-bottom:8px">📊 行为统计</div>
          <div class="profile-row"><span class="k">消费笔数</span><span class="v">${b.消费笔数}</span></div>
          <div class="profile-row"><span class="k">消费总额</span><span class="v">¥${b.消费总额.toFixed(0)}</span></div>
          <div class="profile-row"><span class="k">平均消费</span><span class="v">¥${b.平均消费.toFixed(0)}</span></div>
          <div class="profile-row"><span class="k">游玩次数</span><span class="v">${b.游玩次数}</span></div>
          <div class="profile-row"><span class="k">去过景点</span><span class="v">${b.去过的景点数} 个</span></div>
        </div>

        <!-- ML 预测 -->
        <div class="profile-box" style="background: linear-gradient(135deg, #0a0e27 0%, #131a3a 100%); border: 1px solid #00d4ff">
          <div style="color:#10b981; font-size:12px; margin-bottom:8px">🤖 ML 预测</div>
          <div class="profile-row">
            <span class="k">高价值游客</span>
            <span class="v">
              <span class="tag ${m.高价值游客 ? 'tag-green' : 'tag-yellow'}">
                ${m.高价值游客 ? '✓ 是' : '✗ 否'}
              </span>
            </span>
          </div>
          <div class="profile-row"><span class="k">高价值概率</span><span class="v" style="color:#00d4ff">${(m.高价值概率*100).toFixed(1)}%</span></div>
          <div class="profile-row"><span class="k">群体标签</span><span class="v"><span class="tag ${clusterTag.class}">${clusterTag.text}</span></span></div>
          <div class="profile-row"><span class="k">预测总消费</span><span class="v" style="color:#f59e0b">¥${m.预测消费.toFixed(0)}</span></div>
        </div>
      </div>

      <div class="result-block" style="margin-top:14px">
        <div style="font-size:12px; color:#9ca3af">💡 兴趣偏好:</div>
        <div style="margin-top:6px">
          ${p.length ? p.map(x => `<span class="tag tag-blue">${escapeHtml(x.类型)} × ${x.次数}</span>`).join('') : '<span class="text-muted">暂无数据</span>'}
        </div>
        ${m.运营建议 ? `<div style="margin-top:10px; padding:8px 12px; background: rgba(16,185,129,0.08); border-left: 3px solid #10b981; font-size:13px; color:#10b981">💡 运营建议: ${escapeHtml(m.运营建议)}</div>` : ''}
      </div>
    `;
  } catch (e) { renderError(el, '加载失败: ' + e.message); }
}

// ============================================================
// 5. 模型对比表
// ============================================================
async function loadRegressionTable() {
  const el = document.getElementById('table-regression');
  renderLoading(el);
  try {
    const r = await API.predictRegression();
    const results = (r.data && r.data.results) || [];
    if (!results.length) { renderEmpty(el, '暂无'); return; }
    const nameMap = { linear: 'Linear', lasso: 'Lasso', ridge: 'Ridge', rf: 'RandomForest' };
    el.innerHTML = `
      <div style="color:#00d4ff; font-size:12px; margin-bottom:4px">📈 回归模型对比</div>
      <div style="color:#9ca3af; font-size:10px; margin-bottom:6px">预测目标: <span class="text-accent">游客消费总额 (¥)</span></div>
      <table>
        <thead><tr><th>模型</th><th>RMSE</th><th>R²</th></tr></thead>
        <tbody>
          ${results.map(x => `<tr>
            <td>${escapeHtml(nameMap[x.model] || x.model)}</td>
            <td>${fmt(x.rmse, 2)}</td>
            <td><span class="text-accent">${fmt(x.r2, 4)}</span></td>
          </tr>`).join('')}
        </tbody>
      </table>
    `;
  } catch (e) { renderError(el, '失败'); }
}

async function loadClassificationTable() {
  const el = document.getElementById('table-classification');
  renderLoading(el);
  try {
    const r = await API.predictClassification();
    const rows = (r.data && r.data.results) || [];
    if (!rows.length) { renderEmpty(el, '暂无'); return; }
    const nameMap = { rf: 'RandomForest', dt: 'DecisionTree', gbt: 'GBT', lr: 'LogisticReg' };
    el.innerHTML = `
      <div style="color:#a855f7; font-size:12px; margin-bottom:4px">🎯 分类模型对比</div>
      <div style="color:#9ca3af; font-size:10px; margin-bottom:4px">预测目标: <span class="text-accent">是否高频回头客 (visit_count >= 中位数)</span></div>
      <div style="color:#9ca3af; font-size:10px; margin-bottom:6px">特征: age, avg_duration, unique_attractions (3 维, 无数据泄漏)</div>
      <table>
        <thead><tr><th>模型</th><th>CV5 Acc</th><th>Test Acc</th><th>Test F1</th><th>Test AUC</th></tr></thead>
        <tbody>
          ${rows.map(x => `<tr>
            <td>${escapeHtml(nameMap[x.model] || x.model)}</td>
            <td>${fmt(x.cv_acc_mean || x.accuracy, 4)}${x.cv_acc_std ? '±' + x.cv_acc_std.toFixed(3) : ''}</td>
            <td><span class="text-accent">${fmt(x.test_acc || x.accuracy, 4)}</span></td>
            <td>${fmt(x.test_f1 || x.f1, 4)}</td>
            <td>${fmt(x.test_auc || x.auc || 0, 4)}</td>
          </tr>`).join('')}
        </tbody>
      </table>
    `;
  } catch (e) { renderError(el, '失败'); }
}

async function loadClusteringTable() {
  const el = document.getElementById('table-clustering');
  renderLoading(el);
  try {
    const r = await API.predictClustering();
    const stats = (r.data && r.data.cluster_stats) || [];
    if (!stats.length) { renderEmpty(el, '暂无'); return; }
    el.innerHTML = `
      <div style="color:#10b981; font-size:12px; margin-bottom:6px">聚类 (KMeans k=4)</div>
      <table>
        <thead><tr><th>Cluster</th><th>人数</th><th>人均消费</th></tr></thead>
        <tbody>
          ${stats.map(s => `<tr>
            <td><span class="tag tag-purple">#${s.cluster}</span></td>
            <td>${fmtInt(s.n)}</td>
            <td>¥${fmt(s.avg_total_consume, 0)}</td>
          </tr>`).join('')}
        </tbody>
      </table>
    `;
  } catch (e) { renderError(el, '失败'); }
}

// ============================================================
// 6. 真实 vs 预测 折线图
// ============================================================
async function loadCompareChart() {
  const el = document.getElementById('chart-compare');
  const infoEl = document.getElementById('cmp-info');
  const start = document.getElementById('cmp-start').value;
  const end   = document.getElementById('cmp-end').value;
  const split = document.getElementById('cmp-split').value;

  renderLoading(el);
  try {
    const r = await API.analysisDailyCompare(start, end, split);
    if (r.error) { renderError(el, r.error); return; }
    const data = r.results || [];
    if (!data.length) { renderEmpty(el, '暂无数据'); return; }

    const dates = data.map(d => d.date);
    const actual = data.map(d => +d.actual_amount);
    const predicted = data.map(d => +d.predicted_amount);

    const splitIdx = data.findIndex(d => d.date >= split);
    const markLine = { silent: true, symbol: 'none',
      lineStyle: { color: '#ffaa00', type: 'dashed', width: 2 },
      label: { formatter: `训练|测试: ${split}`, color: '#ffaa00', position: 'end' },
      data: [{ xAxis: split }]
    };

    const chart = echarts.init(el);
    chart.setOption({
      backgroundColor: 'transparent',
      title: { text: '每日消费金额: 真实 vs 预测 (sklearn Ridge)', textStyle: { color: '#e0e6ed', fontSize: 13 }, left: 10, top: 5 },
      tooltip: { trigger: 'axis', backgroundColor: '#131a3a', borderColor: '#2a3b6e', textStyle: { color: '#e0e6ed' } },
      legend: { data: ['真实值', '预测值'], textStyle: { color: '#b0c4de' }, top: 5, right: 10 },
      grid: { left: 60, right: 20, top: 60, bottom: 60 },
      xAxis: { type: 'category', data: dates, axisLabel: { color: '#b0c4de', rotate: 30, fontSize: 10 }, axisLine: { lineStyle: { color: '#2a3b6e' } } },
      yAxis: { type: 'value', name: '消费金额 (元)', nameTextStyle: { color: '#b0c4de' },
        axisLabel: { color: '#b0c4de' }, splitLine: { lineStyle: { color: '#1e2a4a' } } },
      dataZoom: [
        { type: 'inside', start: 0, end: 100 },
        { type: 'slider', start: 0, end: 100, textStyle: { color: '#b0c4de' },
          borderColor: '#2a3b6e', fillerStyle: { color: 'rgba(30, 144, 255, 0.2)' }, handleStyle: { color: '#00d4ff' } }
      ],
      series: [
        { name: '真实值', type: 'line', data: actual, smooth: false, lineStyle: { color: '#00d4ff', width: 2 }, itemStyle: { color: '#00d4ff' }, markLine: markLine },
        { name: '预测值', type: 'line', data: predicted, smooth: false, lineStyle: { color: '#ffaa00', width: 2, type: 'dashed' }, itemStyle: { color: '#ffaa00' } }
      ]
    });

    if (splitIdx > 0) {
      const testData = data.slice(splitIdx);
      let sse = 0, n = 0;
      for (const d of testData) {
        const diff = d.actual_amount - d.predicted_amount;
        sse += diff * diff; n++;
      }
      const test_rmse = Math.sqrt(sse / n);
      infoEl.innerHTML = `训练 <span style="color:#00d4ff">${r.train_days}</span> 天 · 测试 <span style="color:#f59e0b">${r.test_days}</span> 天 · 测试集 RMSE = <span class="text-accent">${test_rmse.toFixed(2)}</span>`;
    }

    window.addEventListener('resize', () => chart.resize());
  } catch (e) { renderError(el, '加载失败: ' + e.message); }
}
