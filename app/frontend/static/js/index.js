/* ============================================================
   index.html (总览大屏) logic
   - 8 KPI cards
   - 7 ECharts charts
   ============================================================ */

const CHART_BG = 'transparent';
const AXIS_COLOR = '#6c7a96';
const TEXT_COLOR = '#e0e6ed';

function darkBaseOption() {
  return {
    backgroundColor: CHART_BG,
    textStyle: { color: TEXT_COLOR },
    legend: { textStyle: { color: TEXT_COLOR } },
    tooltip: {
      backgroundColor: 'rgba(20,30,60,0.95)',
      borderColor: '#00d4ff',
      textStyle: { color: TEXT_COLOR },
    },
  };
}

async function loadKPIs() {
  try {
    const r = await API.kpi();
    const d = r.data;
    document.getElementById('kpi-visitors').innerHTML      = fmtInt(d.游客总数) + ' <span class="unit">人</span>';
    document.getElementById('kpi-attractions').innerHTML   = fmtInt(d.景点总数) + ' <span class="unit">个</span>';
    document.getElementById('kpi-consume-total').innerHTML = fmtCNY(d.消费总额);
    document.getElementById('kpi-visits').innerHTML        = fmtInt(d.游玩次数) + ' <span class="unit">次</span>';
    document.getElementById('kpi-avg-consume').innerHTML   = fmtCNY(d.平均消费);
    document.getElementById('kpi-avg-duration').innerHTML = fmt(d.平均游玩时长, 1) + ' <span class="unit">小时</span>';
    document.getElementById('kpi-consume-count').innerHTML = fmtInt(d.消费笔数) + ' <span class="unit">笔</span>';
  } catch (e) {
    console.error('kpi load fail', e);
  }
}

async function loadDailyAvg() {
  try {
    const r = await API.analysisDaily();
    const data = r.data || [];
    const avg = data.length ? data.reduce((s, x) => s + (x.visitors || 0), 0) / data.length : 0;
    document.getElementById('kpi-daily-avg').innerHTML = fmtInt(avg) + ' <span class="unit">人/日</span>';
  } catch (e) { /* ignore */ }
}

async function loadTomorrowSummary() {
  try {
    const r = await API.tourismTomorrowSummary();
    if (r.error) return;
    document.getElementById('kpi-yesterday').innerHTML = fmtInt(r.昨日总游客) + ' <span class="unit">人</span>';
    document.getElementById('kpi-tomorrow').innerHTML = fmtInt(r.预测明日) + ' <span class="unit">人</span>';
    const change = r.变化 || 0;
    const sign = change > 0 ? '↑' : (change < 0 ? '↓' : '·');
    const color = change > 0 ? '#10b981' : (change < 0 ? '#ef4444' : '#9ca3af');
    document.getElementById('kpi-change').innerHTML = `<span style="color:${color}">${sign} ${Math.abs(change)}%</span>`;
    const top = r.最热门景点 || {};
    document.getElementById('kpi-top').innerHTML = top.name ? `${escapeHtml(top.name)} (${top.predicted}人)` : '--';
  } catch (e) { console.error('tomorrow summary fail', e); }
}

async function loadVisitorTrend() {
  const el = document.getElementById('chart-visitor-trend');
  const chart = echarts.init(el);
  try {
    const r = await API.timeseries('visitors');
    const data = r.data || [];
    chart.setOption({
      ...darkBaseOption(),
      grid: { left: 50, right: 20, top: 30, bottom: 30 },
      xAxis: { type: 'category', data: data.map(d => d.date.substring(5)), axisLabel: { color: AXIS_COLOR, fontSize: 10 } },
      yAxis: { type: 'value', axisLabel: { color: AXIS_COLOR } },
      series: [{
        name: '游客数', type: 'line', smooth: true, data: data.map(d => d.value),
        areaStyle: { color: 'rgba(30,144,255,0.3)' },
        lineStyle: { color: '#00d4ff', width: 2 },
        itemStyle: { color: '#00d4ff' },
      }],
    });
  } catch (e) { renderError(el, '加载失败'); }
}

async function loadConsumeTrend() {
  const el = document.getElementById('chart-consume-trend');
  const chart = echarts.init(el);
  try {
    const r = await API.timeseries('consumption');
    const data = r.data || [];
    chart.setOption({
      ...darkBaseOption(),
      grid: { left: 60, right: 20, top: 30, bottom: 30 },
      xAxis: { type: 'category', data: data.map(d => d.date.substring(5)), axisLabel: { color: AXIS_COLOR, fontSize: 10 } },
      yAxis: { type: 'value', axisLabel: { color: AXIS_COLOR, formatter: v => (v/1000) + 'k' } },
      series: [{
        name: '消费金额', type: 'line', smooth: true, data: data.map(d => d.value),
        areaStyle: { color: 'rgba(124,58,237,0.3)' },
        lineStyle: { color: '#a855f7', width: 2 },
        itemStyle: { color: '#a855f7' },
      }],
    });
  } catch (e) { renderError(el, '加载失败'); }
}

async function loadAttractionRank() {
  const el = document.getElementById('chart-attraction-rank');
  const chart = echarts.init(el);
  try {
    const r = await API.attractionRank();
    const data = (r.data || []).slice(0, 10);
    chart.setOption({
      ...darkBaseOption(),
      grid: { left: 100, right: 20, top: 20, bottom: 30 },
      xAxis: { type: 'value', axisLabel: { color: AXIS_COLOR } },
      yAxis: { type: 'category', data: data.reverse().map(d => d.景点名称), axisLabel: { color: TEXT_COLOR, fontSize: 11 } },
      series: [{
        name: '游客数', type: 'bar', data: data.reverse().map(d => d.游客数),
        itemStyle: {
          color: (params) => {
            const colors = ['#00d4ff', '#06b6d4', '#0ea5e9', '#3b82f6', '#6366f1', '#8b5cf6', '#a855f7', '#c026d3', '#db2777', '#ec4899'];
            return colors[params.dataIndex % colors.length];
          },
        },
        label: { show: true, position: 'right', color: TEXT_COLOR },
      }],
    });
  } catch (e) { renderError(el, '加载失败'); }
}

async function loadHourly() {
  const el = document.getElementById('chart-hourly');
  const chart = echarts.init(el);
  try {
    const r = await API.analysisHourly();
    const data = r.data || [];
    chart.setOption({
      ...darkBaseOption(),
      grid: { left: 40, right: 20, top: 20, bottom: 30 },
      xAxis: { type: 'category', data: data.map(d => d.hour + '时'), axisLabel: { color: AXIS_COLOR, fontSize: 10 } },
      yAxis: { type: 'value', axisLabel: { color: AXIS_COLOR } },
      series: [{
        name: '游客数', type: 'bar', data: data.map(d => d.visitors),
        itemStyle: { color: '#10b981' },
      }],
    });
  } catch (e) { renderError(el, '加载失败'); }
}

async function loadAgeGender() {
  const el = document.getElementById('chart-age-gender');
  const chart = echarts.init(el);
  try {
    const r = await API.analysisAgeGroup();
    const rows = r.data || [];
    // group by 年龄段
    const groups = {};
    rows.forEach(x => {
      groups[x.年龄段] = groups[x.年龄段] || { '男': 0, '女': 0, '未知': 0 };
      groups[x.年龄段][x.性别] = x.n;
    });
    const cats = Object.keys(groups);
    chart.setOption({
      ...darkBaseOption(),
      grid: { left: 40, right: 20, top: 30, bottom: 30 },
      legend: { data: ['男', '女', '未知'], textStyle: { color: TEXT_COLOR }, top: 0 },
      xAxis: { type: 'category', data: cats, axisLabel: { color: AXIS_COLOR } },
      yAxis: { type: 'value', axisLabel: { color: AXIS_COLOR } },
      series: [
        { name: '男',  type: 'bar', stack: 'a', data: cats.map(c => groups[c]['男']  || 0), itemStyle: { color: '#3b82f6' } },
        { name: '女',  type: 'bar', stack: 'a', data: cats.map(c => groups[c]['女']  || 0), itemStyle: { color: '#ec4899' } },
        { name: '未知',type: 'bar', stack: 'a', data: cats.map(c => groups[c]['未知']|| 0), itemStyle: { color: '#6b7280' } },
      ],
    });
  } catch (e) { renderError(el, '加载失败'); }
}

async function loadRegion() {
  const el = document.getElementById('chart-region');
  const chart = echarts.init(el);
  try {
    const r = await API.analysisRegion();
    const data = r.data || [];
    chart.setOption({
      ...darkBaseOption(),
      grid: { left: 100, right: 20, top: 20, bottom: 30 },
      xAxis: { type: 'value', axisLabel: { color: AXIS_COLOR } },
      yAxis: { type: 'category', data: data.reverse().map(d => d.地区), axisLabel: { color: TEXT_COLOR, fontSize: 10 } },
      series: [{
        name: '游客数', type: 'bar', data: data.map(d => d.visitors),
        itemStyle: { color: '#f59e0b' },
        label: { show: true, position: 'right', color: TEXT_COLOR, fontSize: 10 },
      }],
    });
  } catch (e) { renderError(el, '加载失败'); }
}

async function loadTypeSummary() {
  const el = document.getElementById('chart-type-summary');
  renderLoading(el);
  try {
    const r = await API.analysisTypeSummary();
    const data = r.data || [];
    el.innerHTML = `
      <table>
        <thead><tr><th>类型</th><th>景点数</th><th>游客数</th><th>消费总额</th><th>平均时长(h)</th></tr></thead>
        <tbody>
          ${data.map(d => `<tr>
            <td><span class="badge badge-blue">${escapeHtml(d.类型)}</span></td>
            <td>${fmtInt(d.景点数)}</td>
            <td>${fmtInt(d.游客数)}</td>
            <td>${fmtCNY(d.消费总额)}</td>
            <td>${fmt(d.平均时长, 1)}</td>
          </tr>`).join('')}
        </tbody>
      </table>
    `;
  } catch (e) { renderError(el, '加载失败'); }
}

window.addEventListener('resize', () => {
  document.querySelectorAll('[id^="chart-"]').forEach(el => {
    const inst = echarts.getInstanceByDom(el);
    if (inst) inst.resize();
  });
});

document.addEventListener('DOMContentLoaded', async () => {
  initPage('overview');
  await Promise.all([
    loadKPIs(),
    loadDailyAvg(),
    loadTomorrowSummary(),
    loadVisitorTrend(),
    loadConsumeTrend(),
    loadAttractionRank(),
    loadHourly(),
    loadAgeGender(),
    loadRegion(),
    loadTypeSummary(),
  ]);
});
