/* ============================================================
   analysis.html (数据分析) logic
   - 6 charts + 1 FPGrowth table
   - 时间范围 + 景点 过滤
   ============================================================ */

const CHART_BG = 'transparent';
const AXIS_COLOR = '#6c7a96';
const TEXT_COLOR = '#e0e6ed';

function baseOpt() {
  return {
    backgroundColor: CHART_BG,
    textStyle: { color: TEXT_COLOR },
    legend: { textStyle: { color: TEXT_COLOR } },
    tooltip: { backgroundColor: 'rgba(20,30,60,0.95)', borderColor: '#00d4ff', textStyle: { color: TEXT_COLOR } },
  };
}

async function loadAttractionOptions() {
  try {
    const r = await API.attractions();
    const sel = document.getElementById('filter-attraction');
    (r.data || []).forEach(a => {
      const opt = document.createElement('option');
      opt.value = a.景点ID; opt.textContent = `${a.景点ID} - ${a.景点名称}`;
      sel.appendChild(opt);
    });
  } catch (e) { /* ignore */ }
}

function getFilters() {
  return {
    start: document.getElementById('filter-start').value,
    end:   document.getElementById('filter-end').value,
  };
}

async function loadDaily() {
  const el = document.getElementById('chart-daily');
  const chart = echarts.init(el);
  const { start, end } = getFilters();
  try {
    const [visitorRes, consumeRes] = await Promise.all([
      API.analysisDaily(start, end),
      API.timeseries('consumption', start, end),
    ]);
    const vd = visitorRes.data || [];
    const cd = consumeRes.data || [];
    chart.setOption({
      ...baseOpt(),
      grid: { left: 60, right: 60, top: 30, bottom: 50 },
      legend: { data: ['游客数', '消费额'], textStyle: { color: TEXT_COLOR }, top: 0 },
      xAxis: { type: 'category', data: vd.map(d => d.date), axisLabel: { color: AXIS_COLOR, fontSize: 10 } },
      yAxis: [
        { type: 'value', name: '游客数', axisLabel: { color: AXIS_COLOR }, nameTextStyle: { color: AXIS_COLOR } },
        { type: 'value', name: '消费额', axisLabel: { color: AXIS_COLOR, formatter: v => (v/1000) + 'k' }, nameTextStyle: { color: AXIS_COLOR } },
      ],
      series: [
        { name: '游客数', type: 'line', smooth: true, data: vd.map(d => d.visitors),
          lineStyle: { color: '#00d4ff' }, itemStyle: { color: '#00d4ff' },
          areaStyle: { color: 'rgba(0,212,255,0.2)' } },
        { name: '消费额', type: 'bar', yAxisIndex: 1, data: cd.map(d => d.value),
          itemStyle: { color: 'rgba(168,85,247,0.6)' } },
      ],
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
      ...baseOpt(),
      grid: { left: 40, right: 20, top: 20, bottom: 30 },
      xAxis: { type: 'category', data: data.map(d => d.hour + '时'), axisLabel: { color: AXIS_COLOR, fontSize: 10 } },
      yAxis: { type: 'value', axisLabel: { color: AXIS_COLOR } },
      series: [{ type: 'bar', data: data.map(d => d.visitors), itemStyle: { color: '#10b981' } }],
    });
  } catch (e) { renderError(el, '加载失败'); }
}

async function loadType() {
  const el = document.getElementById('chart-type');
  const chart = echarts.init(el);
  try {
    const r = await API.analysisTypeSummary();
    const data = r.data || [];
    chart.setOption({
      ...baseOpt(),
      grid: { left: 100, right: 20, top: 20, bottom: 30 },
      xAxis: { type: 'value', axisLabel: { color: AXIS_COLOR } },
      yAxis: { type: 'category', data: data.map(d => d.类型), axisLabel: { color: TEXT_COLOR } },
      series: [{
        name: '游客数', type: 'bar', data: data.map(d => d.游客数),
        itemStyle: { color: '#3b82f6' },
        label: { show: true, position: 'right', color: TEXT_COLOR, formatter: '{c}' },
      }],
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
      ...baseOpt(),
      grid: { left: 110, right: 30, top: 20, bottom: 30 },
      xAxis: { type: 'value', axisLabel: { color: AXIS_COLOR } },
      yAxis: { type: 'category', data: data.slice().reverse().map(d => d.地区),
               axisLabel: { color: TEXT_COLOR, fontSize: 10 } },
      series: [{
        name: '游客数', type: 'bar', data: data.map(d => d.visitors),
        itemStyle: { color: '#f59e0b' },
        label: { show: true, position: 'right', color: TEXT_COLOR, fontSize: 10 },
      }],
    });
  } catch (e) { renderError(el, '加载失败'); }
}

async function loadAge() {
  const el = document.getElementById('chart-age');
  const chart = echarts.init(el);
  try {
    const r = await API.analysisAgeGroup();
    const rows = r.data || [];
    const groups = {};
    rows.forEach(x => {
      groups[x.年龄段] = groups[x.年龄段] || { '男': 0, '女': 0, '未知': 0 };
      groups[x.年龄段][x.性别] = x.n;
    });
    const cats = Object.keys(groups);
    chart.setOption({
      ...baseOpt(),
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

async function loadFpGrowth() {
  const el = document.getElementById('table-fpgrowth');
  renderLoading(el);
  try {
    const r = await API.analysisFpGrowth();
    const data = r.data || [];
    if (!data.length) { renderEmpty(el, '暂无关联规则（请先跑 Spark FPGrowth 关联规则分析）'); return; }
    el.innerHTML = `
      <table>
        <thead><tr>
          <th>前项</th><th>→</th><th>后项</th>
          <th>置信度</th><th>提升度</th><th>支持度</th>
        </tr></thead>
        <tbody>
          ${data.map(r => {
            const ant = (r.antecedent || []).map(a => `<span class="badge badge-blue">${escapeHtml(a.景点名称)}</span>`).join(' ');
            const con = (r.consequent || []).map(c => `<span class="badge badge-green">${escapeHtml(c.景点名称)}</span>`).join(' ');
            return `<tr>
              <td>${ant}</td><td>→</td><td>${con}</td>
              <td><span class="text-accent">${fmt(r.confidence, 3)}</span></td>
              <td>${fmt(r.lift, 2)}</td>
              <td>${fmt(r.support, 4)}</td>
            </tr>`;
          }).join('')}
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
  initPage('analysis');
  await loadAttractionOptions();
  document.getElementById('btn-apply').addEventListener('click', async () => {
    await Promise.all([loadDaily(), loadFpGrowth()]);
  });
  document.getElementById('btn-reset').addEventListener('click', () => {
    document.getElementById('filter-start').value = '2023-01-01';
    document.getElementById('filter-end').value = '2023-12-31';
    document.getElementById('filter-attraction').value = '';
    loadDaily(); loadFpGrowth();
  });
  await Promise.all([loadDaily(), loadHourly(), loadType(), loadRegion(), loadAge(), loadFpGrowth()]);
});
