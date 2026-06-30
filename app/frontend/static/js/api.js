/* ============================================================
   API client for Smart Scenic Backend
   - All calls go to backend at window.API_BASE
   - Returns parsed JSON
   - Auto-prefixes /api/
   ============================================================ */

const API_BASE = (typeof window !== 'undefined' && window.API_BASE) || 'http://localhost:8000';
window.API_BASE = API_BASE;

async function api(path, options = {}) {
  const url = path.startsWith('http') ? path : `${API_BASE}${path}`;
  const opts = {
    method: options.method || 'GET',
    headers: { 'Content-Type': 'application/json' },
    ...options,
  };
  if (options.body) {
    opts.body = typeof options.body === 'string' ? options.body : JSON.stringify(options.body);
  }
  try {
    const r = await fetch(url, opts);
    if (!r.ok) {
      const text = await r.text();
      throw new Error(`HTTP ${r.status}: ${text}`);
    }
    return await r.json();
  } catch (e) {
    console.error('API error:', url, e);
    throw e;
  }
}

const API = {
  // 概览
  kpi:                ()      => api('/api/overview/kpi'),
  timeseries:         (m='visitors', s='2023-01-01', e='2023-12-31') =>
                          api(`/api/overview/timeseries?metric=${m}&start=${s}&end=${e}`),
  attractionRank:     ()      => api('/api/overview/attraction-rank'),
  health:             ()      => api('/api/overview/health'),

  // 景点 / 游客 / 消费 / 游玩
  attractions:        ()      => api('/api/attractions'),
  attraction:         (id)    => api(`/api/attractions/${id}`),
  attractionSummary:  (id)    => api(`/api/attractions/${id}/summary`),
  visitors:           (q={})  => {
    const qs = new URLSearchParams(q).toString();
    return api(`/api/visitors${qs ? '?' + qs : ''}`);
  },
  visitorAggregate:   (id)    => api(`/api/visitors/${id}/aggregate`),
  consumption:        (q={})  => {
    const qs = new URLSearchParams(q).toString();
    return api(`/api/consumption${qs ? '?' + qs : ''}`);
  },
  visits:             (q={})  => {
    const qs = new URLSearchParams(q).toString();
    return api(`/api/consumption/visits${qs ? '?' + qs : ''}`);
  },

  // 分析
  analysisDaily:      (s='2023-01-01', e='2023-12-31') =>
                          api(`/api/analysis/daily?start=${s}&end=${e}`),
  analysisHourly:     ()      => api('/api/analysis/hourly'),
  analysisRegion:     ()      => api('/api/analysis/region?limit=20'),
  analysisAgeGroup:   ()      => api('/api/analysis/age-group'),
  analysisTypeSummary:()      => api('/api/analysis/type-summary'),
  analysisFpGrowth:   ()      => api('/api/analysis/fpgrowth'),
  analysisDailyCompare:(s='2023-01-01', e='2023-12-31', split='2023-09-01') =>
                          api(`/api/analysis/daily-compare?start=${s}&end=${e}&split_date=${split}`),

  // 预测
  predict:            (type, features) => api('/api/predict', {
                            method: 'POST', body: { type, features } }),
  predictRegression:  ()      => api('/api/predict/regression'),
  predictClassification: ()   => api('/api/predict/classification'),
  predictClustering:  ()      => api('/api/predict/clustering'),
  predictCompare:     ()      => api('/api/predict/compare'),

  // 场景化预测（与景区业务结合）
  tourismAttractionForecast: () => api('/api/predict-tourism/attraction-forecast'),
  tourismAttractionRecommend: (aid, k=5) => api(`/api/predict-tourism/attraction-recommend?attraction_id=${aid}&top_k=${k}`),
  tourismRouteRecommend: (q) => {
    const qs = new URLSearchParams(q).toString();
    return api(`/api/predict-tourism/route-recommend?${qs}`);
  },
  tourismVisitorProfile: (vid) => api(`/api/predict-tourism/visitor-profile/${vid}`),
  tourismTomorrowSummary: () => api('/api/predict-tourism/tomorrow-summary'),
  tourismMultiDayForecast: (days=7) => api(`/api/predict-tourism/multi-day-forecast?days=${days}`),
  tourismFpgrowthSankey: (limit=20) => api(`/api/predict-tourism/fpgrowth-sankey?limit=${limit}`),

  // 实时数据
  visitRecent:        (limit=20) => api(`/api/realtime/visit-recent?limit=${limit}`),
  visitorProfile:     (id)    => api(`/api/realtime/visitor/${id}`),
  attractionStat:     (id)    => api(`/api/realtime/attraction/${id}`),
  publishReview:      (body)  => api('/api/realtime/publish/review', { method: 'POST', body }),
  publishEvent:       (body)  => api('/api/realtime/publish/event',  { method: 'POST', body }),
  kafkaStatus:        ()      => api('/api/realtime/kafka/status'),

  // 系统管理
  adminStatus:        ()      => api('/api/admin/status'),
  adminContainers:    ()      => api('/api/admin/containers'),
  adminModels:        ()      => api('/api/admin/models'),
  adminDatasets:      ()      => api('/api/admin/datasets'),
  adminHdfs:          ()      => api('/api/admin/hdfs'),
  adminJobs:          (limit=20) => api(`/api/admin/jobs?limit=${limit}`),
  adminJob:           (id)    => api(`/api/admin/jobs/${id}`),
  adminActions:       ()      => api('/api/admin/actions'),
  adminTrigger:       (name)  => api(`/api/admin/actions/${name}`, { method: 'POST' }),
  adminPipeline:      (actions) => api('/api/admin/pipeline', {
                              method: 'POST', body: { actions } }),
};
window.API = API;