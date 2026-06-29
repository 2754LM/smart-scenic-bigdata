/* ============================================================
   API client for Smart Scenic Backend
   - All calls go to backend at window.API_BASE
   - Returns parsed JSON
   - Auto-prefixes /api/
   ============================================================ */

const API_BASE = (typeof window !== 'undefined' && window.API_BASE) || 'http://localhost:8000';

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
  // overview
  kpi:                ()      => api('/api/overview/kpi'),
  timeseries:         (m='consumption', s='2023-01-01', e='2023-12-31') =>
                          api(`/api/overview/timeseries?metric=${m}&start=${s}&end=${e}`),
  attractionRank:     ()      => api('/api/overview/attraction-rank'),
  health:             ()      => api('/api/overview/health'),

  // attractions
  attractions:        ()      => api('/api/attractions'),
  attraction:         (id)    => api(`/api/attractions/${id}`),
  attractionSummary:  (id)    => api(`/api/attractions/${id}/summary`),

  // visitors
  visitors:           (params={}) => {
    const q = new URLSearchParams(params).toString();
    return api(`/api/visitors${q ? '?' + q : ''}`);
  },
  visitor:            (id)    => api(`/api/visitors/${id}`),
  visitorAggregate:   (id)    => api(`/api/visitors/${id}/aggregate`),

  // consumption
  consumption:        (params={}) => {
    const q = new URLSearchParams(params).toString();
    return api(`/api/consumption${q ? '?' + q : ''}`);
  },
  visits:             (params={}) => {
    const q = new URLSearchParams(params).toString();
    return api(`/api/consumption/visits${q ? '?' + q : ''}`);
  },

  // analysis
  analysisDaily:      (s='2023-01-01', e='2023-12-31') =>
                          api(`/api/analysis/daily?start=${s}&end=${e}`),
  analysisHourly:     ()      => api('/api/analysis/hourly'),
  analysisRegion:     ()      => api('/api/analysis/region?limit=20'),
  analysisAgeGroup:   ()      => api('/api/analysis/age-group'),
  analysisTypeSummary:()      => api('/api/analysis/type-summary'),
  analysisFpGrowth:   ()      => api('/api/analysis/fpgrowth'),

  // predict
  predict:            (type, features) => api('/api/predict', {
                            method: 'POST', body: { type, features } }),
  predictRegression:  ()      => api('/api/predict/regression'),
  predictClassification: ()   => api('/api/predict/classification'),
  predictClustering:  ()      => api('/api/predict/clustering'),
  predictCompare:     ()      => api('/api/predict/compare'),

  // realtime
  visitRecent:        (limit=20) => api(`/api/realtime/visit-recent?limit=${limit}`),
  visitorProfile:     (id)    => api(`/api/realtime/visitor/${id}`),
  attractionStat:     (id)    => api(`/api/realtime/attraction/${id}`),
};
