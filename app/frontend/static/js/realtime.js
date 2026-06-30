/* ============================================================
   realtime.html (⚡ 实时流监控中心) logic
   - 数据流图 + 引擎状态卡
   - 3 步: 生成 → 验证 → 状态
   ============================================================ */

let autoRefresh = null;

document.addEventListener('DOMContentLoaded', () => {
  initPage('realtime');

  document.getElementById('kafka-task-trigger').addEventListener('click', triggerTask);
  document.getElementById('kafka-task-clear').addEventListener('click', clearHBase);
  document.getElementById('hb-search').addEventListener('click', loadHBase);
  document.getElementById('hb-refresh').addEventListener('click', toggleAutoRefresh);

  // 初始加载
  refreshAll();
});

function toggleAutoRefresh() {
  const btn = document.getElementById('hb-refresh');
  if (autoRefresh) {
    clearInterval(autoRefresh);
    autoRefresh = null;
    btn.textContent = '🔄 自动刷新 (3秒/次)';
    btn.style.background = '#10b981';
  } else {
    autoRefresh = setInterval(loadHBase, 3000);
    btn.textContent = '⏸ 停止自动刷新';
    btn.style.background = '#f59e0b';
  }
}

async function refreshAll() {
  await refreshEngineStatus();
  await loadHBase();
}

async function refreshEngineStatus() {
  try {
    const r = await API.kafkaStatus();
    // Producer card
    const prod = r.producer || {};
    const prodEl = document.getElementById('es-producer');
    const prodDetail = document.getElementById('es-producer-detail');
    if (prod.enabled) {
      prodEl.innerHTML = '<span class="tag-pill tag-green">✓ 已连接</span>';
      prodDetail.textContent = `${prod.bootstrap} · topic=${prod.topic_events}`;
    } else {
      prodEl.innerHTML = '<span class="tag-pill tag-yellow">⏸ 降级</span>';
      prodDetail.textContent = prod.error || 'Kafka 不可用';
    }
    // Consumer card
    const cons = r.consumer || {};
    const consEl = document.getElementById('es-consumer');
    const consDetail = document.getElementById('es-consumer-detail');
    if (cons.running) {
      consEl.innerHTML = '<span class="tag-pill tag-green">✓ 运行中</span>';
      const st = cons.stats || {};
      consDetail.textContent = `已消费 ${st.messages_consumed || 0} · 失败 ${st.messages_failed || 0}`;
    } else {
      consEl.innerHTML = '<span class="tag-pill tag-red">✗ 停止</span>';
      consDetail.textContent = cons.error || '';
    }
    // HBase card
    const hbaseEl = document.getElementById('es-hbase');
    const hbaseDetail = document.getElementById('es-hbase-detail');
    hbaseEl.innerHTML = '<span class="tag-pill tag-purple">scenic_realtime</span>';
    try {
      const rec = await API.visitRecent(5);
      hbaseDetail.textContent = `近 ${rec.data?.length || 0} 条记录可查`;
    } catch (e) {
      hbaseDetail.textContent = '查询失败';
    }
    // Total events
    const total = (cons.stats?.messages_consumed || 0);
    document.getElementById('es-total').textContent = total;
    // JSON
    document.getElementById('kafka-status').textContent = JSON.stringify(r, null, 2);
  } catch (e) {
    console.error('refresh engine fail', e);
  }
}

async function triggerTask() {
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
    div.innerHTML = `
      <div style="background: rgba(16,185,129,0.08); border-left: 3px solid #10b981; padding: 12px 16px; border-radius: 4px">
        <div style="color: #10b981; font-size: 14px; font-weight: 500">⚡ ${r.events_published} 个事件已发布到 Kafka</div>
        <div style="color: #9ca3af; font-size: 12px; margin-top: 6px">任务类型: <span class="tag-pill tag-blue">${r.task_type}</span></div>
        <div style="color: #9ca3af; font-size: 12px; margin-top: 4px">${escapeHtml(r.kafka_status || '')}</div>
        <div style="color: #f59e0b; font-size: 12px; margin-top: 4px">⏱ 2秒后自动刷新下方 HBase 数据...</div>
      </div>
    `;
    setTimeout(async () => {
      await loadHBase();
      await refreshEngineStatus();
    }, 2000);
  } catch (e) {
    renderError(div, '触发失败: ' + e.message);
  }
}

async function clearHBase() {
  if (!confirm('确认清空 HBase scenic_realtime 表？此操作不可恢复')) return;
  const div = document.getElementById('kafka-task-result');
  renderLoading(div);
  try {
    const r = await fetch(window.API_BASE + '/api/realtime/hbase/clear', { method: 'POST' }).then(r => r.json());
    div.innerHTML = `<div style="background: rgba(239,68,68,0.08); border-left: 3px solid #ef4444; padding: 12px 16px; border-radius: 4px">
      <div style="color: #ef4444">🗑 HBase 已清空 · 删除 ${r.deleted || 0} 行</div>
    </div>`;
    setTimeout(loadHBase, 1000);
  } catch (e) {
    renderError(div, '清空失败: ' + e.message);
  }
}

async function loadHBase() {
  const el = document.getElementById('hbase-result');
  const type = document.getElementById('hb-type')?.value || 'recent';
  const idEl = document.getElementById('hb-id');
  const id = idEl && idEl.value ? +idEl.value : null;
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
    if (!r.data) { renderEmpty(el, '暂无数据 (请先点"触发任务"生成事件)'); return; }

    // 来源标识
    const sourceTag = r.source === 'hbase'
      ? '<span class="tag-pill tag-green">💾 HBase</span>'
      : '<span class="tag-pill tag-yellow">🗄 MySQL 回退</span>';

    if (r.note) {
      el.innerHTML = `${sourceTag}<span style="color:#9ca3af;font-size:12px;margin-left:8px">${escapeHtml(r.note)}</span><br>`;
    } else {
      el.innerHTML = sourceTag + '<br>';
    }

    const data = Array.isArray(r.data) ? r.data : [r.data];
    let html = '<div style="margin-top: 8px">';
    html += '<table style="width:100%; border-collapse:collapse">';
    if (data.length) {
      const keys = Object.keys(data[0]);
      html += '<tr>' + keys.map(k => `<th>${escapeHtml(k)}</th>`).join('') + '</tr>';
      data.forEach(d => {
        html += '<tr>' + keys.map(k => {
          let v = d[k];
          if (v === null || v === undefined) v = '';
          // 高亮长字符串 (JSON) 或时间戳
          if (typeof v === 'string' && v.length > 50) v = v.substring(0, 50) + '...';
          return `<td>${escapeHtml(String(v))}</td>`;
        }).join('') + '</tr>';
      });
    }
    html += '</table>';
    html += `<div style="margin-top:8px; font-size:12px; color:#9ca3af">共 ${data.length} 条记录</div>`;
    html += '</div>';
    el.innerHTML += html;
  } catch (e) { renderError(el, '查询失败: ' + e.message); }
}
