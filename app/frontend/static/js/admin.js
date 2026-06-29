/* ============================================================
   System Admin UI - 系统管理面板 JS
   - 显示 17 容器状态
   - 显示已训练模型
   - 显示数据集状态
   - 显示异步任务进度
   - 触发数据处理操作
   ============================================================ */

let ADMIN_POLL_INTERVAL = null;

document.addEventListener('DOMContentLoaded', () => {
  // 页面加载后自动拉一次
  if (document.getElementById('tab-system')) {
    refreshAll();
    ADMIN_POLL_INTERVAL = setInterval(refreshJobsOnly, 2000);
  }
});

async function refreshAll() {
  await Promise.all([
    refreshStatus(),
    refreshContainers(),
    refreshModels(),
    refreshDatasets(),
    refreshHdfs(),
    refreshJobs(),
  ]);
}

async function refreshStatus() {
  try {
    const data = await API.adminStatus();
    const el = document.getElementById('sys-overview');
    if (el) {
      el.innerHTML = `
        <div class="metric"><span class="metric-label">容器总数</span><span class="metric-value">${data.containers.total}</span></div>
        <div class="metric"><span class="metric-label">运行中</span><span class="metric-value" style="color:green">${data.containers.healthy}</span></div>
        <div class="metric"><span class="metric-label">已训练模型</span><span class="metric-value">${data.models.count || 0}</span></div>
        <div class="metric"><span class="metric-label">活跃任务</span><span class="metric-value" id="active-jobs">-</span></div>
        <div class="metric"><span class="metric-label">最后更新</span><span class="metric-value" style="font-size:14px">${(data.ts || '').substring(11, 19)}</span></div>
      `;
    }
  } catch (e) { console.error('refreshStatus', e); }
}

async function refreshContainers() {
  try {
    const data = await API.adminContainers();
    const el = document.getElementById('sys-containers');
    if (!el) return;
    const groups = {
      '存储层':    ['mysql'],
      '协调层':    ['zookeeper-1', 'zookeeper-2', 'zookeeper-3'],
      '计算层':    ['hadoop-namenode', 'hadoop-datanode-1', 'hadoop-datanode-2',
                  'spark-master', 'spark-worker-1'],
      '存储 NoSQL': ['hbase-master', 'hbase-regionserver-1', 'hbase-regionserver-2'],
      '消息队列':  ['kafka-1', 'kafka-2'],
      '数仓':      ['hive-server-1', 'hive-server-2'],
      '应用':      ['demo-backend'],
    };
    let html = '<table style="width:100%; border-collapse:collapse">';
    html += '<tr><th style="text-align:left; padding:6px; border-bottom:1px solid #eee">组件</th><th style="text-align:left; padding:6px; border-bottom:1px solid #eee">容器</th><th style="text-align:left; padding:6px; border-bottom:1px solid #eee">镜像</th><th style="text-align:left; padding:6px; border-bottom:1px solid #eee">状态</th></tr>';
    const byName = {};
    (data.containers || []).forEach(c => { byName[c.name] = c; });
    for (const [group, names] of Object.entries(groups)) {
      html += `<tr><td colspan="4" style="padding:8px 6px 4px; font-weight:bold; background:#f8f9fa">${group}</td></tr>`;
      for (const n of names) {
        const c = byName[n];
        if (!c) {
          html += `<tr><td style="padding:4px 6px"></td><td style="padding:4px 6px; font-family:monospace">${n}</td><td colspan="2" style="padding:4px 6px; color:#999">未运行</td></tr>`;
          continue;
        }
        const color = c.healthy ? '#28a745' : '#dc3545';
        const dot = c.healthy ? '●' : '○';
        const status = c.status.replace(/^Up /, '').substring(0, 50);
        html += `<tr><td style="padding:4px 6px"></td><td style="padding:4px 6px; font-family:monospace">${c.name}</td><td style="padding:4px 6px; font-size:12px; color:#666">${(c.image || '').substring(0, 40)}</td><td style="padding:4px 6px; color:${color}">${dot} ${status}</td></tr>`;
      }
    }
    html += '</table>';
    el.innerHTML = html;
  } catch (e) { console.error('refreshContainers', e); }
}

async function refreshModels() {
  try {
    const data = await API.adminModels();
    const el = document.getElementById('sys-models');
    if (!el) return;
    if (data.error) {
      el.innerHTML = `<p style="color:#999">未训练（${data.error}）</p>`;
      return;
    }
    if (!data.models || data.models.length === 0) {
      el.innerHTML = '<p style="color:#999">暂无已训练模型</p>';
      return;
    }
    let html = '<table style="width:100%; border-collapse:collapse"><tr><th style="text-align:left; padding:6px; border-bottom:1px solid #eee">模型</th><th style="text-align:left; padding:6px; border-bottom:1px solid #eee">类型</th><th style="text-align:left; padding:6px; border-bottom:1px solid #eee">训练时间</th></tr>';
    data.models.forEach(m => {
      const color = m.kind === 'regression' ? '#007bff' : m.kind === 'classification' ? '#28a745' : '#ffc107';
      html += `<tr><td style="padding:4px 6px; font-family:monospace">${m.name}</td><td style="padding:4px 6px"><span style="background:${color}; color:#fff; padding:2px 6px; border-radius:3px; font-size:12px">${m.kind}</span></td><td style="padding:4px 6px; color:#666; font-size:12px">${(m.modified_at || '').substring(0, 19).replace('T', ' ')}</td></tr>`;
    });
    html += '</table>';
    el.innerHTML = html;
  } catch (e) { console.error('refreshModels', e); }
}

async function refreshDatasets() {
  try {
    const data = await API.adminDatasets();
    const el = document.getElementById('sys-datasets');
    if (!el) return;
    let html = '<table style="width:100%; border-collapse:collapse"><tr><th style="text-align:left; padding:6px; border-bottom:1px solid #eee">MySQL 业务表</th><th style="text-align:right; padding:6px; border-bottom:1px solid #eee">行数</th></tr>';
    for (const t of ['t_attraction', 't_visitor', 't_consumption', 't_visit_record']) {
      const v = (data.mysql_tables || {})[t];
      const display = typeof v === 'number' ? v.toLocaleString() : (v || '—');
      const color = typeof v === 'number' ? (v > 0 ? 'green' : 'orange') : 'red';
      html += `<tr><td style="padding:4px 6px; font-family:monospace">${t}</td><td style="padding:4px 6px; text-align:right; color:${color}">${display}</td></tr>`;
    }
    html += '</table>';
    html += '<h4 style="margin-top:12px">CSV 文件</h4>';
    html += '<table style="width:100%; border-collapse:collapse"><tr><th style="text-align:left; padding:6px; border-bottom:1px solid #eee">文件</th><th style="text-align:right; padding:6px; border-bottom:1px solid #eee">行数</th><th style="text-align:right; padding:6px; border-bottom:1px solid #eee">大小</th></tr>';
    for (const f of ['attractions.csv', 'visitors.csv', 'consumption.csv', 'visit_records.csv']) {
      const v = (data.csv_files || {})[f];
      if (v) {
        html += `<tr><td style="padding:4px 6px; font-family:monospace">${f}</td><td style="padding:4px 6px; text-align:right">${v.lines.toLocaleString()}</td><td style="padding:4px 6px; text-align:right">${(v.size / 1024).toFixed(1)} KB</td></tr>`;
      } else {
        html += `<tr><td style="padding:4px 6px; font-family:monospace">${f}</td><td colspan="2" style="padding:4px 6px; text-align:right; color:red">缺失</td></tr>`;
      }
    }
    html += '</table>';
    el.innerHTML = html;
  } catch (e) { console.error('refreshDatasets', e); }
}

async function refreshHdfs() {
  try {
    const data = await API.adminHdfs();
    const el = document.getElementById('sys-hdfs');
    if (!el) return;
    if (!data.available) {
      el.innerHTML = `<p style="color:red">HDFS 不可用: ${data.error || 'unknown'}</p>`;
      return;
    }
    const lines = (data.output || '').split('\n').slice(0, 30);
    el.innerHTML = '<pre style="background:#f8f9fa; padding:8px; border-radius:4px; max-height:300px; overflow:auto; font-size:12px; margin:0">' +
      lines.map(l => l.replace(/</g, '&lt;')).join('\n') + '</pre>';
  } catch (e) { console.error('refreshHdfs', e); }
}

async function refreshJobs() {
  try {
    const data = await API.adminJobs(20);
    renderJobs(data.jobs || []);
    // 顺便更新活跃任务数
    const running = (data.jobs || []).filter(j => j.status === 'running' || j.status === 'pending').length;
    const el = document.getElementById('active-jobs');
    if (el) el.textContent = running;
  } catch (e) { console.error('refreshJobs', e); }
}

async function refreshJobsOnly() {
  // 2 秒轮询（不更新容器和模型，节省资源）
  await refreshJobs();
}

function renderJobs(jobs) {
  const el = document.getElementById('sys-jobs');
  if (!el) return;
  if (jobs.length === 0) {
    el.innerHTML = '<p style="color:#999">暂无任务</p>';
    return;
  }
  let html = '<table style="width:100%; border-collapse:collapse"><tr><th style="text-align:left; padding:6px; border-bottom:1px solid #eee">ID</th><th style="text-align:left; padding:6px; border-bottom:1px solid #eee">名称</th><th style="text-align:left; padding:6px; border-bottom:1px solid #eee">状态</th><th style="text-align:left; padding:6px; border-bottom:1px solid #eee">开始</th><th style="text-align:right; padding:6px; border-bottom:1px solid #eee">操作</th></tr>';
  jobs.forEach(j => {
    const color = {
      'pending':  '#999',
      'running':  '#007bff',
      'success':  '#28a745',
      'failed':   '#dc3545',
    }[j.status] || '#333';
    const started = (j.started_at || '').substring(11, 19);
    html += `<tr>
      <td style="padding:4px 6px; font-family:monospace; font-size:12px">${j.id}</td>
      <td style="padding:4px 6px">${j.name}</td>
      <td style="padding:4px 6px"><span style="background:${color}; color:#fff; padding:2px 6px; border-radius:3px; font-size:12px">${j.status}</span></td>
      <td style="padding:4px 6px; color:#666; font-size:12px">${started}</td>
      <td style="padding:4px 6px; text-align:right"><button class="btn" style="padding:2px 8px; font-size:12px" onclick="showJobDetail('${j.id}')">详情</button></td>
    </tr>`;
  });
  html += '</table>';
  el.innerHTML = html;
}

async function showJobDetail(id) {
  try {
    const j = await API.adminJob(id);
    const log = (j.log_tail || []).join('\n');
    const win = window.open('', '_blank', 'width=800,height=600');
    win.document.write(`
      <html><head><title>Job ${j.id}</title>
      <style>body{font-family:monospace;padding:20px;background:#1e1e1e;color:#d4d4d4}
      pre{background:#252526;padding:12px;border-radius:4px;overflow:auto;max-height:500px}
      .ok{color:#4ec9b0}.err{color:#f48771}.info{color:#569cd6}</style>
      </head><body>
      <h2>Job ${j.id}: ${j.name}</h2>
      <p>状态: <b class="${j.status === 'success' ? 'ok' : j.status === 'failed' ? 'err' : 'info'}">${j.status}</b></p>
      <p>类型: ${j.kind} | 开始: ${j.started_at || '—'} | 结束: ${j.finished_at || '—'}</p>
      ${j.error ? `<p class="err">错误: ${j.error}</p>` : ''}
      <h3>日志（末尾 30 行）</h3>
      <pre>${log.replace(/</g, '&lt;')}</pre>
      </body></html>
    `);
  } catch (e) { alert('查看任务失败: ' + e); }
}

async function triggerAction(name) {
  if (!confirm('确认触发操作：' + name + '？')) return;
  try {
    const r = await API.adminTrigger(name);
    alert('已提交任务 #' + r.job_id + '\n\n' + r.name + '\n\n可到下方"异步任务"区域查看进度');
    setTimeout(refreshJobs, 1000);
  } catch (e) { alert('触发失败: ' + e); }
}

async function triggerPipeline() {
  if (!confirm('确认一键初始化？\n\n这会依次执行：\n1. 加载 CSV 到 MySQL\n2. Sqoop 导入 HDFS\n3. Spark 清洗\n4. Hive DDL + 视图\n5. PySpark 训练\n\n（约 5-10 分钟）')) return;
  try {
    const r = await API.adminPipeline(['load_csv', 'sqoop', 'spark_clean', 'hive_ddl', 'spark_train']);
    alert('已提交一键初始化任务 #' + r.job_id + '\n\n约 5-10 分钟，可到下方"异步任务"区域查看进度');
    setTimeout(refreshJobs, 1000);
  } catch (e) { alert('触发失败: ' + e); }
}