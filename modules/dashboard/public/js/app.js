// PQG6 Dashboard — Main Application
// WebSocket → Chart.js, Alert Feed, ML Metrics, Controls

(function() {
  'use strict';

  // ---- State ----
  let ws = null;
  let flowPaused = false;
  let alertCount = 0;
  let flowChart = null;
  let importanceChart = null;
  const severityFilter = { current: 'all' };

  // ---- WebSocket ----
  function connectWS() {
    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    ws = new WebSocket(`${proto}//${location.host}`);
    const statusEl = document.getElementById('ws-status');

    ws.onopen = () => {
      statusEl.className = 'connection-status connected';
      statusEl.innerHTML = '<span class="status-dot"></span> Connected';
    };
    ws.onclose = () => {
      statusEl.className = 'connection-status disconnected';
      statusEl.innerHTML = '<span class="status-dot"></span> Disconnected';
      setTimeout(connectWS, 3000);
    };
    ws.onmessage = (e) => {
      try {
        const msg = JSON.parse(e.data);
        if (msg.topic === '_init') {
          // Initial history
          (msg.alerts || []).reverse().forEach(a => addAlert(a));
          (msg.stats || []).reverse().forEach(s => updateStats(s));
        } else if (msg.topic === 'network-flows') {
          if (!flowPaused) addFlowPoint(msg.data);
        } else if (msg.topic === 'security-alerts') {
          addAlert(msg.data);
        } else if (msg.topic === 'flow-statistics') {
          updateStats(msg.data);
        }
      } catch {}
    };
  }

  // ---- Flow Chart (Chart.js) ----
  function initFlowChart() {
    const ctx = document.getElementById('flow-chart').getContext('2d');
    flowChart = new Chart(ctx, {
      type: 'line',
      data: {
        labels: [],
        datasets: [
          {
            label: 'Bytes/s',
            data: [],
            borderColor: '#06b6d4',
            backgroundColor: 'rgba(6,182,212,0.1)',
            borderWidth: 2,
            pointRadius: 0,
            fill: true,
            tension: 0.3
          },
          {
            label: 'Packets/s',
            data: [],
            borderColor: '#8b5cf6',
            backgroundColor: 'rgba(139,92,246,0.1)',
            borderWidth: 2,
            pointRadius: 0,
            fill: true,
            tension: 0.3,
            yAxisID: 'y1'
          }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: { duration: 200 },
        interaction: { mode: 'index', intersect: false },
        plugins: {
          legend: { display: true, labels: { color: '#94a3b8', font: { size: 11 } } }
        },
        scales: {
          x: { display: true, ticks: { color: '#64748b', maxRotation: 0, maxTicksLimit: 8, font: { size: 10 } }, grid: { color: 'rgba(45,53,85,0.5)' } },
          y: { display: true, position: 'left', ticks: { color: '#06b6d4', font: { size: 10 } }, grid: { color: 'rgba(45,53,85,0.3)' }, title: { display: true, text: 'Bytes/s', color: '#06b6d4', font: { size: 10 } } },
          y1: { display: true, position: 'right', ticks: { color: '#8b5cf6', font: { size: 10 } }, grid: { display: false }, title: { display: true, text: 'Packets/s', color: '#8b5cf6', font: { size: 10 } } }
        }
      }
    });
  }

  let flowCounter = 0;
  let totalBytes = 0;
  let totalPackets = 0;
  const maxPoints = 60;

  function addFlowPoint(flow) {
    flowCounter++;
    totalBytes += flow.flow_bytes_s || flow.avgBytesPerSec || 0;
    totalPackets += flow.flow_packets_s || flow.avgPacketsPerSec || 0;

    const now = new Date().toLocaleTimeString();
    const bytesVal = flow.flow_bytes_s || flow.avgBytesPerSec || 0;
    const pktsVal = flow.flow_packets_s || flow.avgPacketsPerSec || 0;

    flowChart.data.labels.push(now);
    flowChart.data.datasets[0].data.push(bytesVal);
    flowChart.data.datasets[1].data.push(pktsVal);

    if (flowChart.data.labels.length > maxPoints) {
      flowChart.data.labels.shift();
      flowChart.data.datasets[0].data.shift();
      flowChart.data.datasets[1].data.shift();
    }
    flowChart.update('none');

    document.getElementById('flow-count').textContent = flowCounter;
    document.getElementById('avg-bytes').textContent = formatNum(totalBytes / flowCounter);
    document.getElementById('avg-packets').textContent = formatNum(totalPackets / flowCounter);
  }

  // ---- Alert Feed ----
  function addAlert(alert) {
    alertCount++;
    document.getElementById('alert-counter').textContent = alertCount;

    const sev = alert.severity || 'LOW';
    if (severityFilter.current !== 'all' && sev !== severityFilter.current) return;

    // Normalize field names (support both Flink camelCase and Spark snake_case)
    const attackType = alert.attackType || alert.attack_type || 'Unknown';
    const srcIp = alert.srcIp || alert.src_ip || '?';
    const dstIp = alert.dstIp || alert.dst_ip || '?';
    const bytesPerSec = alert.flowBytesPerSec || alert.flowBytesS || alert.bytes_per_sec || 0;
    const pqcEnabled = alert.pqcEnabled != null ? alert.pqcEnabled : alert.pqc_enabled;
    const ts = alert.timestamp || alert.classified_at || Date.now();

    const feed = document.getElementById('alert-feed');
    if (feed.querySelector('.empty-state')) feed.innerHTML = '';

    const item = document.createElement('div');
    item.className = `alert-item ${sev}`;
    item.innerHTML = `
      <span class="alert-severity ${sev}">${sev}</span>
      <div class="alert-body">
        <div class="alert-type">${attackType} Attack${alert.source === 'spark-ml' ? ' <span style="color:#8b5cf6;font-size:0.7rem">[ML]</span>' : ''}</div>
        <div class="alert-detail">${srcIp} → ${dstIp} | ${formatNum(bytesPerSec)} B/s | PQC: ${pqcEnabled ? 'ON' : 'OFF'}</div>
      </div>
      <span class="alert-time">${new Date(ts).toLocaleTimeString()}</span>
    `;
    feed.prepend(item);

    // Limit feed size
    while (feed.children.length > 100) feed.lastChild.remove();
  }

  // ---- Statistics ----
  function updateStats(stats) {
    addFlowPoint(stats);
  }

  // ---- ML Metrics ----
  async function loadMLResults() {
    try {
      const res = await fetch('/api/spark-results');
      const data = await res.json();

      if (data.metrics) {
        document.getElementById('ml-accuracy').textContent = pct(data.metrics.accuracy);
        document.getElementById('ml-precision').textContent = pct(data.metrics.precision);
        document.getElementById('ml-recall').textContent = pct(data.metrics.recall);
        document.getElementById('ml-f1').textContent = pct(data.metrics.f1);
      }

      if (data.featureImportance && data.featureImportance.length > 0) {
        renderImportanceChart(data.featureImportance);
      }
    } catch {}
  }

  function renderImportanceChart(features) {
    const ctx = document.getElementById('importance-chart').getContext('2d');
    if (importanceChart) importanceChart.destroy();

    const sorted = features.sort((a, b) => b.importance - a.importance).slice(0, 8);
    const colors = ['#06b6d4', '#3b82f6', '#8b5cf6', '#ec4899', '#f59e0b', '#10b981', '#ef4444', '#64748b'];

    importanceChart = new Chart(ctx, {
      type: 'bar',
      data: {
        labels: sorted.map(f => f.feature.replace(/_/g, ' ')),
        datasets: [{
          label: 'Importance',
          data: sorted.map(f => f.importance),
          backgroundColor: colors.slice(0, sorted.length),
          borderRadius: 4
        }]
      },
      options: {
        indexAxis: 'y',
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { display: false } },
        scales: {
          x: { ticks: { color: '#64748b', font: { size: 10 } }, grid: { color: 'rgba(45,53,85,0.3)' } },
          y: { ticks: { color: '#94a3b8', font: { size: 10 } }, grid: { display: false } }
        }
      }
    });
  }

  // ---- Health Badges ----
  async function updateHealth() {
    try {
      const res = await fetch('/api/health');
      const data = await res.json();
      ['kafka', 'flink', 'hdfs'].forEach(svc => {
        const el = document.getElementById(`badge-${svc}`);
        const st = data[svc]?.status || 'unknown';
        el.className = `badge badge-${st === 'healthy' ? 'healthy' : 'unreachable'}`;
      });
    } catch {}
  }

  // ---- Controls ----
  function initControls() {
    document.getElementById('btn-pause-flow').addEventListener('click', function() {
      flowPaused = !flowPaused;
      this.textContent = flowPaused ? '▶ Resume' : '⏸ Pause';
    });

    document.getElementById('severity-filter').addEventListener('change', function() {
      severityFilter.current = this.value;
    });

    document.getElementById('sim-pqc').addEventListener('change', function() {
      document.getElementById('pqc-label').textContent = this.checked ? 'Kyber + Dilithium' : 'ECDH + ECDSA';
    });

    document.getElementById('btn-simulate').addEventListener('click', async function() {
      this.disabled = true;
      this.textContent = '⏳ Running...';
      try {
        await fetch('/api/simulate', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            duration: +document.getElementById('sim-duration').value,
            normalFlows: +document.getElementById('sim-normal').value,
            floodFlows: +document.getElementById('sim-flood').value,
            stealthFlows: +document.getElementById('sim-stealth').value,
            burstFlows: +document.getElementById('sim-burst').value,
            pqcEnabled: document.getElementById('sim-pqc').checked
          })
        });
        this.textContent = '✓ Queued';
        setTimeout(() => { this.textContent = '▶ Start Simulation'; this.disabled = false; }, 3000);
      } catch {
        this.textContent = '✗ Failed';
        setTimeout(() => { this.textContent = '▶ Start Simulation'; this.disabled = false; }, 2000);
      }
    });

    document.getElementById('btn-refresh-ml').addEventListener('click', loadMLResults);
  }

  // ---- Helpers ----
  function formatNum(n) {
    if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
    if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
    return Math.round(n).toString();
  }
  function pct(v) { return v != null ? (v * 100).toFixed(1) + '%' : '—'; }

  // ---- Init ----
  document.addEventListener('DOMContentLoaded', () => {
    initFlowChart();
    initControls();
    connectWS();
    loadMLResults();
    updateHealth();
    setInterval(updateHealth, 15000);
  });
})();
