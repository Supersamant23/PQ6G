const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const { Kafka } = require('kafkajs');
const fetch = require('node-fetch');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const PORT = process.env.PORT || 3000;
const KAFKA_BROKERS = (process.env.KAFKA_BOOTSTRAP_SERVERS || 'kafka:9092').split(',');
const HDFS_NAMENODE = process.env.HDFS_NAMENODE || 'hdfs://namenode:9000';
// WebHDFS REST runs on port 9870 (HTTP), not the RPC port
const WEBHDFS_URL = process.env.WEBHDFS_URL || (() => {
  try { const u = new URL(HDFS_NAMENODE); return `http://${u.hostname}:9870`; }
  catch { return 'http://namenode:9870'; }
})();

// ---- Serve static frontend ----
app.use(express.static(path.join(__dirname, '..', 'public')));
app.use(express.json());

// ---- Kafka Consumer ----
const kafka = new Kafka({
  clientId: 'pqg6-dashboard',
  brokers: KAFKA_BROKERS,
  retry: { initialRetryTime: 3000, retries: 10 }
});

const consumer = kafka.consumer({ groupId: 'pqg6-dashboard-group' });
const recentAlerts = [];
const recentStats = [];
const MAX_HISTORY = 200;

async function startKafka() {
  try {
    await consumer.connect();
    await consumer.subscribe({ topics: ['network-flows', 'security-alerts', 'flow-statistics'], fromBeginning: false });

    await consumer.run({
      eachMessage: async ({ topic, message }) => {
        const value = message.value.toString();
        const payload = JSON.stringify({ topic, data: JSON.parse(value), ts: Date.now() });

        // Cache recent messages
        if (topic === 'security-alerts') {
          recentAlerts.unshift(JSON.parse(value));
          if (recentAlerts.length > MAX_HISTORY) recentAlerts.pop();
        } else if (topic === 'flow-statistics') {
          recentStats.unshift(JSON.parse(value));
          if (recentStats.length > MAX_HISTORY) recentStats.pop();
        }

        // Broadcast to all connected WebSocket clients
        wss.clients.forEach(client => {
          if (client.readyState === 1) client.send(payload);
        });
      }
    });
    console.log('[Kafka] Connected and consuming topics');
  } catch (err) {
    console.error('[Kafka] Connection failed, retrying in 5s:', err.message);
    setTimeout(startKafka, 5000);
  }
}

// ---- WebSocket ----
wss.on('connection', (ws) => {
  console.log('[WS] Client connected');
  // Send recent history on connect
  ws.send(JSON.stringify({ topic: '_init', alerts: recentAlerts.slice(0, 50), stats: recentStats.slice(0, 20) }));
  ws.on('close', () => console.log('[WS] Client disconnected'));
});

// ---- REST API ----

// Recent alerts
app.get('/api/alerts', (req, res) => {
  res.json(recentAlerts.slice(0, 50));
});

// Recent statistics
app.get('/api/stats', (req, res) => {
  res.json(recentStats.slice(0, 20));
});

// Spark ML results from HDFS
app.get('/api/spark-results', async (req, res) => {
  try {
    // Helper: read a Spark output directory (contains part-00000 file)
    async function readSparkFile(hdfsDir) {
      // List the directory to find the part-* file
      const listUrl = `${WEBHDFS_URL}/webhdfs/v1${hdfsDir}?op=LISTSTATUS`;
      const listResp = await fetch(listUrl);
      const listData = await listResp.json();
      const files = listData.FileStatuses?.FileStatus || [];
      const partFile = files.find(f => f.pathSuffix.startsWith('part-'));
      if (!partFile) return null;
      // Read the part file content
      const readUrl = `${WEBHDFS_URL}/webhdfs/v1${hdfsDir}/${partFile.pathSuffix}?op=OPEN`;
      const readResp = await fetch(readUrl);
      return await readResp.text();
    }

    const [metricsText, importText] = await Promise.all([
      readSparkFile('/pqg6/metrics/evaluation.json').catch(() => null),
      readSparkFile('/pqg6/metrics/feature-importance.txt').catch(() => null)
    ]);

    res.json({
      metrics: metricsText ? JSON.parse(metricsText.trim()) : null,
      featureImportance: importText
        ? importText.trim().split('\n').filter(l => l.trim()).map(l => JSON.parse(l))
        : []
    });
  } catch (err) {
    res.json({ metrics: null, featureImportance: [], error: err.message });
  }
});

// System health
app.get('/api/health', async (req, res) => {
  const services = {
    dashboard: { status: 'healthy', uptime: process.uptime() },
    kafka: { status: 'unknown' },
    hdfs: { status: 'unknown' },
    flink: { status: 'unknown' }
  };

  // Check Kafka
  try {
    const admin = kafka.admin();
    await admin.connect();
    const topics = await admin.listTopics();
    services.kafka = { status: 'healthy', topics: topics.length };
    await admin.disconnect();
  } catch { services.kafka.status = 'unreachable'; }

  // Check HDFS
  try {
    const r = await fetch(`${WEBHDFS_URL}/webhdfs/v1/?op=LISTSTATUS`, { timeout: 3000 });
    services.hdfs.status = r.ok ? 'healthy' : 'degraded';
  } catch { services.hdfs.status = 'unreachable'; }

  // Check Flink
  try {
    const r = await fetch('http://flink-jobmanager:8081/overview', { timeout: 3000 });
    if (r.ok) {
      const data = await r.json();
      services.flink = { status: 'healthy', runningJobs: data['jobs-running'] || 0 };
    }
  } catch { services.flink.status = 'unreachable'; }

  res.json(services);
});

// Trigger simulation via Docker Engine API
const DOCKER_SOCKET = '/var/run/docker.sock';

function dockerAPI(apiPath, method = 'GET', body = null) {
  return new Promise((resolve, reject) => {
    const opts = {
      socketPath: DOCKER_SOCKET,
      path: apiPath,
      method,
      headers: { 'Content-Type': 'application/json' },
    };
    const req = http.request(opts, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        resolve({ status: res.statusCode, data: data ? JSON.parse(data) : null });
      });
    });
    req.on('error', reject);
    if (body) req.write(JSON.stringify(body));
    req.end();
  });
}

app.post('/api/simulate', async (req, res) => {
  try {
    const envOverrides = {
      SIM_DURATION: String(req.body.duration || 30),
      SIM_NORMAL_FLOWS: String(req.body.normalFlows || 20),
      SIM_FLOOD_FLOWS: String(req.body.floodFlows || 3),
      SIM_STEALTH_FLOWS: String(req.body.stealthFlows || 3),
      SIM_BURST_FLOWS: String(req.body.burstFlows || 2),
      SIM_PQC_ENABLED: String(req.body.pqcEnabled !== false)
    };

    console.log('[API] Starting NS-3 simulation:', envOverrides);

    // Check if a simulation is already running
    const existingRun = await dockerAPI('/containers/pqg6-sim-run/json');
    if (existingRun.status === 200 && existingRun.data.State.Running) {
      return res.json({ status: 'already-running', message: 'Simulation already in progress' });
    }

    // Get the original ns3-simulator container to find image + base env
    const original = await dockerAPI('/containers/ns3-simulator/json');
    if (original.status === 404) {
      return res.status(503).json({ status: 'error', message: 'NS-3 simulator container not found. Run with --profile full.' });
    }

    const imageName = original.data.Config.Image;
    const baseEnv = original.data.Config.Env || [];

    // Merge: base env + user overrides (overrides win)
    const overrideKeys = new Set(Object.keys(envOverrides));
    const mergedEnv = baseEnv
      .filter(e => !overrideKeys.has(e.split('=')[0]))
      .concat(Object.entries(envOverrides).map(([k, v]) => `${k}=${v}`));

    // Remove old run container if it exists
    if (existingRun.status === 200) {
      await dockerAPI('/containers/pqg6-sim-run?force=true', 'DELETE');
    }

    // Create new container with overridden env
    const createResp = await dockerAPI('/containers/create?name=pqg6-sim-run', 'POST', {
      Image: imageName,
      Env: mergedEnv,
      HostConfig: {
        NetworkMode: 'pqg6-net',
        AutoRemove: false
      }
    });

    if (createResp.status !== 201) {
      return res.status(500).json({ status: 'error', message: `Create failed: ${JSON.stringify(createResp.data)}` });
    }

    // Start the container
    const startResp = await dockerAPI(`/containers/pqg6-sim-run/start`, 'POST');
    if (startResp.status === 204 || startResp.status === 304) {
      res.json({ status: 'started', config: envOverrides, message: 'NS-3 simulation started with custom parameters' });
    } else {
      res.status(500).json({ status: 'error', message: `Start returned ${startResp.status}` });
    }
  } catch (err) {
    console.error('[API] Simulate error:', err.message);
    res.status(500).json({ status: 'error', message: err.message });
  }
});

// Check simulation status
app.get('/api/simulate/status', async (req, res) => {
  try {
    const inspect = await dockerAPI('/containers/pqg6-sim-run/json');
    if (inspect.status === 404) {
      return res.json({ running: false, exists: false });
    }
    const state = inspect.data.State;
    res.json({
      running: state.Running,
      exists: true,
      status: state.Status,
      exitCode: state.ExitCode,
      startedAt: state.StartedAt,
      finishedAt: state.FinishedAt
    });
  } catch (err) {
    res.json({ running: false, exists: false, error: err.message });
  }
});

// ---- Start ----
server.listen(PORT, () => {
  console.log(`[Dashboard] Running on http://0.0.0.0:${PORT}`);
  startKafka();
});
