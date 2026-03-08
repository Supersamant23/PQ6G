# PQG6 — Post-Quantum 6G Network Security Analytics

> A hybrid security layer combining NIST-standardized post-quantum cryptography (PQC) with real-time, stream-based intrusion detection for next-generation 6G infrastructure.

**Group 17 · Computer Security Final Project**  
Ansh Raj Rath · Abiruth S. · Aditya Krishna Samant · Dinesh Karthikeyan

---

## Overview

PQG6 simulates a 6G network environment protected by quantum-resistant cryptography (Kyber-768 + Dilithium-3 via liboqs), generates labeled attack traffic using NS-3, streams it through Apache Kafka, detects attacks in real-time with Apache Flink, trains an ML classifier with Apache Spark, and visualizes everything on a live dashboard.

### Architecture

```
[NS-3 Simulator + liboqs PQC]
        ↓ JSON flow events
[Kafka (KRaft)] ←→ [Flink Real-Time IDS] → security-alerts / flow-statistics
        ↓
[Spark MLlib Training + Streaming Classifier]
        ↓
[HDFS (model, metrics, alerts)]
        ↓
[Node.js Dashboard + WebSocket → Browser UI]
```

---

## Modules

| Module | Tech | Purpose |
|---|---|---|
| `modules/ns3-simulator` | C++, NS-3.41, liboqs | Simulates 6G network with real PQC handshakes |
| `modules/flink-processor` | Scala, Flink 1.18 | Real-time rule-based attack detection |
| `modules/spark-analytics` | Scala, Spark 3.5.1 | Random Forest training + streaming classifier |
| `modules/hdfs-storage` | Hadoop 3.3.6 | Persistent storage for training data and models |
| `modules/dashboard` | Node.js, Chart.js | Live WebSocket dashboard with Kafka consumer |
| `stellar-aldrin/` | HTML/CSS/JS | Presentation slides (14 slides) |

---

## Quick Start

### Prerequisites

- Docker + Docker Compose v2
- 12 GB+ RAM (full profile), 3 GB (dev profile)

### Build All Images

```bash
make build
```

### Run (Local)

```bash
# Full stack (~10-11 GB RAM)
make up

# Dev mode: Kafka + Flink + Dashboard only (~3 GB)
make up-dev

# ML mode: Kafka + Spark + HDFS only (~7 GB)
make up-ml
```

### Stop

```bash
make down
```

---

## Deployment Profiles

| Profile | Services | RAM | Use Case |
|---|---|---|---|
| `full` | All | ~10–11 GB | End-to-end demo |
| `dev` | Kafka + Flink + Dashboard | ~3 GB | Real-time pipeline dev |
| `ml` | Kafka + Spark + HDFS | ~7 GB | ML training workflows |

---

## Common Operations

```bash
# Run NS-3 simulation (generates flow data → Kafka + HDFS)
make simulate

# Train Spark ML model (standard)
make train-ml

# Train on large dataset (29 GB)
make train-ml-bigdata

# Upload dataset to HDFS
make upload-dataset

# Classify flows (streaming)
make classify

# Health check all services
make test

# Tail logs
make logs

# View running containers
make ps

# Remove all volumes
make clean
```

---

## Service Endpoints

| Service | URL | Notes |
|---|---|---|
| Dashboard | http://localhost:3000 | Main UI + WebSocket |
| Flink UI | http://localhost:8081 | Job Manager |
| Spark UI | http://localhost:8080 | Spark Master |
| HDFS NameNode UI | http://localhost:9870 | WebHDFS REST API |
| Kafka | localhost:9092 (internal) / 9093 (external) | KRaft mode |

---

## Kafka Topics

| Topic | Partitions | Producer | Consumer |
|---|---|---|---|
| `network-flows` | 3 | NS-3 export script | Flink, Spark |
| `security-alerts` | 1 | Flink, Spark | Dashboard |
| `flow-statistics` | 1 | Flink | Dashboard |

---

## ML Model

- **Algorithm:** Random Forest (100 trees, max depth 10)
- **Classes:** `Normal`, `FLOOD`, `BURST`, `STEALTH`
- **Features (15):** Flow bytes/s, packets/s, IAT stats, packet length stats, flow duration, KEM handshake time, SIG sign/verify time
- **Split:** 80/20 train/test
- **Model path (HDFS):** `/pqg6/models/rf-model`
- **Metrics path (HDFS):** `/pqg6/metrics/evaluation.json`, `/pqg6/metrics/feature-importance.txt`

---

## Dataset

| Traffic Class | Count | Share |
|---|---|---|
| Normal | 11,009 | 68.13% |
| Flood | 1,989 | 12.31% |
| Stealth | 1,632 | 10.10% |
| Burst | 1,530 | 9.47% |
| **Total** | **16,160** | — |

### Generating Larger Datasets

```bash
# Inside the ns3-simulator container
./scripts/batch-simulate.sh 100 /opt/pqg6/output/dataset.csv

# Convert to Parquet for Spark
python3 scripts/convert-to-parquet.py /opt/pqg6/output/dataset.csv /opt/pqg6/output/dataset.parquet
```

---

## PQC Cryptography

Each simulated flow performs a real (not mocked) PQC handshake:

- **KEM:** Kyber-768 (FIPS 203 / ML-KEM) — Module-LWE based key encapsulation
  - Public key: ~1,184 bytes | Ciphertext: ~1,088 bytes
- **Signature:** Dilithium-3 (FIPS 204 / ML-DSA) — M-LWE/SIS based digital signatures
  - Signature size: ~3,293 bytes
- **Symmetric:** AES-256-GCM post-handshake

All implemented via **liboqs 0.10.1** compiled into NS-3 at build time.

---

## Docker Swarm (Production)

For multi-machine cluster deployment across 4 laptops:

```bash
# On manager node
docker swarm init --advertise-addr <YOUR_IP>

# On worker nodes
docker swarm join --token <TOKEN> <MANAGER_IP>:2377

# Label the 24 GB machine
docker node update --label-add ram=24gb <NODE_ID>

# Deploy
make swarm-deploy

# Status
make swarm-status

# Remove
make swarm-rm
```

**Memory allocation:**  
- Manager: NameNode, Kafka, Flink, Spark Master, Dashboard  
- 16 GB workers: Spark workers (8 GB each)  
- 24 GB worker: Spark worker (12 GB)

---

## Attack Traffic Models (NS-3)

| Type | DataRate | PacketSize | On/Off Pattern | Detection |
|---|---|---|---|---|
| FLOOD | 50 Mbps | 1024 B | Constant | `bytes_s > 2M` or `pkts_s > 4K` |
| BURST | 100 Mbps | 1400 B | 0.5s on / 3s off | `bytes_s > 500K` + high IAT std |
| STEALTH | 100 Kbps | 64 B | 0.1s on / ~2s off | `bytes_s < 5K` + `pkt_len < 100` |
| Normal | 5 Mbps | 512 B | 1s on / ~1s off | None |

---

## Project Structure

```
pqg6/
├── Makefile                        # Top-level automation
├── docker-compose.yaml             # Local development compose
├── docker-compose.swarm.yaml       # 4-machine Swarm deployment
├── .env                            # Environment variables (create from .env.example)
├── data/
│   └── ns3-output/                 # Simulation output (CSV + JSON flows)
├── modules/
│   ├── ns3-simulator/
│   │   ├── Dockerfile              # Multi-stage: liboqs → NS-3 → runtime
│   │   ├── scratch/pqg6-sim.cc     # Main simulation C++ source
│   │   └── scripts/                # run-simulation.sh, export-flows.py, batch-simulate.sh
│   ├── flink-processor/
│   │   ├── Dockerfile              # sbt builder → Flink 1.18 runtime
│   │   ├── build.sbt
│   │   └── src/.../FlinkNetworkFlowProcessor.scala
│   ├── spark-analytics/
│   │   ├── Dockerfile              # sbt builder → JRE runtime
│   │   ├── build.sbt
│   │   └── src/.../TrainModel.scala / ClassifyFlows.scala
│   ├── hdfs-storage/
│   │   ├── Dockerfile.hadoop-base / .namenode / .datanode
│   │   ├── config/core-site.xml / hdfs-site.xml
│   │   └── scripts/namenode-entrypoint.sh / datanode-entrypoint.sh
│   └── dashboard/
│       ├── Dockerfile
│       ├── backend/server.js       # Express + KafkaJS + WebSocket + Docker API
│       └── public/                 # index.html, css/quantum-theme.css, js/app.js
└── stellar-aldrin/                 # 14-slide HTML presentation
    ├── index.html
    ├── style.css
    └── script.js
```

---

## Environment Variables

Create a `.env` file at the project root:

```env
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC_FLOWS=network-flows
KAFKA_TOPIC_ALERTS=security-alerts
KAFKA_TOPIC_STATS=flow-statistics
HDFS_NAMENODE=hdfs://namenode:9000
SPARK_MASTER=spark://spark-master:7077
FLINK_OUTPUT_PATH=hdfs://namenode:9000/flink-output
```

---

## Build Details

### NS-3 Simulator (build time: ~30–45 min)

3-stage Docker build:
1. Build **liboqs** from source (Kyber-768, Dilithium-3)
2. Build **NS-3.41** with PQG6 scratch module linked against liboqs
3. Runtime image with Python + confluent-kafka for Kafka export

### Flink Processor

2-stage: `sbt assembly` → fat JAR copied into Flink 1.18 runtime. Hadoop filesystem plugin included for `hdfs://` output support.

### Spark Analytics

2-stage: `sbt assembly` → fat JAR. Jobs submitted via `spark-submit` from the Spark Master container.

---

## Flink Attack Detection Rules

```
FLOOD:   flow_bytes_s > 2,000,000  OR  flow_packets_s > 4,000
BURST:   flow_bytes_s > 500,000    AND flow_iat_std > flow_iat_mean × 0.3
STEALTH: flow_bytes_s < 5,000      AND flow_packets_s < 100  AND fwd_pkt_len_mean < 100
Normal:  everything else
```

Severity thresholds (CRITICAL / HIGH / MEDIUM / LOW) based on bytes/sec and packets/sec.

---

## Dashboard Features

- **Live Flow Monitor** — Chart.js real-time line chart (bytes/s + packets/s)
- **Attack Alert Feed** — Severity-filtered, auto-updating alert list from Kafka
- **ML Analytics Panel** — Accuracy, Precision, Recall, F1 + feature importance bar chart (loaded from HDFS)
- **Control Panel** — Trigger NS-3 simulation with configurable parameters via Docker Engine API
- **Health Badges** — Kafka, Flink, HDFS status polled every 15 seconds

---

## Technology Stack

| Layer | Technology | Version |
|---|---|---|
| Network Simulation | NS-3 | 3.41 |
| Post-Quantum Crypto | liboqs (OQS) | 0.10.1 |
| Message Broker | Apache Kafka (KRaft) | 7.6.0 (Confluent) |
| Stream Processing | Apache Flink | 1.18.1 |
| ML / Batch Processing | Apache Spark | 3.5.1 |
| Distributed Storage | Apache Hadoop HDFS | 3.3.6 |
| Dashboard Backend | Node.js / Express | 20 (LTS) |
| Simulation Language | C++ | 17 |
| Processing Language | Scala | 2.12.18 |
| Scripting | Python | 3 |
| Frontend | Vanilla JS + Chart.js | 4.4.1 |
| Container Runtime | Docker / Docker Swarm | Compose v2 |

---

## Known Limitations & Notes

- The NS-3 build takes 30–45 minutes the first time (C++ compilation + liboqs)
- The `--profile full` requires ~10–11 GB RAM; use `--profile dev` on constrained machines
- The dashboard's `/api/simulate` endpoint requires the Docker socket mounted (done by default in `docker-compose.yaml`)
- Spark ML metrics appear on the dashboard only after running `make train-ml`
- HDFS replication factor is set to 1 (single datanode) for local dev; set `HDFS_REPLICATION=3` in Swarm mode
- The per-packet batch dataset generator (`batch-simulate.sh`) can produce 0.5–2 GB per run; the 29 GB dataset requires ~50–100 runs

---

## Current State (All Phases Complete)

- [x] Phase 1 — Foundation: Docker Compose, Kafka KRaft, HDFS, Makefile
- [x] Phase 2 — Cryptography: liboqs integration, NS-3 PQC handshake per flow
- [x] Phase 3 — Intelligence: Flink detection jobs, Spark RF training + streaming classifier
- [x] Phase 4 — Integration: NS-3 → Kafka pipeline, Dashboard with WebSocket + Docker API

---

## License

Academic project. See individual module dependencies for their respective licenses (Apache 2.0 for Kafka/Flink/Spark/Hadoop, MIT for liboqs, GPL for NS-3).
