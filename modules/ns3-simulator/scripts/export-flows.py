#!/usr/bin/env python3
"""
PQG6 — Flow Exporter
Reads NS-3 simulation output (JSON flow files) and:
  1. Publishes each flow to Kafka topic 'network-flows' (real-time pipeline)
  2. Uploads ground-truth CSV to HDFS (for Spark ML training)
"""

import os
import sys
import json
import glob
import time
import requests
from confluent_kafka import Producer

# ---- Configuration ----
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC_FLOWS", "network-flows")
HDFS_NAMENODE = os.environ.get("HDFS_NAMENODE", "hdfs://namenode:9000")
HDFS_WEBHDFS_URL = HDFS_NAMENODE.replace("hdfs://", "http://").replace(":9000", ":9870")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/opt/pqg6/output")


def delivery_callback(err, msg):
    """Kafka delivery confirmation callback."""
    if err:
        print(f"[ERROR] Kafka delivery failed: {err}", file=sys.stderr)
    else:
        print(f"[OK] Sent flow to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")


def publish_flows_to_kafka(flows_dir: str):
    """Read JSON flow files and publish each to Kafka."""
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "client.id": "pqg6-ns3-exporter",
        "acks": "all",
    })

    json_files = sorted(glob.glob(os.path.join(flows_dir, "flow-*.json")))
    print(f"[INFO] Found {len(json_files)} flow files to publish")

    for fpath in json_files:
        with open(fpath, "r") as f:
            flow_data = json.load(f)

        # Add export metadata
        flow_data["exported_at"] = int(time.time() * 1000)  # Unix ms
        flow_data["source"] = "ns3-simulator"

        key = str(flow_data.get("flow_id", "unknown"))
        value = json.dumps(flow_data)

        producer.produce(
            topic=KAFKA_TOPIC,
            key=key.encode("utf-8"),
            value=value.encode("utf-8"),
            callback=delivery_callback,
        )
        producer.poll(0)  # Trigger delivery callbacks

    # Flush remaining messages
    remaining = producer.flush(timeout=30)
    if remaining > 0:
        print(f"[WARN] {remaining} messages still in queue after flush", file=sys.stderr)

    print(f"[INFO] Published {len(json_files)} flows to Kafka topic '{KAFKA_TOPIC}'")


def upload_csv_to_hdfs(csv_path: str):
    """Upload ground-truth CSV to HDFS via WebHDFS REST API."""
    hdfs_path = "/pqg6/training/ground-truth.csv"
    url = f"{HDFS_WEBHDFS_URL}/webhdfs/v1{hdfs_path}?op=CREATE&overwrite=true"

    print(f"[INFO] Uploading {csv_path} to HDFS {hdfs_path}")

    try:
        # Step 1: Initiate create (returns redirect to DataNode)
        resp = requests.put(url, allow_redirects=False, timeout=10)
        if resp.status_code == 307:
            # Step 2: Follow redirect and upload data
            redirect_url = resp.headers["Location"]
            with open(csv_path, "rb") as f:
                upload_resp = requests.put(redirect_url, data=f, timeout=60)
                if upload_resp.status_code == 201:
                    print(f"[OK] Uploaded to HDFS: {hdfs_path}")
                else:
                    print(f"[ERROR] HDFS upload failed: {upload_resp.status_code} {upload_resp.text}",
                          file=sys.stderr)
        else:
            print(f"[ERROR] HDFS create failed: {resp.status_code} {resp.text}", file=sys.stderr)
    except requests.exceptions.ConnectionError:
        print("[WARN] HDFS not reachable — skipping CSV upload (run with HDFS profile)", file=sys.stderr)


def main():
    flows_dir = os.path.join(OUTPUT_DIR, "flows")
    csv_path = os.path.join(OUTPUT_DIR, "ground-truth.csv")

    # Wait for output files to exist
    retries = 0
    while not os.path.exists(csv_path) and retries < 30:
        print(f"[INFO] Waiting for simulation output... ({retries})")
        time.sleep(2)
        retries += 1

    if not os.path.exists(csv_path):
        print("[ERROR] No simulation output found. Did the simulation run?", file=sys.stderr)
        sys.exit(1)

    print(f"[INFO] Simulation output found at {OUTPUT_DIR}")

    # Publish flows to Kafka
    if os.path.isdir(flows_dir):
        publish_flows_to_kafka(flows_dir)
    else:
        print(f"[WARN] No flows directory at {flows_dir}", file=sys.stderr)

    # Upload CSV to HDFS
    upload_csv_to_hdfs(csv_path)

    print("[INFO] Export complete.")


if __name__ == "__main__":
    main()
