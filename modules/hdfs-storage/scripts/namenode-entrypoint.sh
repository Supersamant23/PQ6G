#!/bin/bash

# Exit on error
set -e

echo "Starting NameNode..."

# Format NameNode if not already formatted
if [ ! -d "/hadoop/dfs/name/current" ]; then
    echo "Formatting NameNode..."
    hdfs namenode -format -force -nonInteractive
fi

# Create PQG6-specific HDFS directories after NameNode starts
(
    # Wait for NameNode to be fully ready
    sleep 15
    echo "Creating PQG6 HDFS directories..."
    hdfs dfs -mkdir -p /pqg6/training
    hdfs dfs -mkdir -p /pqg6/models
    hdfs dfs -mkdir -p /pqg6/metrics
    hdfs dfs -mkdir -p /pqg6/predictions
    hdfs dfs -mkdir -p /flink-output/alerts
    hdfs dfs -mkdir -p /flink-output/statistics
    echo "PQG6 HDFS directories created."
) &

# Start NameNode (foreground)
echo "NameNode formatted. Starting NameNode service..."
hdfs namenode
