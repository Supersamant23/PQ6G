#!/bin/bash

# Exit on error
set -e

echo "Starting DataNode..."

# Wait for NameNode to be ready
echo "Waiting for NameNode to be ready..."
while ! nc -z namenode 9000; do
    sleep 2
done

echo "NameNode is ready. Starting DataNode service..."
hdfs datanode
