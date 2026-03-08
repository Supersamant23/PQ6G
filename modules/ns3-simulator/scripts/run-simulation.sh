#!/bin/bash
# ============================================
# PQG6 — Simulation Runner
# ============================================
# Runs the NS-3 simulation, then exports results to Kafka and HDFS.

set -e

OUTPUT_DIR="${OUTPUT_DIR:-/opt/pqg6/output}"
mkdir -p "$OUTPUT_DIR/flows"

DURATION="${SIM_DURATION:-30}"
NORMAL_FLOWS="${SIM_NORMAL_FLOWS:-20}"
FLOOD_FLOWS="${SIM_FLOOD_FLOWS:-3}"
STEALTH_FLOWS="${SIM_STEALTH_FLOWS:-3}"
BURST_FLOWS="${SIM_BURST_FLOWS:-2}"
PQC_ENABLED="${SIM_PQC_ENABLED:-true}"
PACKET_LOG="${SIM_PACKET_LOG:-false}"

echo "============================================"
echo "PQG6 Simulation Starting"
echo "  Duration:      ${DURATION}s"
echo "  Normal flows:  ${NORMAL_FLOWS}"
echo "  FLOOD flows:   ${FLOOD_FLOWS}"
echo "  STEALTH flows: ${STEALTH_FLOWS}"
echo "  BURST flows:   ${BURST_FLOWS}"
echo "  PQC enabled:   ${PQC_ENABLED}"
echo "  Packet log:    ${PACKET_LOG}"
echo "============================================"

# Build packet log argument
PACKET_LOG_ARG=""
if [ "$PACKET_LOG" = "true" ]; then
    PACKET_LOG_ARG="--packetLog=auto"
fi

# Run NS-3 simulation
echo "[1/2] Running NS-3 simulation..."
/opt/ns3/build/scratch/pqg6/ns3.41-pqg6-sim \
    --duration="$DURATION" \
    --normalFlows="$NORMAL_FLOWS" \
    --floodFlows="$FLOOD_FLOWS" \
    --stealthFlows="$STEALTH_FLOWS" \
    --burstFlows="$BURST_FLOWS" \
    --pqc="$PQC_ENABLED" \
    --outputDir="$OUTPUT_DIR" \
    $PACKET_LOG_ARG

echo "[1/2] Simulation complete."

# Export results to Kafka and HDFS
echo "[2/2] Exporting to Kafka + HDFS..."
python3 /opt/pqg6/scripts/export-flows.py

echo "============================================"
echo "PQG6 Simulation Complete"
echo "  CSV:  ${OUTPUT_DIR}/ground-truth.csv"
echo "  JSON: ${OUTPUT_DIR}/flows/"
echo "============================================"
