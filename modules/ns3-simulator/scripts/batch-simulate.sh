#!/bin/bash
# ============================================
# PQG6 — Batch Dataset Generator
# ============================================
# Runs repeated NS-3 simulations with varied parameters
# to accumulate a large per-packet dataset.
#
# Usage:
#   ./batch-simulate.sh [NUM_RUNS] [OUTPUT_CSV]
#
# Example:
#   ./batch-simulate.sh 100 /opt/pqg6/output/dataset.csv
#   → Generates ~100 × 5-20MB = 0.5-2GB of per-packet data

set -e

NUM_RUNS="${1:-50}"
OUTPUT_CSV="${2:-/opt/pqg6/output/dataset.csv}"
SIM_BINARY="/opt/ns3/build/scratch/pqg6/ns3.41-pqg6-sim"
TEMP_DIR="/opt/pqg6/output/batch-temp"

mkdir -p "$TEMP_DIR"

# Write CSV header once
echo "timestamp_s,flow_id,src_ip,dst_ip,port,packet_size_bytes,direction,attack_type,pqc_enabled,kem_handshake_ms,sig_sign_ms,sig_verify_ms" > "$OUTPUT_CSV"

echo "============================================"
echo "PQG6 Batch Dataset Generator"
echo "  Runs:     ${NUM_RUNS}"
echo "  Output:   ${OUTPUT_CSV}"
echo "============================================"

# Parameter pools for variation
DURATIONS=(30 60 90 120)
NORMAL_FLOWS=(100 150 200 300 400)
FLOOD_FLOWS=(20 40 60 80)
STEALTH_FLOWS=(15 30 50 60)
BURST_FLOWS=(10 20 40 50)
PQC_OPTIONS=("true" "false")

TOTAL_ROWS=0
START_TIME=$(date +%s)

for RUN in $(seq 1 "$NUM_RUNS"); do
    # Pick random parameters for this run
    DURATION=${DURATIONS[$((RANDOM % ${#DURATIONS[@]}))]}
    NORMAL=${NORMAL_FLOWS[$((RANDOM % ${#NORMAL_FLOWS[@]}))]}
    FLOOD=${FLOOD_FLOWS[$((RANDOM % ${#FLOOD_FLOWS[@]}))]}
    STEALTH=${STEALTH_FLOWS[$((RANDOM % ${#STEALTH_FLOWS[@]}))]}
    BURST=${BURST_FLOWS[$((RANDOM % ${#BURST_FLOWS[@]}))]}
    PQC=${PQC_OPTIONS[$((RANDOM % ${#PQC_OPTIONS[@]}))]}

    TOTAL_FLOWS=$((NORMAL + FLOOD + STEALTH + BURST))
    TEMP_PKT="$TEMP_DIR/run-${RUN}.csv"

    echo ""
    echo "--- Run $RUN / $NUM_RUNS ---"
    echo "  Duration=${DURATION}s  Flows=${TOTAL_FLOWS} (N=${NORMAL} F=${FLOOD} S=${STEALTH} B=${BURST})  PQC=${PQC}"

    # Run simulation with per-packet logging
    "$SIM_BINARY" \
        --duration="$DURATION" \
        --normalFlows="$NORMAL" \
        --floodFlows="$FLOOD" \
        --stealthFlows="$STEALTH" \
        --burstFlows="$BURST" \
        --pqc="$PQC" \
        --outputDir="$TEMP_DIR" \
        --packetLog="$TEMP_PKT" \
        2>/dev/null

    # Append data (skip header) to master CSV
    if [ -f "$TEMP_PKT" ]; then
        ROWS=$(tail -n +2 "$TEMP_PKT" | wc -l)
        tail -n +2 "$TEMP_PKT" >> "$OUTPUT_CSV"
        TOTAL_ROWS=$((TOTAL_ROWS + ROWS))
        SIZE_MB=$(du -m "$OUTPUT_CSV" | cut -f1)
        echo "  → ${ROWS} packets (total: ${TOTAL_ROWS} rows, ${SIZE_MB} MB)"
        rm -f "$TEMP_PKT"
    else
        echo "  → [WARN] No packet log generated"
    fi

    # Clean up temp flow files
    rm -rf "$TEMP_DIR/flows/" "$TEMP_DIR/ground-truth.csv"
done

# Final stats
END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))
FINAL_SIZE=$(du -h "$OUTPUT_CSV" | cut -f1)

echo ""
echo "============================================"
echo "Dataset Generation Complete"
echo "  Total runs:    ${NUM_RUNS}"
echo "  Total rows:    ${TOTAL_ROWS}"
echo "  File size:     ${FINAL_SIZE}"
echo "  Time elapsed:  ${ELAPSED}s"
echo "  Output:        ${OUTPUT_CSV}"
echo "============================================"
