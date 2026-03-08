#!/usr/bin/env python3
"""
PQG6 — CSV to Parquet Converter

Converts the accumulated per-packet CSV dataset into Parquet format
for efficient storage and fast analytics with Spark/Pandas.

Usage:
    python3 convert-to-parquet.py [INPUT_CSV] [OUTPUT_PARQUET]

Example:
    python3 convert-to-parquet.py /opt/pqg6/output/dataset.csv /opt/pqg6/output/pq6g-dataset.parquet
"""

import sys
import os

def convert_csv_to_parquet(csv_path: str, parquet_path: str):
    try:
        import pandas as pd
    except ImportError:
        print("[ERROR] pandas is required: pip install pandas pyarrow")
        sys.exit(1)

    try:
        import pyarrow  # noqa: F401
    except ImportError:
        print("[ERROR] pyarrow is required: pip install pyarrow")
        sys.exit(1)

    print(f"Reading CSV: {csv_path}")
    
    # Define proper dtypes for efficiency
    dtypes = {
        "timestamp_s": "float64",
        "flow_id": "uint32",
        "src_ip": "category",
        "dst_ip": "category",
        "port": "uint16",
        "packet_size_bytes": "uint32",
        "direction": "category",
        "attack_type": "category",
        "pqc_enabled": "uint8",
        "kem_handshake_ms": "float64",
        "sig_sign_ms": "float64",
        "sig_verify_ms": "float64",
    }
    
    # Read in chunks for large files
    chunk_size = 1_000_000
    chunks = pd.read_csv(csv_path, dtype=dtypes, chunksize=chunk_size)
    
    df = pd.concat(chunks, ignore_index=True)
    
    total_rows = len(df)
    csv_size_mb = os.path.getsize(csv_path) / (1024 * 1024)
    
    print(f"Loaded {total_rows:,} rows ({csv_size_mb:.1f} MB CSV)")
    print(f"Attack distribution:")
    print(df["attack_type"].value_counts().to_string())
    print()
    
    # Write Parquet with snappy compression
    print(f"Writing Parquet: {parquet_path}")
    df.to_parquet(parquet_path, engine="pyarrow", compression="snappy", index=False)
    
    parquet_size_mb = os.path.getsize(parquet_path) / (1024 * 1024)
    ratio = csv_size_mb / parquet_size_mb if parquet_size_mb > 0 else 0
    
    print(f"Done!")
    print(f"  CSV:     {csv_size_mb:.1f} MB")
    print(f"  Parquet: {parquet_size_mb:.1f} MB ({ratio:.1f}x compression)")
    print(f"  Rows:    {total_rows:,}")


if __name__ == "__main__":
    csv_in = sys.argv[1] if len(sys.argv) > 1 else "/opt/pqg6/output/dataset.csv"
    parquet_out = sys.argv[2] if len(sys.argv) > 2 else "/opt/pqg6/output/pq6g-dataset.parquet"
    
    if not os.path.exists(csv_in):
        print(f"[ERROR] Input file not found: {csv_in}")
        sys.exit(1)
    
    convert_csv_to_parquet(csv_in, parquet_out)
