import pandas as pd
import numpy as np
import time
import os

def aggregate_large_csv(input_file, output_file, chunk_size=1000000):
    print(f"Starting aggregation of {input_file} to {output_file}")
    start_time = time.time()
    
    # Check if input file exists
    if not os.path.exists(input_file):
        print(f"Error: Input file {input_file} not found.")
        return

    # Delete output file if it exists to start fresh
    if os.path.exists(output_file):
        os.remove(output_file)

    # Initialize variables for continuous aggregation
    aggregated_frames = []
    total_rows_processed = 0
    total_bytes = os.path.getsize(input_file)
    # Estimate total rows (average CSV line is ~135 bytes for this schema)
    estimated_total_rows = total_bytes / 135.0
    print(f"Total file size: {total_bytes / (1024**3):.2f} GB")
    print(f"Estimated total rows: ~{int(estimated_total_rows):,}")

    try:
        # Read the CSV in chunks
        # Only read the columns we need to save memory and speed up reading
        cols_to_use = ['timestamp_s', 'direction', 'packet_size_bytes']
        
        chunk_iter = pd.read_csv(input_file, chunksize=chunk_size, usecols=cols_to_use)
        
        for i, chunk in enumerate(chunk_iter):
            # 1. Round timestamp to nearest integer (1 second buckets)
            chunk['time_bucket'] = chunk['timestamp_s'].astype(int)
            
            # 2. Group by time bucket and direction, summing packet bytes
            agg_chunk = chunk.groupby(['time_bucket', 'direction'])['packet_size_bytes'].sum().reset_index()
            
            # Store the aggregated chunk
            aggregated_frames.append(agg_chunk)
            
            total_rows_processed += len(chunk)
            percent_complete = min(100.0, (total_rows_processed / estimated_total_rows) * 100.0)
            print(f"Processed {total_rows_processed:,} rows (~{percent_complete:.2f}%)... ({time.time() - start_time:.2f}s elapsed)")
            
            # To prevent memory bloat, periodically combine the grouped frames and reduce them
            if len(aggregated_frames) >= 10:
                print("Consolidating aggregated frames...")
                combined = pd.concat(aggregated_frames)
                consolidated = combined.groupby(['time_bucket', 'direction'])['packet_size_bytes'].sum().reset_index()
                aggregated_frames = [consolidated]
                
    except Exception as e:
        print(f"Error during processing: {e}")
        return

    # Final consolidation
    if aggregated_frames:
        print("Performing final consolidation and formatting...")
        combined = pd.concat(aggregated_frames)
        final_df = combined.groupby(['time_bucket', 'direction'])['packet_size_bytes'].sum().reset_index()
        
        # Pivot the table so 'tx' and 'rx' are columns, and index is the time
        # This is the ideal format for Time Series models (esp. VAR)
        pivot_df = final_df.pivot(index='time_bucket', columns='direction', values='packet_size_bytes').fillna(0)
        
        # Rename columns for clarity
        pivot_df.columns.name = None
        if 'rx' in pivot_df.columns and 'tx' in pivot_df.columns:
            pivot_df = pivot_df[['tx', 'rx']] # Ensure order
            pivot_df.rename(columns={'tx': 'tx_bytes_per_sec', 'rx': 'rx_bytes_per_sec'}, inplace=True)
            
            # Add a total bandwidth column
            pivot_df['total_bytes_per_sec'] = pivot_df['tx_bytes_per_sec'] + pivot_df['rx_bytes_per_sec']

        # Save to output file
        pivot_df.to_csv(output_file)
        
        end_time = time.time()
        print(f"\nAggregation complete in {end_time - start_time:.2f} seconds.")
        print(f"Total rows processed: {total_rows_processed:,}")
        print(f"Output saved to: {output_file}")
        print(f"Final aggregated shape: {pivot_df.shape}")
        
        print("\n--- Preview of Aggregated Data ---")
        print(pivot_df.head(10))
    else:
        print("No data processed.")

if __name__ == "__main__":
    input_path = r"data\ns3-output\pq6g-20gb-dataset.csv"
    output_path = r"data\aggregated_ts_bandwidth.csv"
    
    # Ensure data directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    aggregate_large_csv(input_path, output_path, chunk_size=5_000_000)
