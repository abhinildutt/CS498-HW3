#!/usr/bin/env python3
"""
Script to load CSV data into Google Cloud Bigtable.
"""
import os
import pandas as pd
from google.cloud import bigtable
from google.cloud.bigtable import column_family
from google.cloud.bigtable import row

# Configuration
PROJECT_ID = 'cs498-451021'
INSTANCE_ID = 'ev-bigtable'
TABLE_ID = 'ev-population'
CSV_FILE = 'Electric_Vehicle_Population_Data.csv'

# Initialize Bigtable client
client = bigtable.Client(project=PROJECT_ID, admin=True)
instance = client.instance(INSTANCE_ID)
table = instance.table(TABLE_ID)

# Read CSV in chunks to handle large files
chunk_size = 1000
chunks = pd.read_csv(CSV_FILE, chunksize=chunk_size)

total_rows = 0
for i, chunk in enumerate(chunks):
    print(f"Processing chunk {i+1}...")
    
    rows_to_insert = []
    for idx, row_data in chunk.iterrows():
        row_key = str(row_data.iloc[0]).encode()
        if not row_key or row_key == b'nan':
            row_key = f"row-{total_rows + idx}".encode()
        
        direct_row = row.DirectRow(row_key)
        
        for col_name, value in row_data.items():
            if pd.isna(value):
                continue
                
            column = f"{col_name}".encode()
            cell_value = str(value).encode()
            
            direct_row.set_cell(
                'ev_data',
                column,
                cell_value,
                timestamp=None
            )
        
        rows_to_insert.append(direct_row)
    
    if rows_to_insert:
        table.mutate_rows(rows_to_insert)
        total_rows += len(rows_to_insert)
        print(f"Inserted {len(rows_to_insert)} rows. Total rows inserted: {total_rows}")

print(f"Completed loading {total_rows} rows to Bigtable table {TABLE_ID}") 