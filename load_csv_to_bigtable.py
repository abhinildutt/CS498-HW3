#!/usr/bin/env python3
"""
Script to load CSV data into Google Cloud Bigtable.
"""
import csv
import datetime
import time
from google.cloud import bigtable
from google.cloud.bigtable import column_family
from google.cloud.bigtable import row_filters

# Configuration
PROJECT_ID = 'cs498-451021'
INSTANCE_ID = 'ev-bigtable'
TABLE_ID = 'ev-population'
COLUMN_FAMILY_ID = 'ev_info'
CSV_FILE = 'Electric_Vehicle_Population_Data.csv'

client = bigtable.Client(project=PROJECT_ID, admin=True)
instance = client.instance(INSTANCE_ID)
table = instance.table(TABLE_ID)

if not table.exists():
    table.create(column_families={COLUMN_FAMILY_ID: column_family.MaxVersionsGCRule(1)})

start_time = time.time()
total_rows = 0
last_print_time = time.time()

with open(CSV_FILE, 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        # Create unique row key using VIN and counter
        row_key = f"{row['VIN (1-10)']}_{total_rows}".encode()
        row_data = table.row(row_key)
        
        for column, value in row.items():
            if value:  # Only add non-empty values
                row_data.set_cell(
                    COLUMN_FAMILY_ID,
                    column.encode(),
                    value.encode(),
                    timestamp=datetime.datetime.now(datetime.UTC)
                )
        
        table.mutate_rows([row_data])
        total_rows += 1
        
        # Print progress every 5 seconds
        current_time = time.time()
        if current_time - last_print_time >= 5:
            elapsed = current_time - start_time
            rows_per_second = total_rows / elapsed
            print(f"Processed {total_rows} rows... ({rows_per_second:.2f} rows/second)")
            last_print_time = current_time

end_time = time.time()
total_time = end_time - start_time
print(f"\nLoaded {total_rows} rows into Bigtable table {TABLE_ID}.")
print(f"Total time: {total_time:.2f} seconds")
print(f"Average speed: {total_rows/total_time:.2f} rows/second")