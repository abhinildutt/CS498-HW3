#!/usr/bin/env python3
"""
Script to read data from Google Cloud Bigtable.
"""
from google.cloud import bigtable

# Configuration
PROJECT_ID = 'cs498-451021'
INSTANCE_ID = 'ev-bigtable'
TABLE_ID = 'ev-population'
COLUMN_FAMILY_ID = 'ev_info'

# Initialize Bigtable client
client = bigtable.Client(project=PROJECT_ID, admin=True)
instance = client.instance(INSTANCE_ID)
table = instance.table(TABLE_ID)

print("Reading rows from the table...")

# Try to read any rows from the table
rows = table.read_rows(limit=5)

count = 0
for row in rows:
    print(f"\nRow key: {row.row_key.decode()}")
    for column_family, cells in row.cells.items():
        print(f"Column Family: {column_family}")
        for column, cell in cells.items():
            try:
                print(f"  Column: {column.decode()}")
                print(f"  Value: {cell[0].value.decode()}")
            except UnicodeDecodeError:
                print(f"  Column: {column.decode()}")
                print(f"  Value: <binary data>")
    count += 1

if count == 0:
    print("No rows found in the table.")
else:
    print(f"\nFound {count} rows in the table.") 