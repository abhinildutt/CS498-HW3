#!/usr/bin/env python3
"""
Script to delete all rows from Google Cloud Bigtable.
"""
from google.cloud import bigtable

# Configuration
PROJECT_ID = 'cs498-451021'
INSTANCE_ID = 'ev-bigtable'
TABLE_ID = 'ev-population'

def delete_all_rows():
    # Initialize Bigtable client
    client = bigtable.Client(project=PROJECT_ID, admin=True)
    instance = client.instance(INSTANCE_ID)
    table = instance.table(TABLE_ID)

    print("Deleting all rows from the table...")

    # Read all row keys
    rows = table.read_rows()
    row_keys = [row.row_key for row in rows]

    # Delete rows in batches
    batch_size = 100
    for i in range(0, len(row_keys), batch_size):
        batch = row_keys[i:i + batch_size]
        mutations = []
        for key in batch:
            row = table.row(key)
            row.delete()
            mutations.append(row)
        table.mutate_rows(mutations)
        print(f"Deleted {len(batch)} rows...")

    print("All rows have been deleted successfully.")

if __name__ == "__main__":
    delete_all_rows() 