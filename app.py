#!/usr/bin/env python3
"""
Flask API for interacting with Google Cloud Bigtable containing EV population data.
"""
from flask import Flask, jsonify
from google.cloud import bigtable
from google.cloud.bigtable import column_family
from google.cloud.bigtable import row_filters

#configuration
PROJECT_ID = 'cs498-451021'
INSTANCE_ID = 'ev-bigtable'
TABLE_ID = 'ev-population'

app = Flask(__name__)

def get_bigtable_client(): # initialize bigtable client
    return bigtable.Client(project=PROJECT_ID, admin=True)

@app.route('/rows', methods=['GET'])
def get_total_rows():
    client = get_bigtable_client()
    instance = client.instance(INSTANCE_ID)
    table = instance.table(TABLE_ID)
    
    # filter to just get row data
    row_filter = row_filters.CellsColumnLimitFilter(1)
    
    # Count rows by reading them with the filter
    row_count = 0
    rows = table.read_rows(filter_=row_filter)
    for row in rows:
        row_count += 1
    
    return str(row_count)

@app.route('/Best-BMW', methods=['GET'])
def get_best_bmw():
    client = get_bigtable_client()
    instance = client.instance(INSTANCE_ID)
    table = instance.table(TABLE_ID)
    
    family_filter = row_filters.FamilyNameRegexFilter(b'ev_info')
    
    # Read the rows with just the family filter
    rows = table.read_rows(filter_=family_filter)
    
    # Process the rows to find BMW EVs with range > 100
    count = 0
    for row in rows:
        make = None
        electric_range = None
        
        for column_family_id, columns in row.cells.items():
            for column_qualifier, cells in columns.items():
                if column_qualifier == b'Make':
                    make = cells[0].value.decode('utf-8')
                elif column_qualifier == b'Electric Range':
                    try:
                        electric_range = float(cells[0].value.decode('utf-8'))
                    except ValueError:
                        continue
        
        if make == 'BMW' and electric_range is not None and electric_range > 100:
            count += 1
    
    return str(count)

@app.route('/tesla-owners', methods=['GET'])
def get_tesla_in_seattle():
    """Retrieve the count of all Tesla vehicles registered in Seattle."""
    client = get_bigtable_client()
    instance = client.instance(INSTANCE_ID)
    table = instance.table(TABLE_ID)
    
    family_filter = row_filters.FamilyNameRegexFilter(b'ev_info')
    
    rows = table.read_rows(filter_=family_filter)
    
    # Count Tesla vehicles in Seattle
    count = 0
    for row in rows:
        make = None
        city = None
        
        for column_family_id, columns in row.cells.items():
            for column_qualifier, cells in columns.items():
                if column_qualifier == b'Make':
                    make = cells[0].value.decode('utf-8')
                elif column_qualifier == b'City':
                    city = cells[0].value.decode('utf-8')
        
        if make == 'TESLA' and city == 'Seattle':
            count += 1
    
    return str(count)

@app.route('/update', methods=['POST'])
def update_vehicle(): # update the electric range of the vehicle with DOL Vehicle ID 257246118 to 200 miles
    client = get_bigtable_client()
    instance = client.instance(INSTANCE_ID)
    table = instance.table(TABLE_ID)
    
    # First, find the row with the specified DOL Vehicle ID
    family_filter = row_filters.FamilyNameRegexFilter(b'ev_info')
    
    # Read the rows with just the family filter
    rows = table.read_rows(filter_=family_filter)
    
    target_row_key = None
    for row in rows:
        for column_family_id, columns in row.cells.items():
            for column_qualifier, cells in columns.items():
                if column_qualifier == b'DOL Vehicle ID':
                    if cells[0].value.decode('utf-8') == '257246118':
                        target_row_key = row.row_key
                        break
    
    if target_row_key:
        # Update the Electric Range for this row
        row_to_update = table.direct_row(target_row_key)
        row_to_update.set_cell(
            'ev_info',
            b'Electric Range',
            b'200',
            timestamp=None
        )
        row_to_update.commit()
        
        return "Success"
    else:
        return "Vehicle not found", 404

@app.route('/delete', methods=['DELETE'])
def delete_old_vehicles(): 
    # delete all records where the model year is less than 2014 and retrieve the count of remaining records
    client = get_bigtable_client()
    instance = client.instance(INSTANCE_ID)
    table = instance.table(TABLE_ID)
    
    # First, identify rows to delete by finding those with model year < 2014
    family_filter = row_filters.FamilyNameRegexFilter(b'ev_info')
    
    # Read the rows with just the family filter
    rows = table.read_rows(filter_=family_filter)
    
    row_keys_to_delete = []
    for row in rows:
        model_year = None
        
        for column_family_id, columns in row.cells.items():
            for column_qualifier, cells in columns.items():
                if column_qualifier == b'Model Year':
                    try:
                        model_year = int(cells[0].value.decode('utf-8'))
                    except ValueError:
                        continue
        
        if model_year is not None and model_year < 2014:
            row_keys_to_delete.append(row.row_key)
    
    # Delete the identified rows
    rows_deleted = 0
    for row_key in row_keys_to_delete:
        row_to_delete = table.direct_row(row_key)
        row_to_delete.delete()
        row_to_delete.commit()
        rows_deleted += 1
    
    # Count remaining rows
    row_filter = row_filters.CellsColumnLimitFilter(1)
    rows = table.read_rows(filter_=row_filter)
    remaining_count = 0
    for row in rows:
        remaining_count += 1
    
    return str(remaining_count)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000, debug=True) 