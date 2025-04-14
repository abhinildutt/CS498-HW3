# CSV to Google Cloud Bigtable Loader

This script loads CSV data into a Google Cloud Bigtable instance.

## Prerequisites

1. Google Cloud SDK installed and configured
2. Python 3.7 or higher
3. Required Python packages (install using `pip install -r requirements.txt`)
4. A Google Cloud Platform account with Bigtable enabled
5. Appropriate permissions to access and modify Bigtable instances

## Setup

1. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

2. Authenticate with Google Cloud:
   ```
   gcloud auth application-default login
   ```

3. Edit the `load_csv_to_bigtable.py` script to update the following configuration variables:
   - `PROJECT_ID`: Your GCP project ID
   - `INSTANCE_ID`: Your Bigtable instance ID
   - `TABLE_ID`: The name of the table where data will be loaded
   - `CSV_FILE`: Path to your CSV file (default is set to `Electric_Vehicle_Population_Data.csv`)

## Usage

Run the script:
```
python load_csv_to_bigtable.py
```

The script will:
1. Create the Bigtable table if it doesn't exist
2. Process the CSV file in chunks to handle large datasets efficiently
3. Create a row in Bigtable for each row in the CSV file
4. Use the first column (VIN) as the row key
5. Store all CSV columns as column qualifiers under the 'ev_data' column family

## Customization

- Adjust the `chunk_size` variable to optimize memory usage based on your file size
- Modify the column family name or GC policy as needed
- Change the row key generation logic if needed 