import requests
import pandas as pd
import os
import time
from google.oauth2 import service_account
from google.cloud import bigquery
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# --- CONFIGURATION ---
PROJECT_ID = "f1-data-lakehouse"
KEY_PATH = "gcp_credentials.json"
BASE_URL = "https://api.jolpi.ca/ergast/f1"

def fetch_f1_data(endpoint):
    """
    Fetches data from Jolpica and flatens the nested JSON into a Dataframe
    """
    all_results = []
    limit = 100
    offset = 0
    total = 1 # Temporary placeholder to start the loop
    
    # Setup retry strategy
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    print(f"--- Starting Paginated Fetch for {endpoint} ---")
    
    while offset < total: 
        url = f"{BASE_URL}/{endpoint}.json?limit={limit}&offset={offset}"
        response = session.get(url)
        response.raise_for_status()
        data = response.json()

        # Update total count from the first API response
        total = int(data['MRData']['total'])

        # Dig into the JSON to find the list of items
        table_key = next((k for k in data['MRData'] if 'Table' in k), None)
        if not table_key:
            raise ValueError(f"Could not find table key in response for {endpoint}")
        print(f"Found Table Key: {table_key}")

        list_key = next((k for k in data['MRData'][table_key].keys() if isinstance(data['MRData'][table_key][k], list)), None)
        if not list_key:
            raise ValueError(f"Could not find list key in response for {endpoint}")
        print(f"Found List Key: {list_key}")

        # 1. Grab the current batch (up to 100 records) and add it to our master list
        batch_data = data['MRData'][table_key][list_key]
        all_results.extend(batch_data)
        
        print(f"Fetched {len(all_results)} of {total} records...")
        
        # 2. Update the offset to get the next page on the next loop
        offset += limit

    # 4. Once the loop is done, convert the list of JSON objects into a flat Pandas Table
    df = pd.json_normalize(all_results)
    return sanitize_column_names(df)

def upload_to_bigquery(df, table_name):
    """Takes the DataFrame and pushes it to BigQuery using BigQuery client."""
    if df.empty:
        print(f"Skipping {table_name}: No data.")
        return

    credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
    client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
    
    target_table = f"{PROJECT_ID}.raw_f1_data.{table_name}"
    
    print(f"Uploading {len(df)} rows to BigQuery: {target_table}...")
    
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df, target_table, job_config=job_config)
    job.result()
    
    print(f"Successfully loaded {target_table}!")

def sanitize_column_names(df):
    """Replace dots in column names with underscores for BigQuery compatibility."""
    df.columns = df.columns.str.replace('.', '_', regex=False)
    return df

if __name__ == "__main__":
    # The list of tables we decided to pull for the MVP
    ENDPOINTS = ['drivers', 'circuits', 'constructors', 'results', 'seasons', 'status']
    
    for ep in ENDPOINTS:
        try:
            # Extract
            df = fetch_f1_data(ep)
            
            # Load
            upload_to_bigquery(df, f"raw_{ep}")
            
        except Exception as e:
            print(f"Failed to process {ep}: {e}")