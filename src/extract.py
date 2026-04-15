import requests
import pandas as pd
import os
import json
import time
from google.oauth2 import service_account
from google.cloud import bigquery
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- CONFIGURATION ---
PROJECT_ID = "f1-data-lakehouse"
KEY_PATH = "gcp_credentials.json"  # Still used for local runs
BASE_URL = "https://api.jolpi.ca/ergast/f1"

def fetch_f1_data(endpoint):
    """Fetches data from Jolpica and flattens the nested JSON into a DataFrame"""
    all_results = []
    limit = 100
    offset = 0
    total = 1 
    
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

        total = int(data['MRData']['total'])
        table_key = next((k for k in data['MRData'] if 'Table' in k), None)
        list_key = next((k for k in data['MRData'][table_key].keys() if isinstance(data['MRData'][table_key][k], list)), None)

        batch_data = data['MRData'][table_key][list_key]
        all_results.extend(batch_data)
        
        print(f"Fetched {len(all_results)} of {total} records...")
        offset += limit

    df = pd.json_normalize(all_results)
    return sanitize_column_names(df)

def upload_to_bigquery(df, table_name):
    """Pushes DataFrame to BigQuery using either local or GitHub Secrets credentials."""
    if df.empty:
        print(f"Skipping {table_name}: No data.")
        return

    # 1. AUTHENTICATION LOGIC: Check for GitHub Action Secret first
    env_key = os.environ.get('GCP_SA_KEY')
    
    if env_key:
        print("Using service account key from environment variables (GitHub Actions)...")
        # Parse the JSON string from the environment variable
        info = json.loads(env_key)
        credentials = service_account.Credentials.from_service_account_info(info)
    else:
        print(f"Using local key file: {KEY_PATH}")
        credentials = service_account.Credentials.from_service_account_file(KEY_PATH)

    client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
    target_table = f"{PROJECT_ID}.raw_f1_data.{table_name}"
    
    print(f"Uploading {len(df)} rows to BigQuery: {target_table}...")
    
    # Using WRITE_TRUNCATE to refresh data daily
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df, target_table, job_config=job_config)
    job.result()
    
    print(f"Successfully loaded {target_table}!")

def sanitize_column_names(df):
    """Replace dots in column names with underscores for BigQuery compatibility."""
    df.columns = df.columns.str.replace('.', '_', regex=False)
    return df

if __name__ == "__main__":
    ENDPOINTS = ['drivers', 'circuits', 'constructors', 'results', 'seasons', 'status']
    
    for ep in ENDPOINTS:
        try:
            df = fetch_f1_data(ep)
            upload_to_bigquery(df, f"raw_{ep}")
        except Exception as e:
            print(f"Failed to process {ep}: {e}")