# Formula 1 Enterprise ELT Pipeline

An end-to-end data engineering pipeline designed to extract Formula 1 race data from the Jolpica-F1 API, load it into a Google BigQuery data warehouse, and transform it using dbt for analytical readiness.

## Architecture
The project utilizes a Modern Data Stack (MDS) approach to handle batch data ingestion and transformation:

* **Orchestration:** GitHub Actions for scheduled execution and CI/CD.
* **Extraction:** Python-based ingestion logic utilizing REST API pagination and Pandas for data normalization.
* **Data Warehouse:** Google BigQuery (Cloud-native storage).
* **Transformation:** dbt (Data Build Tool) for modular SQL modeling and testing.
* **Visualization:** Google Looker Studio for interactive business intelligence.

## Project Status
* **Phase 1: Ingestion (Complete)** - Python scripts successfully pull raw JSON data from the Jolpica-F1 API, flatten nested structures, and load them into BigQuery landing tables.
* **Phase 2: Modeling (In Progress)** - Implementing dbt models to transform raw data into a Star Schema (Dimensions and Facts).
* **Phase 3: Visualization (Pending)** - Building a public-facing dashboard to track driver performance and constructor standings.

## Setup and Installation

### Prerequisites
* Python 3.9 or higher
* A Google Cloud Platform account with BigQuery enabled
* dbt Cloud or dbt Core installed

### Repository Setup
1. Clone the repository:
   git clone https://github.com/your-username/f1-enterprise-elt.git
   cd f1-enterprise-elt

2. Environment Configuration:
   * Create a Service Account in GCP with the BigQuery Admin role.
   * Download the JSON key and save it as gcp_credentials.json in the project root.
   * Ensure gcp_credentials.json is listed in your .gitignore to prevent credential leakage.

3. Install Dependencies:
   pip install -r requirements.txt

### Execution
To trigger the extraction and load process manually:
python src/extract.py

## Data Pipeline Logic
The pipeline follows a Medallion Architecture:
* **Bronze (Raw):** Immutable landing tables containing direct API responses (e.g., raw_drivers, raw_results).
* **Silver (Staging):** dbt models that handle type-casting, column renaming, and basic deduplication.
* **Gold (Mart):** Final analytical tables optimized for reporting (e.g., fct_race_results, dim_circuits).

## Data Quality and Testing
* **Schema Sanitization:** Automatic conversion of API-provided dot-notation keys into SQL-compatible underscores.
* **Integrity Tests:** (Planned) dbt tests to ensure unique keys and non-null critical fields across the warehouse.