# Data Pipeline with Airflow, Cosmos, and Delta Live Tables

This repository demonstrates an end‑to‑end data pipeline that:

- Uses **Cosmos** to orchestrate a local Airflow environment.
- Ingests CSV data into a PostgreSQL database using an asynchronous Airflow DAG that processes data in batches.
- Transforms the ingested data with another asynchronous DAG.
- Includes a sample **Delta Live Tables (DLT)** pipeline for Databricks.
- Uses environment variables stored in a `.env` file.
- Contains basic tests to ensure DAGs load correctly.

## Repository Structure

````bash
my-data-pipeline/
├── dags/
│   ├── ingest_data_dag.py       # Asynchronous DAG to ingest CSV data into PostgreSQL
│   └── transform_data_dag.py    # Asynchronous DAG to transform the ingested data
├── dlt_pipeline/
│   ├── pipeline.py              # Delta Live Tables (DLT) pipeline for Databricks
│   └── sample_data.csv          # Sample CSV file for data ingestion
├── tests/
│   └── test_dags.py             # Tests to ensure DAGs load correctly
├── .env                       # Environment variables (DB credentials, file paths, etc.)
├── cosmos.yaml                # Cosmos configuration file to orchestrate services
├── README.md                  # Project documentation and instructions
└── requirements.txt           # Python dependencies
````


## Setup

1. **Install Dependencies**

   ```bash
   pip install -r requirements.txt

2. **Configure Environment**

- Edit the `.env` file if necessary.
- Ensure the sample CSV file (`sample_data.csv`) is placed in the directory defined by DATA_DIR.



3. **Using Cosmos:**
- Install Cosmos if not already installed:

```bash
pip install cosmos-cli
```

- Start the environment:

 ```bash
 bash
cosmos run
```
- Access the Airflow UI at http://localhost:8080

4. **Running Tests:**

 ```bash
 bash
pytest tests/
```


