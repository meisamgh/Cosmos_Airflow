# dags/ingest_data_dag.py
import os
import csv
import asyncio
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
import asyncpg
import aiofiles

# Load environment variables from .env
load_dotenv()

# Get configuration from environment variables
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_USER = os.getenv("DB_USER", "pipeline_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "pipeline_password")
DB_NAME = os.getenv("DB_NAME", "pipeline_db")
DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")
CSV_FILE_NAME = os.getenv("CSV_FILE", "sample_data.csv")
CSV_FILE_PATH = os.path.join(DATA_DIR, CSV_FILE_NAME)

# Batch size to process the CSV file incrementally
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))


async def ingest_data_async():
    """
    Asynchronously:
      - Creates a connection pool to PostgreSQL.
      - Ensures the raw_data table exists.
      - Reads the CSV file in batches.
      - Inserts each batch into the raw_data table.
    """
    pool = await asyncpg.create_pool(
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        host=DB_HOST,
        port=DB_PORT,
        min_size=1,
        max_size=10,
    )

    # Create table if it does not exist.
    async with pool.acquire() as connection:
        await connection.execute("""
            CREATE TABLE IF NOT EXISTS raw_data (
                id INT,
                name TEXT,
                value NUMERIC
            )
        """)

    # Process the CSV file in batches.
    async with aiofiles.open(CSV_FILE_PATH, mode="r") as f:
        # Read the header line.
        header_line = await f.readline()
        header = header_line.strip().split(",")

        batch = []
        # Read file line by line asynchronously.
        async for line in f:
            if line.strip():
                batch.append(line.strip())
            if len(batch) >= BATCH_SIZE:
                # Process the current batch.
                reader = csv.DictReader(batch, fieldnames=header)
                rows = []
                for row in reader:
                    try:
                        rows.append((int(row["id"]), row["name"], float(row["value"])))
                    except Exception as e:
                        print(f"Skipping row due to error: {row} - {e}")
                async with pool.acquire() as connection:
                    await connection.executemany(
                        "INSERT INTO raw_data (id, name, value) VALUES ($1, $2, $3)",
                        rows,
                    )
                batch = []  # Reset batch after processing.

        # Process any remaining lines.
        if batch:
            reader = csv.DictReader(batch, fieldnames=header)
            rows = []
            for row in reader:
                try:
                    rows.append((int(row["id"]), row["name"], float(row["value"])))
                except Exception as e:
                    print(f"Skipping row due to error: {row} - {e}")
            async with pool.acquire() as connection:
                await connection.executemany(
                    "INSERT INTO raw_data (id, name, value) VALUES ($1, $2, $3)",
                    rows,
                )

    await pool.close()


def ingest_data():
    """Synchronous wrapper to run the asynchronous ingestion process."""
    asyncio.run(ingest_data_async())


with DAG(
        dag_id="ingest_data_dag",
        start_date=datetime(2023, 1, 1),
        schedule_interval="@daily",
        catchup=False,
        description="Asynchronous DAG to ingest CSV data into PostgreSQL using batch processing",
) as dag:
    ingest_task = PythonOperator(
        task_id="ingest_data_task",
        python_callable=ingest_data,
    )

    ingest_task
