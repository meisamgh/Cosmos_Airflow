# dags/transform_data_dag.py
import os
import asyncio
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
import asyncpg

# Load environment variables from .env
load_dotenv()

# Get configuration from environment variables
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_USER = os.getenv("DB_USER", "pipeline_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "pipeline_password")
DB_NAME = os.getenv("DB_NAME", "pipeline_db")


async def transform_data_async():
    """
    Asynchronously:
      - Creates a connection pool.
      - Drops the transformed_data table if it exists.
      - Creates a new transformed_data table by transforming data from raw_data.
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

    async with pool.acquire() as connection:
        await connection.execute("DROP TABLE IF EXISTS transformed_data")
        await connection.execute("""
            CREATE TABLE transformed_data AS
            SELECT 
                id,
                UPPER(name) AS name,
                value * 100 AS value
            FROM raw_data
        """)
    await pool.close()


def transform_data():
    """Synchronous wrapper to run the asynchronous transformation process."""
    asyncio.run(transform_data_async())


with DAG(
        dag_id="transform_data_dag",
        start_date=datetime(2023, 1, 1),
        schedule_interval="@daily",
        catchup=False,
        description="Asynchronous DAG to transform data from raw_data to transformed_data",
) as dag:
    transform_task = PythonOperator(
        task_id="transform_data_task",
        python_callable=transform_data,
    )

    transform_task
