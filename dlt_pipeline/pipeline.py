# dlt_pipeline/pipeline.py
import os
import dlt
import pyspark.sql.functions as F
from dotenv import load_dotenv

# Optionally load environment variables (in Databricks you might manage configs differently)
load_dotenv()

@dlt.table(
    comment="This table ingests raw data from a CSV file using environment variables for file path configuration"
)
def raw_data():
    data_dir = os.getenv("DATA_DIR", "/dbfs/data")
    csv_file = os.getenv("CSV_FILE", "sample_data.csv")
    path = f"{data_dir}/{csv_file}"
    return spark.read.format("csv").option("header", "true").load(path)

@dlt.table(
    comment="This table transforms the raw data: converting names to uppercase and scaling values"
)
def transformed_data():
    df = dlt.read("raw_data")
    return df.withColumn("name", F.upper(F.col("name"))) \
             .withColumn("value", F.col("value").cast("float") * 100)
