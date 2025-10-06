from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
import os, sys

os.environ["PYSPARK_PYTHON"] = sys.executable
PROJECT_DIR = os.environ.get("PROJECT_DIR", "/opt/airflow/project")

builder = (
    SparkSession.builder.appName("export-silver-csv")
    .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

silver = f"{PROJECT_DIR}/lakehouse/silver/events_enriched"
out_csv = f"{PROJECT_DIR}/data/ai4i_stream_silver.csv"

df = spark.read.format("delta").load(silver)
df.coalesce(1).write.mode("overwrite").option("header", True).csv(out_csv)

print(f"Exported {out_csv}")
spark.stop()