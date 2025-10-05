from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
import os, sys
os.environ["PYSPARK_PYTHON"] = sys.executable

builder = (
    SparkSession.builder.appName("export-silver")
    .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

silver = "/Users/swetha/predictive-maintenance-etl-ml/lakehouse/silver/events_enriched"
out = "/Users/swetha/predictive-maintenance-etl-ml/data/ai4i_stream_silver.csv"

df = spark.read.format("delta").load(silver)
df.coalesce(1).toPandas().to_csv(out, index=False)

spark.stop()
print(f"Exported {out}")