from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
import os, sys
os.environ["PYSPARK_PYTHON"] = sys.executable

builder = (
    SparkSession.builder.appName("ai4i-bronze")
    .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

src = "data/stream_data/*.parquet"
bronze = "/Users/swetha/predictive-maintenance-etl-ml/lakehouse/bronze/sensor_readings"

df = spark.read.parquet(src)
df = (df
    .withColumnRenamed("air_temp","air_temperature_k")
    .withColumnRenamed("process_temp","process_temperature_k")
    .withColumnRenamed("rot_speed","rotational_speed_rpm")
    .withColumnRenamed("torque","torque_nm")
    .withColumnRenamed("tool_wear","tool_wear_min")
)
df.write.format("delta").mode("append").save(bronze)

spark.read.format("delta").load(bronze).show(5, truncate=False)
spark.stop()