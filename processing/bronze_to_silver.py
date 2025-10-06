from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, when
import os, sys

os.environ["PYSPARK_PYTHON"] = sys.executable


PROJECT_DIR = os.environ.get("PROJECT_DIR", "/opt/airflow/project")

builder = (
    SparkSession.builder.appName("ai4i-silver")
    .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

bronze = f"{PROJECT_DIR}/lakehouse/bronze/sensor_readings"
silver = f"{PROJECT_DIR}/lakehouse/silver/events_enriched"


df = spark.read.format("delta").load(bronze)


df = (
    df.withColumn("temp_diff", col("process_temperature_k") - col("air_temperature_k"))
      .withColumn("power", col("torque_nm") * col("rotational_speed_rpm"))
      .withColumn("wear_torque", col("tool_wear_min") * col("torque_nm"))
)

if "type" in df.columns:
    df = (
        df.withColumn("type_l", when(lower(col("type")) == "l", 1).otherwise(0))
          .withColumn("type_m", when(lower(col("type")) == "m", 1).otherwise(0))
          .drop("type")
    )

df.write.format("delta").mode("overwrite").save(silver)

spark.read.format("delta").load(silver).show(5, truncate=False)
spark.stop()