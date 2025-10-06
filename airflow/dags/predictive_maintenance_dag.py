from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
import subprocess, os, sys, glob

PROJECT_ROOT = "/opt/airflow/project"

def run(cmd: str):
    print("Running:", cmd)
    env = {
        **os.environ,
        "PYSPARK_PYTHON": sys.executable,
        "PROJECT_DIR": PROJECT_ROOT,
    }
    subprocess.run(cmd, shell=True, check=True, cwd=PROJECT_ROOT, env=env)

# Function to check if new data (.parquet) exists
def any_parquet_present() -> bool:
    pattern = os.path.join(PROJECT_ROOT, "data/stream_data", "*.parquet")
    return len(glob.glob(pattern)) > 0

default_args = {"owner": "you", "retries": 1, "retry_delay": timedelta(minutes=2)}

# Define the DAG
with DAG(
    dag_id="predictive_maintenance_etl_ml",
    start_date=datetime(2025, 10, 1),
    schedule=None,  # manual run only
    catchup=False,
    default_args=default_args,
    tags=["predictive-maintenance"],
) as dag:

    # Step 1: wait until some parquet file exists
    wait_for_data = PythonSensor(
        task_id="wait_for_stream_batch",
        python_callable=any_parquet_present,
        poke_interval=30,  # check every 30 sec
        timeout=60 * 30,   # stop after 30 min
    )

    # Step 2–5: ETL tasks
    bronze = PythonOperator(
        task_id="to_delta_bronze",
        python_callable=run,
        op_args=["python processing/to_delta_bronze.py"],
    )

    silver = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=run,
        op_args=["python processing/bronze_to_silver.py"],
    )

    export_csv = PythonOperator(
        task_id="export_silver_csv",
        python_callable=run,
        op_args=["python processing/export_silver_csv.py"],
    )

    score = PythonOperator(
        task_id="batch_score",
        python_callable=run,
        op_args=["python scoring/batch_score.py"],
    )


    wait_for_data >> bronze >> silver >> export_csv >> score