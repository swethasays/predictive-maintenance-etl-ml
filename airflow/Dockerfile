FROM apache/airflow:2.9.3

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
      openjdk-17-jre-headless procps \
    && apt-get clean && rm -rf /var/lib/apt/lists/*


USER airflow
RUN python -m pip install --no-cache-dir \
      pyspark==3.5.1 \
      delta-spark==3.2.0 \
      pyarrow pandas joblib xgboost kafka-python

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64