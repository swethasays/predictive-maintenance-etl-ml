from kafka import KafkaConsumer
import json, pandas as pd, os, time
from datetime import datetime

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
OUT_DIR = os.path.join(os.getenv("PROJECT_DIR", "/opt/airflow/project"), "data/stream_data")
os.makedirs(OUT_DIR, exist_ok=True)

consumer = KafkaConsumer(
    "sensor_readings",
    bootstrap_servers=[BOOTSTRAP_SERVERS],
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="etl-consumer-group"
)

buffer, batch_size, max_secs = [], 100, 30
last_flush = time.time()
print("Listening for messages ...")

def flush():
    global buffer, last_flush
    if not buffer: 
        return
    df = pd.DataFrame(buffer)
    # ensure stable column order (helps Parquet schema)
    cols = ["machine_id","air_temp","process_temp","torque","rot_speed","tool_wear","timestamp"]
    for c in cols:
        if c not in df: df[c] = None
    df = df[cols]
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = f"{OUT_DIR}/sensor_data_batch_{ts}.parquet"
    df.to_parquet(path, engine="pyarrow", index=False)
    print(f"Saved {len(buffer)} messages to {path}")
    buffer, last_flush = [], time.time()

for msg in consumer:
    buffer.append(msg.value)
    if len(buffer) >= batch_size or (time.time() - last_flush) >= max_secs:
        flush()