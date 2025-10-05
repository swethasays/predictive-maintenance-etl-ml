from kafka import KafkaConsumer
import json, pandas as pd, os
from datetime import datetime

TOPIC_NAME = "sensor_readings"
BOOTSTRAP_SERVERS = "localhost:9092"

OUT_DIR = "/Users/swetha/predictive-maintenance-etl-ml/data/stream_data"
os.makedirs(OUT_DIR, exist_ok=True)

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[BOOTSTRAP_SERVERS],
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="latest", 
    enable_auto_commit=True,
    group_id="etl-consumer-group"
)

buffer, batch_size = [], 10
print(f"Listening for messages on topic '{TOPIC_NAME}'...")

for msg in consumer:
    buffer.append(msg.value)

    if len(buffer) >= batch_size:
        df = pd.DataFrame(buffer)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        batch_path = f"{OUT_DIR}/sensor_data_batch_{ts}.parquet"
        df.to_parquet(batch_path, engine="pyarrow", index=False)
        print(f"Saved {len(buffer)} messages to {batch_path}")
        buffer = []