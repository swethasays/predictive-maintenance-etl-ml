from kafka import KafkaProducer
import json, time, random

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

while True:
    msg = {
        "machine_id": random.randint(1000, 1020),
        "air_temp": round(random.uniform(295, 305), 2),
        "process_temp": round(random.uniform(305, 315), 2),
        "torque": round(random.uniform(20, 80), 2),
        "rot_speed": random.randint(1200, 3000),
        "tool_wear": random.randint(0, 250),
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }

    producer.send("sensor_readings", msg)
    print("Sent:", msg)
    time.sleep(1)