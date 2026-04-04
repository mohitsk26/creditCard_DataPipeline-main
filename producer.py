import pandas as pd
import json
import time
from kafka import KafkaProducer

# -----------------------------
# CONFIG
# -----------------------------
TOPIC = "fraudTopic"   # ✅ Fixed topic name
BOOTSTRAP_SERVERS = "localhost:9092"
SCALE = 0.01  # speed factor (controls real-time simulation)

# -----------------------------
# PRODUCER CONFIG (Optimized)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,

    # Convert dict → JSON → bytes
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),

    # Optional: key serializer
    key_serializer=lambda k: str(k).encode("utf-8"),

    #  PERFORMANCE SETTINGS
    linger_ms=10,            # wait to batch messages
    batch_size=16384,         # batch size (16KB)
    # RELIABILITY SETTINGS
    acks="all",              #  strongest durability
    retries=5,               # retry on failure
    max_in_flight_requests_per_connection=5
)

# -----------------------------
# LOAD DATA

df = pd.read_csv("creditcard.csv")

# Ensure correct event-time order
df = df.sort_values("Time")

prev_time = None

# -----------------------------
# STREAMING SIMULATION

for current_time, group in df.groupby("Time"):

    # Simulate real-time delay
    if prev_time is not None:
        gap = (current_time - prev_time) * SCALE
        if gap > 0:
            time.sleep(gap)

    # Send records
    for _, row in group.iterrows():
        producer.send(
            TOPIC,
            key=row["Class"],          # partition key (fraud/non-fraud)
            value=row.to_dict()
        )

    print(f"Sent {len(group)} transactions at time {current_time}")

    prev_time = current_time

# -----------------------------
# ENSURE DELIVERY
producer.flush()
producer.close()
