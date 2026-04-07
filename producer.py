import pandas as pd
import json
import time
from datetime import datetime
from kafka import KafkaProducer

# -----------------------------
# CONFIG
# -----------------------------
TOPIC = "fraudTopic"
BOOTSTRAP_SERVERS = "localhost:9092"
SCALE = 0.01

# -----------------------------
# PRODUCER CONFIG
# -----------------------------
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: str(k).encode("utf-8"),
    linger_ms=10,
    batch_size=16384,
    acks="all",
    retries=5
)

# -----------------------------
# LOAD DATA
# -----------------------------
df = pd.read_csv("creditcard.csv")
df = df.sort_values("Time")

prev_time = None

# -----------------------------
# STREAMING SIMULATION
# -----------------------------
for current_time, group in df.groupby("Time"):

    # Simulate event-time delay
    if prev_time is not None:
        gap = (current_time - prev_time) * SCALE
        if gap > 0:
            time.sleep(gap)

    records = group.to_dict(orient="records")  # ✅ faster than iterrows

    for record in records:

        # -----------------------------
        # ENRICH DATA (VERY IMPORTANT)
        # -----------------------------
        record["event_time"] = float(record["Time"])
        record["ingestion_time"] = datetime.utcnow().isoformat()

        try:
            producer.send(
                TOPIC,
                key=record["Class"],
                value=record
            )
        except Exception as e:
            print(f"❌ Error sending record: {e}")

    print(f"Sent {len(records)} transactions at time {current_time}")

    prev_time = current_time

# -----------------------------
# ENSURE DELIVERY
# -----------------------------
producer.flush()
producer.close()
