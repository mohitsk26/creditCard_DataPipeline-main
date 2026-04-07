#!/bin/bash

set -e  # stop on error

echo "🚀 Starting Kafka..."
bash start_kafka.sh

echo "📌 Creating Topic..."
bash create_topic.sh

echo "📊 Creating Hive Tables..."
hive -f hivetable_creation.hive

echo "⏳ Waiting for services to stabilize..."
sleep 5

echo "⚡ Starting Spark Consumer..."

spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
kafka_to_hive.py > consumer.log 2>&1 &

CONSUMER_PID=$!

echo "✅ Consumer started (PID: $CONSUMER_PID)"

sleep 5

echo "📤 Starting Kafka Producer..."

python3 producer.py > producer.log 2>&1 &

PRODUCER_PID=$!

echo "✅ Producer started (PID: $PRODUCER_PID)"

echo "📡 Streaming pipeline is running..."
echo "Logs:"
echo "👉 tail -f consumer.log"
echo "👉 tail -f producer.log"

wait
