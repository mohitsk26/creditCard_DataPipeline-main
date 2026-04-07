#!/bin/bash

KAFKA_HOME=~/kafka
BOOTSTRAP_SERVER=localhost:9092
TOPIC=fraudTopic

PARTITIONS=3
REPLICATION_FACTOR=1   # keep 1 for local, but explain 3 in interview

echo "Checking if topic exists..."

EXISTS=$($KAFKA_HOME/bin/kafka-topics.sh \
  --list \
  --bootstrap-server $BOOTSTRAP_SERVER | grep -w $TOPIC)

if [ "$EXISTS" == "$TOPIC" ]; then
    echo "✅ Topic already exists."
else
    echo "Creating topic $TOPIC..."

    $KAFKA_HOME/bin/kafka-topics.sh \
      --bootstrap-server $BOOTSTRAP_SERVER \
      --create \
      --topic $TOPIC \
      --partitions $PARTITIONS \
      --replication-factor $REPLICATION_FACTOR \
      --config retention.ms=604800000 \
      --config segment.ms=86400000

    if [ $? -ne 0 ]; then
        echo "❌ Failed to create topic"
        exit 1
    fi

    echo "✅ Topic created successfully"
fi

echo "Verifying topic..."
$KAFKA_HOME/bin/kafka-topics.sh \
  --describe \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --topic $TOPIC
