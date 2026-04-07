# 🚀 Step-by-Step Execution Guide (Kafka → Spark → Hive → Tableau)

This section explains how to run the complete real-time ETL pipeline from scratch.

---

## 🧠 Pipeline Flow

Kafka → Spark Streaming → Bronze Layer → Silver Layer → Hive → Tableau

---

## 📌 Prerequisites

- Java installed
- Kafka installed
- Spark installed
- Hive installed
- Python 3 installed
- Tableau (for visualization)

---

# 🔥 STEP 1: Start Kafka & Zookeeper

```bash✔️ What happens:
Starts Zookeeper (port 2181)
Starts Kafka Broker (port 9092)
Waits until both services are ready
🔥 STEP 2: Create Kafka Topic
bash create_topic.sh
✔️ What happens:
Checks if topic exists (fraudTopic)
Creates topic if not present
Configures partitions & retention
🔥 STEP 3: Create Hive Tables
hive -f hivetable_creation.hive
✔️ What happens:
Creates database fraud_db
Creates:
Bronze table (raw data)
Silver table (cleaned, partitioned data)
🔥 STEP 4: Run Full Pipeline
bash master_script.sh
✔️ What happens internally:
Starts Kafka services
Creates topic
Creates Hive tables
Starts Spark Streaming consumer
Starts Kafka producer
Streams data continuously
Writes data into:
/data/fraud/bronze/
/data/fraud/silver/
🔥 STEP 5: Verify Data in Hive
USE fraud_db;
SELECT * FROM fraud_silver LIMIT 10;
⚠️ IMPORTANT: MSCK REPAIR TABLE (VERY IMPORTANT)
📌 What is MSCK REPAIR?

Spark writes partitioned data like:

/data/fraud/silver/class=0/
/data/fraud/silver/class=1/

👉 Hive does NOT automatically detect new partitions.

👉 MSCK REPAIR TABLE loads these partitions into Hive metadata.

🔥 When to run?
Option 1 (Manual)

After data is written:

MSCK REPAIR TABLE fraud_silver;
Option 2 (Automated - Recommended)

Already included in master_script.sh

sleep 20
hive -e "USE fraud_db; MSCK REPAIR TABLE fraud_silver;"
Option 3 (Continuous Refresh)
while true; do
  sleep 30
  hive -e "USE fraud_db; MSCK REPAIR TABLE fraud_silver;"
done &
⚠️ Why important?

If not run:

Data exists in HDFS ❌
But Hive cannot see it ❌
Tableau will show incomplete data ❌
📊 STEP 6: Connect Tableau
🔹 Connection Details
Host: localhost
Port: 10000
Database: fraud_db
Table: fraud_silver
🔹 Dashboards to Create
Fraud vs Non-Fraud count
Transactions over time
Fraud rate KPI
Amount distribution
🧠 FINAL EXECUTION FLOW
start_kafka.sh
   ↓
create_topic.sh
   ↓
hivetable_creation.hive
   ↓
master_script.sh
   ↓
Kafka Producer → Kafka Topic → Spark Consumer
   ↓
Bronze Layer (Raw Data)
   ↓
Silver Layer (Clean Data - Partitioned)
   ↓
MSCK REPAIR TABLE
   ↓
Hive Query Layer
   ↓
Tableau Dashboard
⚠️ KEY NOTES
Always start consumer before producer
Always run MSCK REPAIR after partitioned writes
Tableau should connect to fraud_silver, not bronze
This pipeline is near real-time, not fully real-time
🎯 PROJECT RESULT
End-to-end streaming ETL pipeline
Data lake architecture (Bronze → Silver)
Fault-tolerant streaming using checkpointing
Tableau dashboards for analytics
bash start_kafka.sh
