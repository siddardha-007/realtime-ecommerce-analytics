# Real-Time E-Commerce Analytics

Real-time data pipeline using Python, Apache Kafka, PySpark Structured Streaming, Docker, and PostgreSQL.

## Prerequisites

Install:

* Python 3.11
* Docker Desktop
* VS Code
* Git

---

## 1. Clone the Repository

```powershell
git clone https://github.com/siddardha-007/realtime-ecommerce-analytics
cd realtime-ecommerce-analytics
```

---

## 2. Create Python Virtual Environment

```powershell
python -m venv .venv
```

Activate it:

```powershell
.\.venv\Scripts\Activate.ps1
```

Install dependencies:

```powershell
pip install -r requirements.txt
```

---

## 3. Start Docker Services

Go to the Docker folder:

```powershell
cd docker
```

Start Kafka, Spark and PostgreSQL:

```powershell
docker compose up -d
```

Check containers:

```powershell
docker ps
```

You should see:

```text
ecommerce-kafka
ecommerce-spark
ecommerce-postgres
```

---

## 4. Create Kafka Topic

Create the `orders_raw` topic:

```powershell
docker exec ecommerce-kafka /opt/kafka/bin/kafka-topics.sh `
  --create `
  --topic orders_raw `
  --bootstrap-server localhost:9092 `
  --partitions 3 `
  --replication-factor 1
```

Check that the topic exists:

```powershell
docker exec ecommerce-kafka /opt/kafka/bin/kafka-topics.sh `
  --list `
  --bootstrap-server localhost:9092
```

You should see:

```text
orders_raw
```

Check topic details:

```powershell
docker exec ecommerce-kafka /opt/kafka/bin/kafka-topics.sh `
  --describe `
  --topic orders_raw `
  --bootstrap-server localhost:9092
```


Start Spark

After creating the Kafka topic, re-run only the Spark container:

```powershell
docker compose up -d spark
```

---

## 5. Start Spark Streaming

From the `docker` folder:

```powershell
docker logs -f ecommerce-spark
```

Keep this terminal open.

---

## 6. Start Python Producer

Open a **new VS Code terminal**.

Go to the project root:

```powershell
cd ..
```

Activate the virtual environment:

```powershell
.\.venv\Scripts\Activate.ps1
```

Start the producer:

```powershell
python producer\producer.py
```

The producer will generate a new order approximately every 2 seconds.

---

## 7. Check PostgreSQL

Open another terminal.

From the project root:

```powershell
cd docker
```

Connect to PostgreSQL:

```powershell
docker exec -it ecommerce-postgres psql -U admin -d ecommerce
```

Run:

```sql
SELECT * FROM orders ORDER BY order_id DESC LIMIT 10;
```

Exit:

```sql
\q
```

---

## 8. Stop the Application

Stop the Python producer:

```text
Ctrl + C
```

Stop Docker services:

```powershell
docker compose down
```

---

## Project Flow

```text
Python Producer
      ↓
    Kafka
      ↓
   PySpark
      ↓
 PostgreSQL
```
