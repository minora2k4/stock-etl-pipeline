# Real-time Financial Data Pipeline


## 1. Project Overview

This project implements an end-to-end Big Data engineering pipeline capable of ingesting, processing, storing, and visualizing real-time financial market data (Crypto & US Stocks). It demonstrates a modern "Lakehouse" architecture using open-source technologies to handle high-velocity WebSocket streams.

**Core Capabilities:**
* **Ingestion:** Low-latency streaming from Finnhub.io via WebSocket.
* **Buffering:** Decoupled data production and consumption using Apache Kafka.
* **Processing:** Near real-time ETL using Apache Spark Structured Streaming.
* **Storage:** Scalable Data Lake storage using MinIO (S3-compatible) with Parquet partitioning.
* **Analytics:** Serverless SQL analysis on raw files using DuckDB.
* **Visualization:** Interactive, auto-refreshing dashboard using Streamlit.

---

## 2. System Architecture

The pipeline follows a linear data flow:

<img width="1306" height="296" alt="image" src="https://github.com/user-attachments/assets/ec2f4a3e-a356-400d-b4ce-71b598095d4e" />


---

## 3. Data Schema

### 3.1 Raw Input (WebSocket)
Data received from Finnhub is in JSON format:
```json
{
  "type": "trade",
  "data": [
    {
      "p": 67000.50,  // Price
      "s": "BINANCE:BTCUSDT",  // Symbol
      "v": 0.005,  // Volume
      "t": 1708500000000  // Timestamp (ms)
    }
  ]
}
```

### 3.2 Processed Output (Data Lake)
<img width="266" height="342" alt="image" src="https://github.com/user-attachments/assets/3632b472-1edc-41ee-9d6f-0728ccb6d8b4" />

## Prerequisites

* Docker and Docker Compose installed on your machine.
* A free API Key from Finnhub.io.

## Configuration

1.  Create a file named `.env` in the root directory.
2.  Add the following configuration variables:

FINNHUB_API_TOKEN=your_finnhub_api_key_here
FINNHUB_TICKERS=BINANCE:BTCUSDT,BINANCE:ETHUSDT,BINANCE:SOLUSDT,AAPL,TSLA,MSFT,NVDA
KAFKA_SERVER=kafka:9092
KAFKA_TOPIC=stock-ticks

## Installation and Running

### 1. Build and Start Services

Run the following command in the root directory to build the Docker images and start the containers:

docker-compose up --build -d

### 2. Verify Services

Check if all containers are running:

docker-compose ps

You should see the following services running:
* zookeeper
* kafka
* minio
* minio-setup (exits after configuration)
* kafka-producer
* spark-master
* spark-worker
* spark-job
* duckdb-analytics
* bi-dashboard

### 3. Accessing Interfaces

* **Streamlit Dashboard**: http://localhost:8501
* **MinIO Console**: http://localhost:9001 (User: minioadmin, Pass: minioadmin)
* **Spark Master UI**: http://localhost:8080
* **Kafka UI** (if configured): http://localhost:8082

## Service Details

### Kafka Producer
Located in `kafka/`. This service connects to the Finnhub WebSocket. It handles connection retries, subscribes to the tickers defined in `.env`, and serializes the incoming JSON data before sending it to the Kafka topic.

### Spark Processor
Located in `spark/`. This job submits a Spark application that:
* Subscribes to the Kafka topic.
* Parses the JSON payload.
* Calculates total trade value.
* Partitions data by date and symbol.
* Writes data to `s3a://warehouse/processed_trades` in Parquet format.
* Trigger interval: 5 seconds.

### Data Visualization (Streamlit)
Located in `duckdb/`. The application uses a Separation of Concerns pattern:
* `analytics.py`: Handles DuckDB connections, S3/MinIO configuration, and SQL queries.
* `data_visualization.py`: Handles the UI layout, charts (Plotly), and auto-refresh logic.


## Customization

To add more stocks or crypto pairs, simply edit the `FINNHUB_TICKERS` variable in the `.env` file and restart the producer container:

docker-compose restart kafka-producer
