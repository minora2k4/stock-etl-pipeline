# Real-time Financial Data Pipeline

## Project Overview

This project is a comprehensive Big Data pipeline designed to ingest, process, store, and visualize real-time stock and cryptocurrency market data. It leverages a modern data stack including Apache Kafka, Apache Spark, MinIO (S3), DuckDB, and Streamlit.

The system streams live trade data from the Finnhub API via WebSocket, queues it in Kafka, processes it using Spark Structured Streaming, stores it as partitioned Parquet files in a Data Lake (MinIO), and provides a real-time analytics dashboard using Streamlit powered by DuckDB.

## Architecture

1.  **Data Source**: Finnhub.io API (WebSocket).
2.  **Ingestion Layer**: Python-based Kafka Producer connecting to WebSocket and publishing to the `stock-ticks` topic.
3.  **Message Queue**: Apache Kafka (running with Zookeeper).
4.  **Processing Layer**: Apache Spark Structured Streaming.
    * Reads from Kafka.
    * Performs schema enforcement and data transformation.
    * Writes data to MinIO in Parquet format with partitioning (Date/Symbol).
5.  **Storage Layer**: MinIO (Object Storage compatible with AWS S3).
6.  **Serving Layer**: DuckDB (In-memory OLAP database) reading directly from Parquet files in MinIO.
7.  **Visualization Layer**: Streamlit Dashboard for real-time monitoring, charts, and tape reading.

## Directory Structure

vnstock-pipeline/
├── duckdb/
│   ├── analytics.py          # Data access layer (DuckDB connection & SQL logic)
│   ├── data_visualization.py # Streamlit UI application
│   ├── Dockerfile            # Shared Docker image for Analytics & Dashboard
│   └── requirements.txt      # Python dependencies for serving layer
├── kafka/
│   ├── config.py             # Configuration loader
│   ├── producer.py           # Main WebSocket client & Kafka producer
│   ├── utils.py              # Logging and data formatting utilities
│   ├── Dockerfile            # Docker image for the Producer
│   └── requirements.txt      # Dependencies for Producer
├── spark/
│   ├── processor.py          # Spark Structured Streaming job
│   └── Dockerfile            # Custom Spark image (optional)
├── .env                      # Environment variables (API Keys, Config)
├── docker-compose.yml        # Orchestration for all services
└── README.md                 # Project documentation

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
