import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, to_timestamp, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Schema khớp 100% với Producer gửi lên
schema = StructType([
    StructField("event_time", StringType()),
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("volume", DoubleType()),
    StructField("side", StringType()),
    StructField("source", StringType())
])


def run_spark_job():
    spark = (SparkSession.builder
             .appName("FinnhubProcessor")
             .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
             .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
             .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
             .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
             .config("spark.sql.streaming.checkpointLocation", "s3a://warehouse/checkpoints")
             .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")

    try:
        # 1. Đọc Kafka
        df = (spark.readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", "kafka:9092")
              .option("subscribe", "stock-ticks")
              .option("startingOffsets", "earliest")
              .load())

        # 2. Parse & Transform
        df_parsed = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

        df_final = (df_parsed
                    .withColumn("event_timestamp", to_timestamp(col("event_time")))
                    .withColumn("total_value", expr("price * volume"))
                    .withColumn("date_partition", to_date(col("event_timestamp"))))

        # 3. Ghi xuống MinIO (Path: processed_trades)
        query = (df_final.writeStream
                 .format("parquet")
                 .option("path", "s3a://warehouse/processed_trades")
                 .partitionBy("date_partition", "symbol")
                 .outputMode("append")
                 .trigger(processingTime="5 seconds")  # Trigger 5s là tối ưu cho Parquet
                 .start())

        print(">>> SPARK PIPELINE RUNNING: Kafka -> processed_trades <<<")
        query.awaitTermination()

    except Exception as e:
        print(f"CRITICAL ERROR: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    run_spark_job()