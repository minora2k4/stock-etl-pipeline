from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType

# Schema chuẩn
schema = StructType([
    StructField("event_time", StringType()),  # Nhận String trước rồi cast sau
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("volume", LongType()),
    StructField("side", StringType()),
    StructField("source", StringType())
])


def run_spark_job():
    spark = SparkSession.builder \
        .appName("StockProcessor") \
        # Sửa đường dẫn Checkpoint
    .config("spark.sql.streaming.checkpointLocation", "/opt/spark/work-dir/datalake/checkpoints") \
        .getOrCreate()


spark.sparkContext.setLogLevel("WARN")

# Read Kafka (Giữ nguyên)
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "stock-ticks") \
    .option("startingOffsets", "earliest") \
    .load()

# Transform (Giữ nguyên)
df_parsed = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

df_final = df_parsed \
    .withColumn("event_timestamp", to_timestamp(col("event_time"))) \
    .withColumn("total_value", expr("price * volume")) \
    .withColumn("date_partition", expr("to_date(event_timestamp)"))

# Write Stream (Sửa đường dẫn Output)
query = df_final.writeStream \
    .format("parquet") \
    .option("path", "/opt/spark/work-dir/datalake/raw_trades") \
    .partitionBy("date_partition", "symbol") \
    .outputMode("append") \
    .trigger(processingTime="15 seconds") \
    .start()

query.awaitTermination()

if __name__ == "__main__":
    run_spark_job()