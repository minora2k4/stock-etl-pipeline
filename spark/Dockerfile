FROM apache/spark:3.5.0-scala2.12-java11-python3-ubuntu

USER root

# 1. Cài đặt các công cụ cần thiết
RUN apt-get update && apt-get install -y \
    curl \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# 2. TẢI JARS TỪ MAVEN CENTRAL
WORKDIR /opt/spark/jars

# --- Nhóm 1: Kafka Dependencies ---
RUN curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# --- Nhóm 2: MinIO/S3 Dependencies (Hadoop 3.3.4 + AWS SDK Bundle) ---
# Lưu ý: Spark 3.5.0 dùng Hadoop 3.3.4, version này tương thích tốt nhất với AWS Bundle 1.12.262
RUN curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# 3. Setup Python Environment
COPY spark/requirements.txt /opt/spark/work-dir/
WORKDIR /opt/spark/work-dir

RUN pip3 install --upgrade pip setuptools wheel && \
    pip3 install --no-cache-dir -r requirements.txt

# 4. Setup quyền và thư mục
RUN mkdir -p /opt/spark/work-dir/checkpoint && \
    chmod -R 777 /opt/spark/work-dir

# 5. Copy Code
COPY spark/processor.py .

USER spark