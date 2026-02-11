import json
import time
import sys
import websocket
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import config
from utils import logger, map_finnhub_to_schema


class FinnhubProducer:
    def __init__(self):
        logger.info("Initializing Finnhub Producer...")

        # Load config
        self.token = config.FINNHUB_API_TOKEN
        self.tickers = config.FINNHUB_TICKERS
        self.kafka_server = config.KAFKA_SERVER
        self.topic = config.KAFKA_TOPIC
        self.producer = None

        # --- VALIDATION API KEY ---
        # Kiểm tra xem Key có tồn tại và có hợp lệ không
        if not self.token or "YOUR_API_KEY" in self.token:
            logger.error("LỖI: Chưa cấu hình FINNHUB_API_TOKEN trong file .env")
            logger.error("Vui lòng tạo file .env và điền API Key vào.")
            sys.exit(1)  # Dừng chương trình ngay lập tức

        logger.info(f"Loaded API Key: {self.token[:5]}***")  # Chỉ in 5 ký tự đầu để debug
        logger.info(f"Tracking Tickers: {self.tickers}")

        self.ws_url = f"wss://ws.finnhub.io?token={self.token}"
        self._setup_kafka()

    def _setup_kafka(self):
        """Handles Kafka topic creation and producer connection."""
        # 1. Ensure Topic Exists
        try:
            admin = KafkaAdminClient(bootstrap_servers=self.kafka_server)
            if self.topic not in admin.list_topics():
                logger.info(f"Creating topic: {self.topic}")
                admin.create_topics([NewTopic(name=self.topic, num_partitions=1, replication_factor=1)])
            admin.close()
        except Exception as e:
            logger.warning(f"Kafka Admin check failed: {e}")

        # 2. Connect Producer
        while not self.producer:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.kafka_server,
                    value_serializer=lambda x: json.dumps(x).encode('utf-8')
                )
                logger.info("Kafka Producer connected.")
            except Exception as e:
                logger.warning(f"Waiting for Kafka... ({e})")
                time.sleep(5)

    def on_message(self, ws, message):
        """Handles incoming WebSocket messages."""
        try:
            raw_msg = json.loads(message)
            msg_type = raw_msg.get('type')

            if msg_type == 'trade':
                trades = raw_msg.get('data', [])
                for trade in trades:
                    record = map_finnhub_to_schema(trade)
                    if record:
                        self.producer.send(self.topic, value=record)
                        logger.info(f"{record['symbol']} | Price: {record['price']} | Vol: {record['volume']}")

            elif msg_type == 'ping':
                pass

        except Exception as e:
            logger.error(f"Message processing error: {e}")

    def on_error(self, ws, error):
        logger.error(f"WebSocket Error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        logger.warning("Finnhub connection closed.")

    def on_open(self, ws):
        logger.info("===> Connected to Finnhub WebSocket.")
        logger.info(f"Preparing to subscribe to {len(self.tickers)} symbols...")

        for i, ticker in enumerate(self.tickers):
            try:
                msg = {"type": "subscribe", "symbol": ticker}
                ws.send(json.dumps(msg))
                if i % 5 == 0: #Log 5 mã
                    logger.info(f"Subscribing batch {i}...")

                # Delay cực nhỏ để tránh bị Rate Limit của WebSocket
                time.sleep(0.1)
            except Exception as e:
                logger.error(f"Error subscribing {ticker}: {e}")

        logger.info(">>> SUBSCRIPTION COMPLETED! Streaming data now...")

    def start(self):
        # websocket.enableTrace(True) # Uncomment for debug
        ws = websocket.WebSocketApp(
            self.ws_url,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open
        )
        ws.run_forever()


if __name__ == "__main__":
    # Wait for Kafka services to fully start
    time.sleep(5)

    # Reconnection Loop
    while True:
        try:
            bot = FinnhubProducer()
            bot.start()
        except KeyboardInterrupt:
            logger.info("Stopping producer...")
            break
        except Exception as e:
            logger.error(f"Critical error: {e}. Retrying in 5s...")
            time.sleep(5)