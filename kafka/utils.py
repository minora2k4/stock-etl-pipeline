import logging
import sys
import datetime
import random


def setup_logger(name="FinnhubProducer"):
    """Configures console logging."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    return logging.getLogger(name)


logger = setup_logger()


def map_finnhub_to_schema(data):
    """
    Maps raw Finnhub trade data to internal JSON schema.
    Input: {'p': price, 's': symbol, 'v': volume, 't': timestamp...}
    """
    try:
        timestamp_ms = data.get('t')
        if not timestamp_ms:
            return None

        # Convert timestamp to ISO String
        event_time = datetime.datetime.fromtimestamp(timestamp_ms / 1000.0).isoformat()

        return {
            "event_time": event_time,
            "symbol": data.get('s'),
            "price": float(data.get('p', 0)),
            "volume": int(data.get('v', 0)),
            # Simulate side (Buy/Sell) as Finnhub free tier lacks this
            "side": random.choice(['B', 'S']),
            "source": "FINNHUB_WS_REAL"
        }
    except Exception as e:
        logger.error(f"Mapping error: {e}")
        return None