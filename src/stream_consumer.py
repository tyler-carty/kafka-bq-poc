from kafka import KafkaConsumer
from google.cloud import bigquery
import json
import logging
from datetime import datetime
from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    FULL_TABLE_ID
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaToBigQuery:
    def __init__(self, batch_size: int = 100, flush_interval: int = 30):
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.buffer = []
        self.last_flush = datetime.now()

        # Initialize clients
        self.consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='bq-streaming-poc-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        self.bq_client = bigquery.Client()

    def flush_buffer(self):
        if not self.buffer:
            return

        try:
            errors = self.bq_client.insert_rows_json(FULL_TABLE_ID, self.buffer)
            if errors:
                logger.error(f"Errors inserting rows: {errors}")
            else:
                logger.info(f"Successfully inserted {len(self.buffer)} rows")
        except Exception as e:
            logger.error(f"Error flushing to BigQuery: {str(e)}")
        finally:
            self.buffer = []
            self.last_flush = datetime.now()

    def should_flush(self) -> bool:
        if len(self.buffer) >= self.batch_size:
            return True

        seconds_since_flush = (datetime.now() - self.last_flush).seconds
        return seconds_since_flush >= self.flush_interval

    def run(self):
        try:
            logger.info(f"Starting consumer for topic: {KAFKA_TOPIC}")
            message_count = 0

            for message in self.consumer:
                self.buffer.append(message.value)
                message_count += 1

                if message_count % 100 == 0:
                    logger.info(f"Processed {message_count} messages")

                if self.should_flush():
                    self.flush_buffer()

        except KeyboardInterrupt:
            logger.info("Shutdown signal received...")
        except Exception as e:
            logger.error(f"Fatal error: {str(e)}")
        finally:
            if self.buffer:
                self.flush_buffer()
            self.consumer.close()
            logger.info(f"Shutdown complete. Processed {message_count} total messages")

if __name__ == "__main__":
    processor = KafkaToBigQuery()
    processor.run()