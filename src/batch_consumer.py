from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    FULL_TABLE_ID
)
from kafka import KafkaConsumer, TopicPartition
from google.cloud import bigquery
import json
import logging
from datetime import datetime
import sqlite3  # For local offset storage
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BatchKafkaToBigQuery:
    def __init__(self,
                 topic: str,
                 bootstrap_servers: list,
                 table_id: str,
                 batch_size: int = 1000):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.table_id = table_id
        self.batch_size = batch_size
        self.buffer = []

        # Initialize SQLite for offset tracking
        self.init_offset_storage()

        # Initialize Kafka consumer
        self.consumer = KafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset='earliest',  # Explicitly start from earliest
            enable_auto_commit=False,
            group_id='bq-batch-ingestion-group-' + datetime.now().strftime('%Y%m%d%H%M%S'),  # Unique group
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        self.bq_client = bigquery.Client()

    def init_offset_storage(self):
        """Initialize SQLite database for offset tracking"""
        db_path = 'offsets.db'
        self.conn = sqlite3.connect(db_path)
        cursor = self.conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS offsets
            (topic TEXT, partition INTEGER, offset INTEGER, 
             timestamp TEXT, PRIMARY KEY (topic, partition))
        ''')
        self.conn.commit()

    def get_last_processed_offset(self, partition):
        """Get last processed offset for a partition"""
        cursor = self.conn.cursor()
        cursor.execute(
            'SELECT offset FROM offsets WHERE topic = ? AND partition = ?',
            (self.topic, partition)
        )
        result = cursor.fetchone()
        return result[0] if result else None

    def save_processed_offset(self, partition, offset):
        """Save the last successfully processed offset"""
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO offsets (topic, partition, offset, timestamp)
            VALUES (?, ?, ?, ?)
        ''', (self.topic, partition, offset, datetime.now().isoformat()))
        self.conn.commit()
        logger.info(f"Saved offset {offset} for partition {partition}")

    def flush_buffer(self):
        """Flush the current buffer to BigQuery"""
        if not self.buffer:
            return

        try:
            errors = self.bq_client.insert_rows_json(self.table_id, self.buffer)
            if errors:
                logger.error(f"Errors inserting rows: {errors}")
                raise Exception("Failed to insert rows into BigQuery")
            logger.info(f"Successfully inserted {len(self.buffer)} rows")
        finally:
            self.buffer = []

    def run_batch(self):
        """Run one complete batch ingestion"""
        try:
            # Subscribe to topic
            self.consumer.subscribe([self.topic])

            # Wait for partition assignment
            while not self.consumer.assignment():
                self.consumer.poll(timeout_ms=1000)

            # Get assigned partitions
            partitions = self.consumer.assignment()

            # Debug: Check beginning and end offsets
            for partition in partitions:
                beginning_offsets = self.consumer.beginning_offsets([partition])
                end_offsets = self.consumer.end_offsets([partition])
                logger.info(f"Partition {partition.partition}:")
                logger.info(f"  Beginning offset: {beginning_offsets[partition]}")
                logger.info(f"  End offset: {end_offsets[partition]}")

            # Seek to last processed offset for each partition
            for partition in partitions:
                last_offset = self.get_last_processed_offset(partition.partition)
                if last_offset is not None:
                    self.consumer.seek(partition, last_offset + 1)
                    logger.info(f"Resuming from offset {last_offset + 1} for partition {partition.partition}")
                else:
                    logger.info(f"Starting from beginning for partition {partition.partition}")

            message_count = 0
            start_time = datetime.now()

            while True:
                messages = self.consumer.poll(timeout_ms=10000)  # 10 second timeout

                if not messages:
                    logger.info("No more messages available")
                    break

                for tp, batch in messages.items():
                    for message in batch:
                        self.buffer.append(message.value)
                        message_count += 1

                        if len(self.buffer) >= self.batch_size:
                            self.flush_buffer()
                            self.save_processed_offset(tp.partition, message.offset)

                        if message_count % 1000 == 0:
                            logger.info(f"Processed {message_count} messages")

            # Final flush
            if self.buffer:
                self.flush_buffer()
                # Save final offsets
                for tp in self.consumer.assignment():
                    position = self.consumer.position(tp)
                    self.save_processed_offset(tp.partition, position - 1)

            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"Batch complete. Processed {message_count} messages in {duration:.2f} seconds")

        except Exception as e:
            logger.error(f"Error in batch processing: {str(e)}")
            raise
        finally:
            self.consumer.close()
            self.conn.close()

if __name__ == "__main__":
    processor = BatchKafkaToBigQuery(
        topic=KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        table_id=FULL_TABLE_ID,
        batch_size=1000
    )
    processor.run_batch()