import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import logging
from datetime import datetime
from src.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    PROJECT_ID,
    DATASET_ID,
    TABLE_ID
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SparkKafkaStreaming:
    def __init__(self):
        self.spark = self._create_spark_session()
        self.schema = self._create_schema()
        self.table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    def _create_spark_session(self):
        return (SparkSession.builder
                .appName("KafkaStreamingToBigQuery")
                .config("spark.jars.packages",
                        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,"
                        "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.27.1")
                .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
                .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                        os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
                .config("spark.streaming.stopGracefullyOnShutdown", "true")
                .getOrCreate())

    def _create_schema(self):
        return StructType([
            StructField("timestamp", TimestampType()),
            StructField("user_id", IntegerType()),
            StructField("action", StringType()),
            StructField("value", IntegerType())
        ])

    def process_stream(self):
        try:
            logger.info(f"Starting streaming from topic {KAFKA_TOPIC}")

            # Read streaming data from Kafka
            stream_df = (self.spark
                         .readStream
                         .format("kafka")
                         .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                         .option("subscribe", KAFKA_TOPIC)
                         .option("startingOffsets", "latest")
                         .load())

            # Parse the value column which contains our JSON data
            parsed_df = stream_df.select(
                from_json(col("value").cast("string"), self.schema).alias("data")
            ).select("data.*")

            def write_to_bigquery(df, epoch_id):
                # Use DataFrame's native BigQuery write method
                df.write \
                    .format("bigquery") \
                    .option("table", self.table_id) \
                    .option("writeMethod", "direct") \
                    .mode("append") \
                    .save()

                logger.info(f"Batch {epoch_id} written to BigQuery")

            # Stream the data using foreachBatch
            query = (parsed_df.writeStream
                     .foreachBatch(write_to_bigquery)
                     .trigger(processingTime="10 seconds")
                     .start())

            logger.info("Streaming query started")
            query.awaitTermination()

        except Exception as e:
            logger.error(f"Error in streaming process: {str(e)}")
            raise
        finally:
            self.spark.stop()

if __name__ == "__main__":
    processor = SparkKafkaStreaming()
    processor.process_stream()