from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
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

class SparkKafkaToBigQuery:
    def __init__(self):
        self.spark = self._create_spark_session()
        self.schema = self._create_schema()
        self.table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    def _create_spark_session(self):
        """Initialize Spark session with required configurations"""
        return (SparkSession.builder
                .appName("KafkaToBigQuery")
                .config("spark.jars.packages",
                        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,"
                        "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.27.1")
                .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
                .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                        "${GOOGLE_APPLICATION_CREDENTIALS}")
                .getOrCreate())

    def _create_schema(self):
        """Define schema matching our Kafka message structure"""
        return StructType([
            StructField("timestamp", TimestampType()),
            StructField("user_id", IntegerType()),
            StructField("action", StringType()),
            StructField("value", IntegerType())
        ])

    def read_from_kafka(self):
        """Read data from Kafka topic"""
        logger.info(f"Starting to read from Kafka topic: {KAFKA_TOPIC}")

        return (self.spark.read
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                .option("subscribe", KAFKA_TOPIC)
                .option("startingOffsets", "earliest")
                .option("endingOffsets", "latest")
                .load())

    def process_batch(self):
        """Process one batch of data"""
        try:
            start_time = datetime.now()
            logger.info("Starting batch processing")

            # Read from Kafka
            df = self.read_from_kafka()

            # Parse JSON data
            parsed_df = df.select(
                from_json(col("value").cast("string"), self.schema).alias("data")
            ).select("data.*")

            # Count records
            record_count = parsed_df.count()
            logger.info(f"Processing {record_count} records")

            if record_count > 0:
                # Write to BigQuery
                (parsed_df.write
                 .format("bigquery")
                 .option("table", self.table_id)
                 .option("temporaryGcsBucket", f"{PROJECT_ID}-temp")  # Need to create this
                 .mode("append")
                 .save())

                duration = (datetime.now() - start_time).total_seconds()
                logger.info(f"Batch complete. Processed {record_count} records in {duration:.2f} seconds")
            else:
                logger.info("No new records to process")

        except Exception as e:
            logger.error(f"Error processing batch: {str(e)}")
            raise
        finally:
            logger.info("Cleaning up Spark session")
            self.spark.stop()

if __name__ == "__main__":
    processor = SparkKafkaToBigQuery()
    processor.process_batch()