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

class SparkKafkaToBigQuery:
    def __init__(self):
        self.spark = self._create_spark_session()
        self.schema = self._create_schema()
        self.table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    def _create_spark_session(self):
        return (SparkSession.builder
                .appName("KafkaToBigQuery")
                .config("spark.jars.packages",
                        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,"
                        "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.27.1")
                .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
                .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                        os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
                .config("spark.hadoop.fs.gs.impl",
                        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
                .config("spark.hadoop.fs.gs.auth.service.account.enable", "true")
                .getOrCreate())

    def _create_schema(self):
        # Match the schema from our test producer
        return StructType([
            StructField("timestamp", TimestampType()),
            StructField("user_id", IntegerType()),
            StructField("action", StringType()),
            StructField("value", IntegerType())
        ])

    def process_batch(self):
        try:
            logger.info(f"Starting batch processing from topic {KAFKA_TOPIC}")

            # Read from Kafka
            df = (self.spark.read
                  .format("kafka")
                  .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                  .option("subscribe", KAFKA_TOPIC)
                  .option("startingOffsets", "earliest")
                  .option("endingOffsets", "latest")
                  .load())

            # Parse the value column which contains our JSON data
            parsed_df = df.select(
                from_json(col("value").cast("string"), self.schema).alias("data")
            ).select("data.*")

            # Count records
            record_count = parsed_df.count()
            logger.info(f"Found {record_count} records to process")

            if record_count > 0:
                # Write to BigQuery
                (parsed_df.write
                 .format("bigquery")
                 .option("table", self.table_id)
                 .option("temporaryGcsBucket", f"{PROJECT_ID}-temp")  # Make sure this bucket exists
                 .option("parentProject", PROJECT_ID)
                 .mode("append")
                 .save())

                logger.info(f"Successfully wrote {record_count} records to BigQuery")
            else:
                logger.info("No records to process")

        except Exception as e:
            logger.error(f"Error processing batch: {str(e)}")
            raise
        finally:
            self.spark.stop()

if __name__ == "__main__":
    processor = SparkKafkaToBigQuery()
    processor.process_batch()