import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'test-events')

# BigQuery Configuration
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT')
DATASET_ID = os.getenv('BIGQUERY_DATASET')
TABLE_ID = os.getenv('BIGQUERY_TABLE')
FULL_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"