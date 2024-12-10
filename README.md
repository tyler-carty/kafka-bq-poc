# Kafka to BigQuery Data Pipeline with Spark

## Purpose
This project demonstrates a batch ingestion pipeline that reads data from a Kafka topic and loads it into BigQuery using Apache Spark. It's designed as a proof of concept for processing large volumes of data with the following features:
- Batch processing of Kafka messages
- Parallel processing using Spark
- Efficient loading into BigQuery
- Containerized deployment using Docker

## Architecture
```
Kafka Topic → Apache Spark → Google Cloud Storage (temp) → BigQuery
```

### Components
- **Kafka Consumer**: Reads messages in batches from specified Kafka topic
- **Spark Processing**: Handles data transformation and batch processing
- **BigQuery Loader**: Loads processed data into BigQuery tables
- **Docker Containers**: Manages dependencies and runtime environment

## Prerequisites
- Docker and Docker Compose installed
- Google Cloud Platform account with:
    - BigQuery dataset created
    - Storage bucket for temporary data
    - Service account with appropriate permissions:
        - BigQuery Job User
        - Storage Object Admin
        - Storage Admin

## Project Structure
```
kafka-bq-poc/
├── docker-compose.yml
├── requirements.txt
├── spark.Dockerfile
├── producer.Dockerfile
├── src/
│   ├── config.py
│   ├── producer.py
│   └── spark_consumer.py
└── .env
```

## Configuration
Create a `.env` file with:
```
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC=test-events
GOOGLE_CLOUD_PROJECT=your-project-id
BIGQUERY_DATASET=your_dataset
BIGQUERY_TABLE=test_events
```

## Setup Instructions

1. Clone the repository:
```bash
git clone https://github.com/tyler-carty/kafka-bq-poc
cd kafka-bq-poc
```

2. Set up Google Cloud credentials:
- Create a service account and download JSON key
- Place the key file in project root
- Update environment variables in .env

3. Start the services:
```bash
docker-compose up -d
```

4. Generate test data:
```bash
docker-compose run producer
```

5. Run the Spark consumer:
```bash
docker-compose run spark python src/spark_consumer.py
```

## Usage

### Running a Batch Job
The pipeline is designed to run as a batch job. To process data:

1. Ensure Kafka and other services are running:
```bash
docker-compose ps
```

2. Run the Spark consumer:
```bash
docker-compose run spark python src/spark_consumer.py
```

3. Verify data in BigQuery:
```sql
SELECT COUNT(*) 
FROM `your-project.dataset.test_events`
```

This will create sample messages in the Kafka topic.

## Maintenance

### Cleaning Up
```bash
# Stop all services
docker-compose down

# Remove temporary data
docker-compose down -v
```

### Updating
1. Pull latest code changes
2. Rebuild containers:
```bash
docker-compose build --no-cache
```

## Development

### Adding New Features
1. Modify src/spark_consumer.py for new processing logic
2. Update Dockerfile if new dependencies are required
3. Rebuild and test changes:
```bash
docker-compose build spark
docker-compose run spark python src/spark_consumer.py
```