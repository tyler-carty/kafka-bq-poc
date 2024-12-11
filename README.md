# Kafka to BigQuery Data Pipeline with Spark Streaming

## Purpose
This project implements a real-time streaming pipeline that continuously reads data from a Kafka topic and streams it into BigQuery using Apache Spark Structured Streaming. It demonstrates:
- Real-time data streaming from Kafka
- Micro-batch processing using Spark Structured Streaming
- Direct streaming writes to BigQuery
- Containerized deployment using Docker

## Architecture
```
Test Producer → Kafka Topic → Spark Streaming → BigQuery
```

### Components
- **Test Producer**: Python script generating sample event data
- **Kafka**: Message broker for data streaming
- **Spark Streaming Consumer**: PySpark application for continuous data processing
- **BigQuery**: Destination for streamed data

## Prerequisites
- Docker and Docker Compose installed
- Google Cloud Platform account with:
    - BigQuery dataset created
    - Service account with permissions:
        - BigQuery Data Editor
        - BigQuery Job User

## Project Structure
```
kafka-bq-poc/
├── docker-compose.yml
├── requirements.producer.txt
├── requirements.spark.txt
├── producer.Dockerfile
├── spark.Dockerfile
├── src/
│   ├── config.py
│   ├── test_producer.py
│   └── spark_consumer.py
└── .env
```

## Setup Instructions

### 1. Initial Setup
```bash
# Clone the repository
git clone https://github.com/tyler-carty/kafka-bq-poc
cd kafka-bq-poc

# Create Python virtual environment (optional, for local development)
python -m venv .venv
source .venv/bin/activate  # or `.venv\Scripts\activate` on Windows
```

### 2. Configure Environment
1. Create a `.env` file:
```
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC=test-events
GOOGLE_CLOUD_PROJECT=your-project-id
BIGQUERY_DATASET=your_dataset
BIGQUERY_TABLE=test_events
```

2. Set up Google Cloud credentials:
- Create a service account with BigQuery permissions
- Download the JSON key
- Save it as `service-account.json` in project root

3. Create BigQuery table:
```sql
CREATE TABLE IF NOT EXISTS `your-project.dataset.test_events`
(
    timestamp TIMESTAMP,
    user_id INT64,
    action STRING,
    value INT64
)
PARTITION BY DATE(timestamp)
```

## Running the Pipeline

### 1. Start the Infrastructure
```bash
# Start all services
docker-compose up -d

# Check services are running
docker-compose ps
```

### 2. Watch the Consumer Logs
```bash
# In one terminal, watch the streaming consumer
docker-compose logs -f spark
```

### 3. Generate Test Data
```bash
# In another terminal, run the test producer
docker-compose run producer
```

### 4. Verify Data Flow
```sql
-- Check BigQuery for new records
SELECT COUNT(*) 
FROM `your-project.dataset.test_events`
WHERE DATE(timestamp) = CURRENT_DATE()
```

## How It Works

### Data Flow
1. Test producer generates events with:
    - timestamp
    - user_id (random 1-1000)
    - action (click/view/purchase)
    - value (random 1-100)

2. Spark Streaming consumer:
    - Continuously reads from Kafka topic
    - Processes data in micro-batches every 10 seconds
    - Writes directly to BigQuery using streaming inserts

### Monitoring
- Watch Spark streaming progress:
```bash
docker-compose logs -f spark
```
- Monitor Kafka topics:
```bash
docker-compose exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092
```

## Development

### Local Development
1. Install dependencies in your virtual environment:
```bash
pip install -r requirements.producer.txt  # For producer development
pip install -r requirements.spark.txt     # For consumer development
```

2. Make changes to code in `src/`

3. Rebuild and restart services:
```bash
docker-compose down
docker-compose build
docker-compose up -d
```

### Cleaning Up
```bash
# Stop all services and remove containers
docker-compose down

# Include --remove-orphans if you see orphaned container warnings
docker-compose down --remove-orphans
```

## Troubleshooting

### Common Issues
1. "Connection refused" to Kafka:
    - Ensure Kafka container is running and healthy
    - Check `KAFKA_BOOTSTRAP_SERVERS` in .env

2. BigQuery permissions:
    - Verify service account has BigQuery Data Editor role
    - Check `service-account.json` is present and valid

3. No data flowing:
    - Check Spark consumer logs for errors
    - Verify Kafka topic exists and contains messages
    - Ensure BigQuery table schema matches expected format