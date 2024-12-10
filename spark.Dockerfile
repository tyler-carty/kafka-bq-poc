FROM bitnami/spark:3.2.0

USER root

WORKDIR /app

# Copy requirements first
COPY requirements.spark.txt requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Download GCS connector
RUN curl -O https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop2-latest.jar && \
    mv gcs-connector-hadoop2-latest.jar /opt/bitnami/spark/jars/

# Copy application code
COPY src/ /app/src/
COPY .env /app/.env

CMD ["python", "src/spark_consumer.py"]