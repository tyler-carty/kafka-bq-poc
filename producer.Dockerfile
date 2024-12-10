FROM python:3.9-slim

WORKDIR /app

# Install only the required dependencies for the producer
COPY requirements.producer.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ /app/src/
COPY .env /app/.env

CMD ["python", "src/test_producer.py"]