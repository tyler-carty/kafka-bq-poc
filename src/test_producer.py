from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

def create_producer():
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

def generate_test_data():
    producer = create_producer()

    try:
        for i in range(100):
            test_data = {
                'timestamp': datetime.now().isoformat(),
                'user_id': random.randint(1, 1000),
                'action': random.choice(['click', 'view', 'purchase']),
                'value': random.randint(1, 100)
            }

            producer.send(KAFKA_TOPIC, value=test_data)

            if i % 10 == 0:
                print(f"Sent {i} messages")
            time.sleep(0.1)  # Small delay between messages

    except Exception as e:
        print(f"Error producing messages: {str(e)}")
    finally:
        producer.close()

if __name__ == "__main__":
    print(f"Starting producer... sending to topic: {KAFKA_TOPIC}")
    generate_test_data()
    print("Done!")