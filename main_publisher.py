# main_publisher.py
from kafka import KafkaProducer
import json

# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'
OUTPUT_TOPIC = 'final_output_topic'

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def publish_data_to_kafka(data):
    """Publish processed emoji data to a specified Kafka topic."""
    try:
        producer.send(OUTPUT_TOPIC, value=data)
        producer.flush()
        print(f"Published data to Kafka: {data}")
    except Exception as e:
        print(f"Failed to publish data: {e}")
