# cluster_publisher.py
from kafka import KafkaProducer, KafkaConsumer
import json
import sys
import threading

class ClusterPublisher:
    def __init__(self, broker='localhost:9092', cluster_topic='cluster1'):
        self.broker = broker
        self.main_topic = 'final_output_topic'
        self.cluster_topic = cluster_topic
        self.running = True
        
        # Initialize Kafka Producer for cluster topic
        self.producer = KafkaProducer(
            bootstrap_servers=broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Initialize Kafka Consumer for main topic
        self.consumer = KafkaConsumer(
            self.main_topic,
            bootstrap_servers=broker,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        print(f"Cluster Publisher started. Listening to {self.main_topic} and publishing to {self.cluster_topic}")

    def start(self):
        """Start consuming from main topic and publishing to cluster topic"""
        try:
            for message in self.consumer:
                if not self.running:
                    break
                    
                data = message.value
                print(f"Received from main topic: {data}")
                
                # Forward data to cluster topic
                self.producer.send(self.cluster_topic, value=data)
                self.producer.flush()
                print(f"Published to cluster topic {self.cluster_topic}: {data}")
        except Exception as e:
            print(f"Error in cluster publisher: {e}")
        finally:
            self.stop()

    def stop(self):
        """Stop the cluster publisher"""
        self.running = False
        self.producer.close()
        self.consumer.close()
        print("Cluster Publisher stopped.")

if __name__ == "__main__":
    cluster_topic = sys.argv[1] if len(sys.argv) > 1 else 'cluster1'
    publisher = ClusterPublisher(cluster_topic=cluster_topic)
    
    try:
        publisher.start()
    except KeyboardInterrupt:
        print("\nShutting down...")
        publisher.stop()
