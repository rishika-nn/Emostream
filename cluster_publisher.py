from kafka import KafkaProducer, KafkaConsumer
import json
import sys

class ClusterPublisher:
    def __init__(self, broker='localhost:9092', cluster_topic='cluster1'):
        self.broker = broker
        self.main_topic = 'final_output_topic'
        self.cluster_topic = cluster_topic
        
        # Kafka Producer for cluster topic
        self.producer = KafkaProducer(
            bootstrap_servers=broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Kafka Consumer for main topic
        self.consumer = KafkaConsumer(
            self.main_topic,
            bootstrap_servers=broker,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )

    def start(self):
        print(f"ClusterPublisher {self.cluster_topic} started.")
        try:
            for message in self.consumer:
                data = message.value
                print(f"[{self.main_topic}] -> [{self.cluster_topic}] {data}")
                self.producer.send(self.cluster_topic, value=data)
                self.producer.flush()
        except Exception as e:
            print(f"Error in ClusterPublisher: {e}")
        finally:
            self.producer.close()

if __name__ == "__main__":
    cluster_topic = sys.argv[1] if len(sys.argv) > 1 else 'cluster1'
    publisher = ClusterPublisher(cluster_topic=cluster_topic)
    publisher.start()
