# subscriber.py
from confluent_kafka import Consumer, KafkaError
import json
import threading

class EmojiSubscriber:
    def __init__(self, broker='localhost:9092', cluster_topic='cluster1'):
        self.broker = broker
        self.cluster_topic = cluster_topic
        self.registered_clients = set()
        self.running = True
        
        # Start registration consumer in a separate thread
        self.registration_thread = threading.Thread(target=self.consume_registrations)
        self.registration_thread.daemon = True
        self.registration_thread.start()
        
        # Start data consumer in the main thread
        self.consume_cluster_data()

    def consume_registrations(self):
        """Consume client registrations from the Kafka topic."""
        consumer = Consumer({
            'bootstrap.servers': self.broker,
            'group.id': 'registration-consumer-group',
            'auto.offset.reset': 'earliest'
        })
        consumer.subscribe(['registration_topic'])

        while self.running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Consumer error: {msg.error()}")
                    continue

            try:
                registration_data = json.loads(msg.value().decode('utf-8'))
                client_id = registration_data.get("client_id")
                if client_id:
                    self.registered_clients.add(client_id)
                    print(f"Registered new client: {client_id}")
            except Exception as e:
                print(f"Error processing registration: {e}")

        consumer.close()

    def consume_cluster_data(self):
        """Consume cluster data and log it for registered clients."""
        consumer = Consumer({
            'bootstrap.servers': self.broker,
            'group.id': f'{self.cluster_topic}-group',
            'auto.offset.reset': 'latest'
        })
        consumer.subscribe([self.cluster_topic])

        while self.running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Consumer error: {msg.error()}")
                    continue

            try:
                cluster_data = json.loads(msg.value().decode('utf-8'))
                print(f"\nReceived from {self.cluster_topic}: {cluster_data}")

                if not self.registered_clients:
                    print("No clients registered.")
                else:
                    print(f"Active clients: {len(self.registered_clients)}")
                    for client in self.registered_clients:
                        print(f"Forwarding data to client {client}")
            except Exception as e:
                print(f"Error processing cluster data: {e}")

        consumer.close()

    def stop(self):
        """Stop the subscriber."""
        self.running = False

if __name__ == '__main__':
    import sys
    cluster_topic = sys.argv[1] if len(sys.argv) > 1 else "cluster1"
    subscriber = EmojiSubscriber(cluster_topic=cluster_topic)
    
    try:
        while True:
            pass
    except KeyboardInterrupt:
        print("\nShutting down subscriber...")
        subscriber.stop()
