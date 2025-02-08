from confluent_kafka import Consumer, KafkaError, Producer
import json
import threading

class EmojiSubscriber:
    def __init__(self, broker='localhost:9092', cluster_topic='cluster1', stream_topic='emoji_stream_topic'):
        self.broker = broker
        self.cluster_topic = cluster_topic
        self.stream_topic = stream_topic  # Topic to forward data in the expected format
        self.registered_clients = set()
        self.running = True
        
        # Initialize producer for forwarding the data
        self.producer = Producer({'bootstrap.servers': self.broker})
        
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
        """Consume cluster data and forward it to the emoji stream topic."""
        consumer = Consumer({
            'bootstrap.servers': self.broker,
            'group.id': f'{self.cluster_topic}-group',  # Unique group per cluster
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
                    # Forward data to the emoji stream topic
                    emoji_data = self.format_emoji_data(cluster_data)

                    # Check if this message has already been processed before forwarding
                    # Implementing offset tracking here
                    if self.has_message_been_processed(msg.offset()):
                        print(f"Skipping already processed message with offset: {msg.offset()}")
                    else:
                        self.producer.produce(self.stream_topic, json.dumps(emoji_data).encode('utf-8'))
                        print(f"Forwarding data to {self.stream_topic} topic: {emoji_data}")
                        self.mark_message_as_processed(msg.offset())

            except Exception as e:
                print(f"Error processing cluster data: {e}")

        consumer.close()

    def format_emoji_data(self, cluster_data):
        """Format the cluster data into the emoji stream format."""
        emoji_type = cluster_data.get("emoji_type")
        scaled_count = int(cluster_data.get("scaled_count", 0))
        if emoji_type and scaled_count > 0:
            return {
                "emoji_type": emoji_type,
                "scaled_count": scaled_count
            }
        return {}

    def has_message_been_processed(self, offset):
        """Check if the message has already been processed based on offset."""
        # For simplicity, we'll store offsets in memory (use a persistent store in real applications)
        if not hasattr(self, 'processed_offsets'):
            self.processed_offsets = set()
        return offset in self.processed_offsets

    def mark_message_as_processed(self, offset):
        """Mark a message as processed by storing its offset."""
        if not hasattr(self, 'processed_offsets'):
            self.processed_offsets = set()
        self.processed_offsets.add(offset)

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
