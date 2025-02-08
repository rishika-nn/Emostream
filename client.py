from kafka import KafkaProducer
import json
import random
import multiprocessing
import time
import uuid

class EmojiClient:
    def __init__(self, broker='localhost:9092'):
        self.client_id = str(uuid.uuid4())  # Generate unique client ID
        self.broker = broker
        
        # Initialize Kafka producer for sending emojis
        self.producer = KafkaProducer(
            bootstrap_servers=self.broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            linger_ms=5,  # Delay to allow batching (5 ms)
            batch_size=16384  # Increase batch size (16 KB)
        )
        
        # Register the client
        self.register_client()

    def register_client(self):
        """Register this client with the system."""
        registration_data = {
            "client_id": self.client_id,
            "timestamp": int(time.time() * 1000)
        }
        try:
            self.producer.send('registration_topic', registration_data)
            self.producer.flush()
            print(f"Successfully registered client with ID: {self.client_id}")
        except Exception as e:
            print(f"Failed to register client: {e}")
            exit(1)

    def send_emojis(self, emojis, count_per_interval=100, interval_seconds=5):
        """Send emojis in bulk at high speed."""
        print(f"Client {self.client_id} started sending emojis...")

        while True:
            messages = []
            for _ in range(count_per_interval):
                emoji = random.choice(emojis)
                emoji_data = {
                    "user_id": self.client_id,
                    "emoji_type": emoji,
                    "timestamp": int(time.time() * 1000)
                }
                messages.append(emoji_data)

            try:
                # Send messages in bulk
                for message in messages:
                    self.producer.send('emoji_topic', message)

                self.producer.flush()
                print(f"Client {self.client_id} sent {len(messages)} emojis.")
            except Exception as e:
                print(f"Error sending emojis: {e}")
                break

            # Wait for the next interval
            time.sleep(interval_seconds)

    def stop(self):
        """Stop the client."""
        if hasattr(self, 'producer'):
            self.producer.close()
        print(f"Client {self.client_id} stopped.")

def run_client(emojis, count_per_interval=100):
    """Function to run an EmojiClient instance and send emojis."""
    client = EmojiClient()  # Initialize the client
    try:
        client.send_emojis(emojis, count_per_interval)
    finally:
        client.stop()

def main():
    num_clients = 10  # Reduce the number of clients (from 50 to 10)
    count_per_interval =100 # Reduce the number of emojis sent per client per interval (from 1000 to 50)
    interval_seconds = 5  # Increase the interval time (to 5 seconds)
    emojis = ["😊", "❤️", "👍", "🔥", "😂"]  # Sample emojis

    processes = []

    # Start multiple clients using multiprocessing
    for i in range(num_clients):
        process = multiprocessing.Process(
            target=run_client,
            args=(emojis, count_per_interval)
        )
        process.start()
        processes.append(process)
    
    # Wait for all clients to finish
    for process in processes:
        process.join()

if __name__ == "__main__":
    main()
