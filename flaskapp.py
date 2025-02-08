from flask import Flask, request, jsonify
from confluent_kafka import Producer
import json
import time
import asyncio

app = Flask(__name__)

# Kafka configuration
producer_config = {
    'bootstrap.servers': 'localhost:9092',
    'queue.buffering.max.ms': 500
}
producer = Producer(producer_config)

async def send_to_kafka(topic, data):
    """Asynchronous function to send data to Kafka."""
    producer.produce(topic, json.dumps(data))
    producer.poll(0)
    await asyncio.sleep(0)

@app.route('/register', methods=['POST'])
def register_client():
    """API endpoint to register clients."""
    try:
        client_id = request.json.get("client_id")
        if not client_id:
            return jsonify({"error": "Client ID is required"}), 400
        
        # Publish registration to Kafka
        data = {"client_id": client_id}
        producer.produce("registration_topic", json.dumps(data))
        producer.poll(0)
        
        return jsonify({"message": f"Client {client_id} registered successfully"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/send_emoji', methods=['POST'])
async def handle_emoji():
    """API endpoint to handle emoji POST requests."""
    try:
        user_id = request.json.get("user_id")
        emoji_type = request.json.get("emoji_type")
        timestamp = request.json.get("timestamp", int(time.time()))

        if not user_id or not emoji_type:
            return jsonify({"error": "Invalid data"}), 400

        data = {
            "user_id": user_id,
            "emoji_type": emoji_type,
            "timestamp": timestamp
        }
        await send_to_kafka("emoji_topic", data)
        return jsonify({"status": "Message queued"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
