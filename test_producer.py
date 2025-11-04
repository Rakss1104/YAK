import requests
import time
import random

BROKER_URL = "http://localhost:5001"
PRODUCER_ID = "test-producer-1"

def produce_message(message_data):
    """Send a message to the broker."""
    data = {
        "producer_id": PRODUCER_ID,
        "message": message_data,
        "timestamp": time.time()
    }
    
    try:
        response = requests.post(f"{BROKER_URL}/produce", json=data, timeout=5)
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Message sent successfully! Offset: {result.get('offset')}")
            return True
        else:
            print(f"❌ Failed to send message: {response.json()}")
            return False
    except Exception as e:
        print(f"❌ Error sending message: {e}")
        return False

def main():
    print(f"🚀 Starting test producer: {PRODUCER_ID}")
    print(f"📡 Broker URL: {BROKER_URL}")
    print("=" * 50)
    
    # Sample messages
    messages = [
        "Hello from YAK broker!",
        "Testing message replication",
        "Real-time dashboard update",
        "Leader election in action",
        "High availability messaging"
    ]
    
    message_count = 0
    
    try:
        while True:
            # Pick a random message
            message = random.choice(messages)
            message_with_count = f"{message} (#{message_count + 1})"
            
            print(f"\n📤 Sending: {message_with_count}")
            if produce_message(message_with_count):
                message_count += 1
            
            # Wait before sending next message
            wait_time = random.randint(2, 5)
            print(f"⏳ Waiting {wait_time} seconds...")
            time.sleep(wait_time)
            
    except KeyboardInterrupt:
        print(f"\n\n🛑 Producer stopped. Total messages sent: {message_count}")

if __name__ == "__main__":
    main()
