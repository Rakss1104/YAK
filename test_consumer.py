import requests
import time

BROKER_URL = "http://localhost:5001"
CONSUMER_ID = "test-consumer-1"

def get_leader():
    """Get the current leader broker."""
    try:
        response = requests.get(f"{BROKER_URL}/metadata/leader", timeout=5)
        if response.status_code == 200:
            return response.json()
        return None
    except Exception as e:
        print(f"❌ Error getting leader: {e}")
        return None

def consume_messages(offset):
    """Consume messages from the broker."""
    try:
        response = requests.get(f"{BROKER_URL}/consume?offset={offset}", timeout=5)
        if response.status_code == 200:
            data = response.json()
            return data.get("messages", [])
        else:
            print(f"❌ Failed to consume: {response.json()}")
            return []
    except Exception as e:
        print(f"❌ Error consuming messages: {e}")
        return []

def main():
    print(f"🚀 Starting test consumer: {CONSUMER_ID}")
    print(f"📡 Broker URL: {BROKER_URL}")
    print("=" * 50)
    
    # Get leader info
    leader_info = get_leader()
    if leader_info:
        print(f"👑 Current leader: {leader_info.get('leader_id')}")
    else:
        print("⚠️  No leader found, waiting...")
    
    print("\n📥 Starting to consume messages...")
    print("=" * 50)
    
    current_offset = 0
    message_count = 0
    
    try:
        while True:
            messages = consume_messages(current_offset)
            
            if messages:
                for msg in messages:
                    message_count += 1
                    offset = msg.get("offset")
                    data = msg.get("data")
                    
                    print(f"\n✅ Message #{message_count}")
                    print(f"   Offset: {offset}")
                    print(f"   Producer: {data.get('producer_id', 'unknown')}")
                    print(f"   Content: {data.get('message', 'N/A')}")
                    
                    current_offset = offset
            
            # Poll every 3 seconds
            time.sleep(3)
            
    except KeyboardInterrupt:
        print(f"\n\n🛑 Consumer stopped. Total messages consumed: {message_count}")

if __name__ == "__main__":
    main()
