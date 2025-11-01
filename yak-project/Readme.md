# Kafka-like Distributed Message Broker

A lightweight, distributed message broker implementation inspired by Apache Kafka, built with Python, Flask, and Redis.

## Features

- **Leader Election**: Automatic leader election using Redis distributed locks
- **Message Production**: Support for producing messages to topics
- **High Availability**: Automatic failover when the leader goes down
- **Distributed**: Multiple broker instances can run simultaneously
- **REST API**: Simple HTTP interface for producing messages and checking status

## Prerequisites

- Python 3.7+
- Redis server running locally (default: localhost:6379)
- Required Python packages (install via `pip install -r requirements.txt`)

## Installation

1. Clone the repository:
   ```bash
   git clone <your-repo-url>
   cd broker
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Start Redis server:
   ```bash
   # On Linux/macOS
   redis-server
   
   # On Windows (if using WSL or similar)
   # or download Redis for Windows
   ```

## Running the Broker

### Starting a Single Broker

```bash
python broker.py
```

### Starting Multiple Brokers

Open multiple terminals and run:

```bash
# Terminal 1 (Leader)
python broker.py

# Terminal 2 (Follower)
FLASK_RUN_PORT=5002 BROKER_ID=broker-2 python broker.py

# Terminal 3 (Another Follower)
FLASK_RUN_PORT=5003 BROKER_ID=broker-3 python broker.py
```

## API Endpoints

### Produce a Message

```http
POST /produce
Content-Type: application/json

{
    "message": "Your message here",
    "topic": "your-topic"
}
```

### Get Current Leader

```http
GET /metadata/leader
```

### Health Check

```http
GET /health
```

## Configuration

You can configure the broker using environment variables:

- `BROKER_ID`: Unique identifier for the broker (default: `broker-{hostname}`)
- `REDIS_HOST`: Redis server host (default: `localhost`)
- `REDIS_PORT`: Redis server port (default: `6379`)
- `FLASK_RUN_PORT`: Port for the Flask server (default: `5001`)
- `LEASE_TIME_SECONDS`: Leader lease duration in seconds (default: `10`)
- `RENEW_INTERVAL_SECONDS`: How often to renew the leader lease (default: `5`)

## How It Works

1. **Leader Election**:
   - Brokers attempt to acquire a distributed lock in Redis to become the leader
   - The lock has a TTL (time-to-live) and must be periodically renewed
   - If the leader fails to renew the lease, another broker can take over

2. **Message Production**:
   - Only the leader can accept produce requests
   - Followers will redirect to the current leader
   - Messages are written to a local log file (in a real implementation, this would be a distributed log)

3. **High Availability**:
   - If the leader fails, the lease will expire
   - Other brokers will detect this and hold a new election
   - The new leader will start accepting produce requests

## Limitations

This is a simplified implementation and has several limitations compared to a production-grade message broker:

- No message persistence (messages are stored in memory)
- No consumer API implemented
- No topic partitioning
- No message replication
- No authentication or authorization

## Future Improvements

- [ ] Implement consumer groups and offset tracking
- [ ] Add support for topic partitioning
- [ ] Implement message replication for fault tolerance
- [ ] Add authentication and authorization
- [ ] Support for message batching and compression
- [ ] Add metrics and monitoring

## License

[Your License Here]
