# YAK - Yet Another Kafka

A lightweight, distributed message broker implementation inspired by Apache Kafka, built with Python, Flask, and Redis.

## Features

- **Topics & Partitions**: Automatic topic creation with configurable partitions
- **Key-Based Routing**: Hash-based partition assignment for load distribution
- **Leader Election**: Automatic leader election using Redis distributed locks
- **Synchronous Replication**: Data replicated to follower before commit
- **High Availability**: Automatic failover when the leader goes down
- **Web UIs**: Modern web interfaces for producer, consumer, and broker monitoring
- **REST API**: Simple HTTP interface for all operations

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

## Quick Start

### 1. Start Redis
```bash
redis-server
```

### 2. Start Broker (Terminal 1)
```bash
cd broker
python broker.py
```
Access dashboard: http://localhost:5001/leader

### 3. Start Follower (Terminal 2)
```bash
cd follower
python follower.py
```
Access dashboard: http://localhost:5002/leader

### 4. Start Producer UI (Terminal 3)
```bash
cd producer
python producer_server.py
```
Access UI: http://localhost:5003

### 5. Start Consumer UI (Terminal 4)
```bash
cd consumer
python consumer_server.py
```
Access UI: http://localhost:5004

## Multi-Machine Deployment

For deploying across different machines, see [DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md)

## Usage

### Producer Portal (http://localhost:5003)

1. **Send Single Message**: Enter topic, key, and JSON payload
2. **Upload CSV File**: Batch send data from CSV files
3. **View Statistics**: Track sent/failed messages

### Consumer Portal (http://localhost:5004)

1. **Subscribe to Topic**: Enter topic name and partition
2. **View Messages**: Real-time message display
3. **Multiple Subscriptions**: Subscribe to multiple topics simultaneously

### Broker Dashboard (http://localhost:5001/leader or :5002/leader)

- Monitor broker health and status
- View topics and partitions
- Track message statistics
- See replication status
- View activity logs

## API Endpoints

### Broker/Follower

```http
POST /produce
{
    "topic": "server_metrics",
    "key": "server_1",
    "payload": {"cpu": 75, "memory": 80}
}

GET /consume?topic=server_metrics&partition=0&offset=0
GET /topics
GET /metrics
GET /metadata/leader
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

### Topics and Partitions
- Topics are created automatically on first message
- Each topic has 3 partitions by default (configurable)
- Messages with the same key go to the same partition (hash-based routing)
- Partitions enable parallel processing and load distribution

### Leader Election
- Brokers use Redis distributed locks for leader election
- Leader lease must be renewed every 5 seconds
- If leader fails, follower automatically becomes leader
- Producers and consumers auto-discover new leader

### Replication
- Leader synchronously replicates to follower before commit
- Follower maintains identical topic/partition structure
- High Water Mark (HWM) tracks committed messages per partition
- Consumers only see committed messages

### Message Flow
1. Producer sends message with topic and key
2. Leader determines partition based on key hash
3. Leader writes to partition log file
4. Leader replicates to follower
5. Leader updates HWM in Redis
6. Leader returns success to producer
7. Consumer polls for messages from specific topic/partition

## Architecture Highlights

✅ **Implemented Features**:
- Topic-based partitioning
- Key-based routing
- Leader election with automatic failover
- Synchronous replication
- Offset tracking per partition
- Web UIs for all components
- Real-time dashboards

⚠️ **Production Considerations**:
- Add authentication/authorization
- Implement consumer groups
- Add message compression
- Use persistent storage (not just log files)
- Add monitoring and alerting
- Implement rate limiting
- Add TLS/SSL support

## License

[Your License Here]
