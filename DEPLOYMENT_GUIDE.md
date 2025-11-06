# YAK Distributed Message Broker - Deployment Guide

## Overview

YAK is a distributed message broker system with topic-based partitioning, leader election, and replication. This guide explains how to deploy all components across different machines.

## System Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Producer   │────▶│   Broker    │◀────│  Follower   │
│  (Port 5003)│     │  (Port 5001)│     │ (Port 5002) │
└─────────────┘     └──────┬──────┘     └─────────────┘
                           │
                           │ Redis
                           │ (Leader Election)
                           │
                    ┌──────▼──────┐
                    │  Consumer   │
                    │ (Port 5004) │
                    └─────────────┘
```

## Components

1. **Broker (Leader)** - Main message broker with leader election
2. **Follower** - Backup broker that replicates data and can become leader
3. **Producer** - Web UI to send messages and upload files
4. **Consumer** - Web UI to subscribe to topics and view messages
5. **Redis** - Distributed coordination for leader election

## Prerequisites

- Python 3.7+
- Redis server
- Network connectivity between machines

## Installation

### 1. Install Dependencies

On each machine, install required packages:

```bash
pip install flask redis requests pandas
```

### 2. Configure Redis

Install and start Redis on a machine accessible to all brokers:

```bash
# Linux/Mac
redis-server

# Or specify config
redis-server --bind 0.0.0.0 --port 6379
```

### 3. Update Configuration

Update the following files with your actual IP addresses:

#### Broker (`broker/broker.py`)
```python
FOLLOWER_URL = "http://<FOLLOWER_IP>:5002"
REDIS_HOST = "<REDIS_IP>"
REDIS_PORT = 6379
PORT = 5001
```

#### Follower (`follower/follower.py`)
```python
REDIS_HOST = '<REDIS_IP>'
REDIS_PORT = 6379
PORT = 5002
```

#### Producer (`producer/producer_server.py`)
```python
BROKER_URLS = [
    "http://<BROKER_IP>:5001", 
    "http://<FOLLOWER_IP>:5002"
]
```

#### Consumer (`consumer/consumer_server.py`)
```python
BROKER_NODES = [
    'http://<BROKER_IP>:5001',
    'http://<FOLLOWER_IP>:5002'
]
```

## Deployment

### Machine 1: Broker (Leader)

```bash
cd broker
python broker.py
```

Access dashboard: `http://<BROKER_IP>:5001/leader`

### Machine 2: Follower

```bash
cd follower
python follower.py
```

Access dashboard: `http://<FOLLOWER_IP>:5002/leader`

### Machine 3: Producer

```bash
cd producer
python producer_server.py
```

Access UI: `http://<PRODUCER_IP>:5003`

### Machine 4: Consumer

```bash
cd consumer
python consumer_server.py
```

Access UI: `http://<CONSUMER_IP>:5004`

## Usage

### Broker/Follower Dashboard

Both broker and follower have identical dashboards showing:
- **Broker Status**: Health, role (leader/follower), Redis connection
- **Message Statistics**: Messages produced, consumed, replications
- **Topics & Partitions**: All created topics with partition counts
- **Activity Log**: Real-time activity feed

### Producer Portal

1. **Send Single Message**:
   - Enter topic name (e.g., `server_metrics`)
   - Optional: Add a key for partition routing
   - Enter JSON payload
   - Click "Send Message"

2. **Upload & Send File**:
   - Click to upload a CSV file
   - Specify topic name
   - Specify key field from CSV (for partition routing)
   - Click "Send File Data" to batch send all records

### Consumer Portal

1. **Subscribe to Topic**:
   - Enter topic name (e.g., `server_metrics`)
   - Select partition (0, 1, or 2)
   - Click "Subscribe"

2. **View Messages**:
   - Click on any active subscription
   - Messages appear in real-time
   - Shows offset, topic, partition, and payload

3. **Unsubscribe**:
   - Click "Unsubscribe" button on any subscription

## Features

### Topics and Partitions

- **Automatic Topic Creation**: Topics are created automatically when first message is sent
- **Default Partitions**: Each topic has 3 partitions by default
- **Key-Based Routing**: Messages with the same key go to the same partition
- **Load Distribution**: Keys are hashed to distribute load across partitions

### Leader Election

- **Automatic Failover**: If leader fails, follower automatically becomes leader
- **Lease-Based**: Leader must renew lease every 5 seconds
- **Redis Coordination**: Uses Redis for distributed lock

### Replication

- **Synchronous Replication**: Leader replicates to follower before commit
- **Topic-Aware**: Replication maintains topic and partition structure
- **Automatic Recovery**: Follower has all data if it becomes leader

### High Availability

- **Producer Auto-Discovery**: Automatically finds current leader
- **Consumer Auto-Discovery**: Automatically reconnects to new leader
- **Graceful Failover**: No message loss during leader transition

## API Endpoints

### Broker/Follower

- `GET /health` - Health check
- `POST /produce` - Produce message
- `GET /consume?topic=X&partition=Y&offset=Z` - Consume messages
- `GET /topics` - List all topics
- `GET /metrics` - Get broker metrics
- `GET /metadata/leader` - Get current leader
- `GET /leader` - Dashboard UI

### Producer

- `GET /` - Producer UI
- `GET /api/leader` - Get current leader
- `POST /api/send` - Send single message
- `POST /api/upload` - Upload CSV file
- `POST /api/send_batch` - Send batch from file

### Consumer

- `GET /` - Consumer UI
- `GET /api/leader` - Get current leader
- `GET /api/topics` - List available topics
- `POST /api/subscribe` - Subscribe to topic
- `GET /api/subscriptions` - List subscriptions
- `GET /api/messages/<id>` - Get messages for subscription
- `DELETE /api/unsubscribe/<id>` - Unsubscribe

## Testing Failover

1. Start broker and follower
2. Send messages from producer
3. Verify messages in consumer
4. Stop broker (Ctrl+C)
5. Follower automatically becomes leader
6. Producer and consumer reconnect automatically
7. Continue sending/receiving messages

## Troubleshooting

### Cannot Connect to Redis
- Verify Redis is running: `redis-cli ping`
- Check firewall allows port 6379
- Verify REDIS_HOST is correct IP

### No Leader Elected
- Check Redis connection on both brokers
- Verify network connectivity between machines
- Check logs for election errors

### Messages Not Appearing
- Verify topic name matches between producer and consumer
- Check partition number (0-2)
- Verify leader is elected
- Check broker logs for errors

### Replication Failing
- Verify FOLLOWER_URL in broker config
- Check network connectivity to follower
- Verify follower is running

## Performance Tips

1. **Partition Count**: Increase DEFAULT_PARTITIONS for higher throughput
2. **Batch Sending**: Use file upload for bulk data
3. **Consumer Polling**: Adjust polling interval in consumer_server.py
4. **Redis Tuning**: Use Redis persistence for production

## Security Considerations

⚠️ **This is a development/demo system. For production:**

- Add authentication/authorization
- Use TLS/SSL for all connections
- Secure Redis with password
- Add rate limiting
- Implement proper error handling
- Add monitoring and alerting

## File Structure

```
YAK/
├── broker/
│   ├── broker.py              # Main broker with leader election
│   └── templates/
│       └── dashboard.html     # Broker dashboard UI
├── follower/
│   └── follower.py            # Follower broker
├── producer/
│   ├── produce.py             # CLI producer (legacy)
│   ├── producer_server.py     # Web UI server
│   └── templates/
│       └── producer.html      # Producer UI
├── consumer/
│   ├── consumer.py            # CLI consumer (legacy)
│   ├── consumer_server.py     # Web UI server
│   └── templates/
│       └── consumer.html      # Consumer UI
└── DEPLOYMENT_GUIDE.md        # This file
```

## Support

For issues or questions, check the logs on each component:
- Broker: Console output from `broker.py`
- Follower: Console output from `follower.py`
- Producer: Console output from `producer_server.py`
- Consumer: Console output from `consumer_server.py`

All components log to console with timestamps and log levels.
