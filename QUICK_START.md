# YAK Broker - Quick Start Guide

## 🚀 Start the Broker

```bash
cd broker
python broker.py
```

The broker will start on `http://localhost:5001`

## 📊 Access the Dashboard

Open your browser and go to:
```
http://localhost:5001/leader
```

## 🧪 Test with Sample Data

### Terminal 1: Start Producer
```bash
python test_producer.py
```
Sends messages every 2-5 seconds

### Terminal 2: Start Consumer
```bash
python test_consumer.py
```
Consumes messages every 3 seconds

### Terminal 3: Watch the Dashboard
Open `http://localhost:5001/leader` in your browser

## 📈 What You'll See

The dashboard displays:
- ✅ **Broker Status**: Leader/Follower role, health, Redis connection
- 📊 **Message Stats**: HWM, produced/consumed counts, replications
- 🔄 **Follower Info**: Follower health and replication status
- 🗳️ **Elections**: Lease time, elections won, leadership changes
- 📝 **Activity Log**: Real-time feed of all broker events

## 🎨 Dashboard Features

- **Auto-refresh**: Updates every 2 seconds
- **Color-coded events**:
  - 🟢 Green: Produce events
  - 🔵 Blue: Consume events
  - 🟡 Yellow: Replication events
  - 🟣 Purple: Election events
- **Health indicators**: Visual status with pulsing effects
- **Responsive design**: Works on desktop and mobile

## 📡 API Endpoints

- `POST /produce` - Send messages to broker
- `GET /consume?offset=N` - Consume messages from offset
- `GET /health` - Broker health check
- `GET /metrics` - Detailed metrics
- `GET /metadata/leader` - Get current leader
- `GET /leader` - Dashboard UI

Enjoy monitoring your YAK broker! 🎉
