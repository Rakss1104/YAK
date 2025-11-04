# YAK Broker Dashboard

A real-time web dashboard for monitoring the YAK message broker system.

## Features

### 📊 Real-Time Monitoring
- **Broker Status**: Current role (Leader/Follower), health status, and Redis connection
- **Message Statistics**: High Water Mark, messages produced/consumed, replication count
- **Follower Status**: Follower URL, health check, and last replication time
- **Election Info**: Lease time, elections won, and leadership changes

### 📝 Activity Log
- Real-time activity feed showing:
  - **Produce events**: Messages received from producers
  - **Consume events**: Messages served to consumers
  - **Replicate events**: Data replicated to followers
  - **Election events**: Leadership changes and elections

### 🎨 Visual Design
- Modern gradient UI with color-coded status indicators
- Auto-refreshing every 2 seconds
- Responsive layout that works on all screen sizes
- Color-coded log entries for different event types

## Accessing the Dashboard

1. Start the broker:
   ```bash
   cd broker
   python broker.py
   ```

2. Open your browser and navigate to:
   ```
   http://localhost:5001/leader
   ```

## Testing the Dashboard

### Start a Test Producer
In a new terminal:
```bash
python test_producer.py
```

This will send random messages to the broker every 2-5 seconds.

### Start a Test Consumer
In another terminal:
```bash
python test_consumer.py
```

This will consume messages from the broker every 3 seconds.

### Watch the Dashboard
The dashboard will show:
- Messages being produced and consumed in real-time
- Replication events to followers
- Current broker statistics
- Health status of all components

## Dashboard Sections

### 1. Header
- Broker ID and current role (Leader/Follower)
- Status badge with visual indicator

### 2. Broker Status Card
- Health indicator (green = healthy, red = unhealthy)
- Current role
- Current leader ID
- Redis connection status

### 3. Message Statistics Card
- High Water Mark (last committed offset)
- Total messages produced
- Total messages consumed
- Total replications

### 4. Follower Status Card
- Follower URL
- Follower health check status
- Last replication timestamp

### 5. Election Info Card
- Lease time configuration
- Number of elections won
- Number of leadership changes

### 6. Activity Log
- Chronological feed of all broker activities
- Color-coded by event type
- Shows last 50 events
- Auto-scrolls to show latest events

## Event Types

- 🟢 **PRODUCE**: Message received from a producer
- 🔵 **CONSUME**: Messages served to a consumer
- 🟡 **REPLICATE**: Data replicated to follower
- 🟣 **ELECTION**: Leadership election or change

## API Endpoints

The dashboard uses these endpoints:

- `GET /leader` - Serves the dashboard HTML
- `GET /health` - Broker health and status
- `GET /metrics` - Detailed broker metrics and activity log

## Notes

- The dashboard auto-refreshes every 2 seconds
- Activity log keeps the last 50 events
- Follower health is checked with a 2-second timeout
- All timestamps are in local time (HH:MM:SS format)
