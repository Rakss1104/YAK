# Quick Start Guide - Local Testing

This guide helps you test all components on a single machine.

## Step 1: Install Dependencies

```bash
pip install -r requirements.txt
```

## Step 2: Start Redis

Open a terminal and run:
```bash
redis-server
```

Keep this terminal open.

## Step 3: Start Broker

Open a new terminal:
```bash
cd broker
python broker.py
```

You should see:
```
[broker-HOSTNAME] Broker starting on port 5001
[broker-HOSTNAME] Became the leader 🏆
```

Keep this terminal open.

## Step 4: Start Follower

Open a new terminal:
```bash
cd follower
python follower.py
```

You should see:
```
[follower-HOSTNAME] Starting follower broker on port 5002
[follower-HOSTNAME] Monitoring leader: broker-HOSTNAME
```

Keep this terminal open.

## Step 5: Start Producer UI

Open a new terminal:
```bash
cd producer
python producer_server.py
```

You should see:
```
Starting Producer Web UI on port 5003...
```

Keep this terminal open.

## Step 6: Start Consumer UI

Open a new terminal:
```bash
cd consumer
python consumer_server.py
```

You should see:
```
Starting Consumer Web UI on port 5004...
```

Keep this terminal open.

## Step 7: Access the UIs

Open your web browser and visit:

1. **Broker Dashboard**: http://localhost:5001/leader
   - Shows broker status, topics, and activity

2. **Follower Dashboard**: http://localhost:5002/leader
   - Shows follower status and replication

3. **Producer Portal**: http://localhost:5003
   - Send messages and upload files

4. **Consumer Portal**: http://localhost:5004
   - Subscribe to topics and view messages

## Step 8: Test the System

### Test 1: Send a Single Message

1. Go to Producer Portal (http://localhost:5003)
2. In "Send Single Message" section:
   - Topic: `test_topic`
   - Key: `key1`
   - Payload: `{"message": "Hello YAK!", "timestamp": 1234567890}`
3. Click "Send Message"
4. Check the Activity Log - should show success

### Test 2: Consume Messages

1. Go to Consumer Portal (http://localhost:5004)
2. In "Subscribe to Topic" section:
   - Topic Name: `test_topic`
   - Partition: `0`
3. Click "Subscribe"
4. You should see the subscription appear in "Active Subscriptions"
5. Click on the subscription
6. The message should appear in the Messages panel

### Test 3: Upload CSV File

1. Go to Producer Portal (http://localhost:5003)
2. In "Upload & Send File" section:
   - Click to upload a CSV file (use `dataset (1).csv` if available)
   - Topic: `server_metrics`
   - Key Field: `server_id`
3. Click "Send File Data"
4. Watch the statistics update

### Test 4: Multiple Partitions

1. Send messages with different keys:
   - Key: `server_1` → Goes to partition X
   - Key: `server_2` → Goes to partition Y
   - Key: `server_3` → Goes to partition Z

2. Subscribe to different partitions in Consumer Portal:
   - Subscribe to partition 0
   - Subscribe to partition 1
   - Subscribe to partition 2

3. Each subscription will show messages for its partition only

### Test 5: Leader Failover

1. Send some messages from Producer
2. Subscribe to a topic in Consumer
3. Go to the Broker terminal and press `Ctrl+C` to stop it
4. Watch the Follower terminal - it should become leader:
   ```
   [follower-HOSTNAME] *** Successfully became the leader ***
   ```
5. Producer and Consumer should automatically reconnect
6. Send more messages - they should work!
7. Consumer should continue receiving messages

### Test 6: Monitor Dashboards

1. Open both dashboards side by side:
   - Broker: http://localhost:5001/leader
   - Follower: http://localhost:5002/leader

2. Send messages from Producer

3. Watch the dashboards update in real-time:
   - Message counts increase
   - Topics appear
   - Activity logs show events
   - Replication status updates

## Troubleshooting

### "No leader available" in Producer
- Check that broker is running
- Check Redis is running
- Wait a few seconds for leader election

### "Connection error" in Consumer
- Check that broker is running
- Verify the broker URLs in consumer_server.py

### Messages not appearing in Consumer
- Verify topic name matches exactly
- Check partition number (0, 1, or 2)
- Make sure you clicked on the subscription to view messages

### Replication failing
- Check that FOLLOWER_URL in broker.py points to correct address
- For local testing, use `http://localhost:5002`

## Stopping Everything

To stop all components:

1. Press `Ctrl+C` in each terminal (Consumer, Producer, Follower, Broker)
2. Press `Ctrl+C` in Redis terminal
3. Close all browser tabs

## Next Steps

- Read [DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md) for multi-machine deployment
- Explore the API endpoints
- Try different partition counts
- Test with larger datasets
- Monitor the activity logs

## Tips

- Keep all terminals visible to see logs
- Use browser developer console (F12) to debug UI issues
- Check the Activity Log in dashboards for detailed events
- Statistics update in real-time in all UIs
