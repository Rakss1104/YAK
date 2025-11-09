import redis
import requests
import time
import threading
import logging
import socket
import json # <-- ADDED for proper JSON handling
import os # <-- ADDED to check if log file exists
import uuid # <-- ADDED for message IDs

from flask import Flask, request, jsonify, render_template

app = Flask(__name__)

# ---------------- CONFIG ----------------
FOLLOWER_URL = "http://192.168.191.242:5002"
BROKER_ID = f"broker-{socket.gethostname()}"
REDIS_HOST = "192.168.191.242"
REDIS_PORT = 6379
LEASE_TIME_SECONDS = 10
RENEW_INTERVAL_SECONDS = 5
PORT = 5001
LOG_FILE = f"{BROKER_ID}_log.txt"

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ---------------- GLOBAL STATE ----------------
IS_LEADER = False
LEASE_RENEWAL_THREAD = None

# Topic and Partition Management
TOPICS = {}  # {topic_name: {partition_id: log_file_path}}
DEFAULT_PARTITIONS = 3  # Default number of partitions per topic

# Metrics tracking
METRICS = {
    "messages_produced": 0,
    "messages_consumed": 0,
    "replications": 0,
    "elections_won": 0,
    "leadership_changes": 0,
    "last_replication": None,
    "recent_activity": [],  # Store recent activity logs
    "topics": {}  # {topic_name: {"partitions": count, "messages": count}}
}

def add_activity_log(log_type, message):
    """Add an activity log entry."""
    timestamp = time.strftime("%H:%M:%S")
    METRICS["recent_activity"].insert(0, {
        "type": log_type,
        "message": message,
        "timestamp": timestamp
    })
    # Keep only last 50 entries
    if len(METRICS["recent_activity"]) > 50:
        METRICS["recent_activity"].pop()

def get_partition_for_key(key, num_partitions):
    """Hash-based partition assignment."""
    if key is None:
        return 0
    return hash(key) % num_partitions

def ensure_topic_exists(topic_name, num_partitions=DEFAULT_PARTITIONS):
    """Create topic with partitions if it doesn't exist."""
    if topic_name not in TOPICS:
        TOPICS[topic_name] = {}
        for partition_id in range(num_partitions):
            # Define log file path for this partition
            log_file = f"{BROKER_ID}_{topic_name}_p{partition_id}.log"
            TOPICS[topic_name][partition_id] = log_file
       
        # Initialize metrics for the new topic
        if topic_name not in METRICS["topics"]:
            METRICS["topics"][topic_name] = {
                "partitions": num_partitions,
                "messages": 0
            }
           
        logger.info(f"[{BROKER_ID}] Ensured topic '{topic_name}' with {num_partitions} partitions")
        add_activity_log("topic", f"Ensured topic '{topic_name}' with {num_partitions} partitions")
    return TOPICS[topic_name]


# ---------------- REDIS INIT ----------------
try:
    REDIS_CLIENT = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
    REDIS_CLIENT.ping()
    logger.info(f"[{BROKER_ID}] Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
except redis.ConnectionError as e:
    logger.error(f"[{BROKER_ID}] Failed to connect to Redis: {e}")
    raise


# ---------------- ROUTES ----------------

@app.route("/produce", methods=["POST"])
def handle_produce():
    """
    Handle producer requests.
    1. Check if this broker is the leader.
    2. Write the message to the local log for the correct partition.
    3. Replicate to the follower.
    4. Update the High Water Mark (HWM) in Redis.
    5. Acknowledge the producer.
    """
    global IS_LEADER
    if not IS_LEADER:
        leader_id = REDIS_CLIENT.get("leader_lease")
        if not leader_id:
            # Service unavailable, no leader elected
            return jsonify({"error": "No leader elected yet. Please try again."}), 503
        # Not the leader, redirect
        return jsonify({"error": "Not the leader", "leader_id": leader_id}), 400

    data = request.get_json()
    if not data:
        return jsonify({"error": "No data provided"}), 400

    logger.info(f"[{BROKER_ID}] Raw data received: {data}")
   
    # Extract topic, key, and payload from the nested structure
    data_payload = data.get('data', {})  # Get the nested 'data' object
    topic = data_payload.get('topic') or data.get('topic') or 'default'  # Handle both nested and top-level topic
    key = data_payload.get('key') or data.get('key')  # Handle both nested and top-level key
    payload = data_payload.get('payload', data_payload)  # Use entire data_payload as fallback
   
    logger.info(f"[{BROKER_ID}] Received produce request for topic '{topic}' with key '{key}'")
    METRICS["messages_produced"] += 1
    add_activity_log("produce", f"Message received for topic '{topic}'")

    try:
        # Ensure topic exists
        topic_partitions = ensure_topic_exists(topic)
       
        # Determine partition
        partition_id = get_partition_for_key(key, len(topic_partitions))
        log_file = topic_partitions[partition_id]
       
        # Prepare message with metadata
        message = {
            "msg_id": data.get("msg_id", str(uuid.uuid4())),
            "topic": topic,
            "partition": partition_id,
            "key": key,
            "payload": payload,
            "timestamp": time.time()
        }
       
        # Step 1: Write locally to partition log
        with open(log_file, "a") as f:
            f.write(json.dumps(message) + "\n")
       
        # Update topic metrics
        METRICS["topics"][topic]["messages"] += 1

        # Step 2: Replicate to follower (non-blocking)
        if FOLLOWER_URL:
            try:
                logger.info(f"[{BROKER_ID}] Attempting to replicate to follower at {FOLLOWER_URL}...")
                rep = requests.post(f"{FOLLOWER_URL}/internal/replicate", json=message, timeout=3)
                if rep.status_code == 200:
                    logger.info(f"[{BROKER_ID}] Follower acknowledged replication.")
                    METRICS["replications"] += 1
                    METRICS["last_replication"] = time.strftime("%H:%M:%S")
                    add_activity_log("replicate", f"Data replicated to follower for topic '{topic}'")
                else:
                    logger.warning(f"[{BROKER_ID}] Follower replication failed with status {rep.status_code}. Continuing...")
                    add_activity_log("warning", f"Follower replication failed (status {rep.status_code}) but message was still committed")
            except requests.exceptions.RequestException as e:
                logger.warning(f"[{BROKER_ID}] Failed to reach follower: {e}. Message will still be committed.")
                add_activity_log("warning", f"Could not reach follower: {str(e)}")
           
        # Step 3: Commit (update HWM for this topic-partition)
        hwm_key = f"hwm:{topic}:{partition_id}"
        new_hwm = REDIS_CLIENT.incr(hwm_key)
        logger.info(f"[{BROKER_ID}] Commit success. Topic: {topic}, Partition: {partition_id}, HWM: {new_hwm}")

        return jsonify({
            "status": "success",
            "offset": new_hwm,
            "topic": topic,
            "partition": partition_id,
            "leader_id": BROKER_ID
        }), 200

    except Exception as e:
        logger.error(f"[{BROKER_ID}] Failed to process produce request: {e}")
        return jsonify({"error": "Internal error"}), 500


@app.route("/metadata/leader", methods=["GET"])
def get_leader():
    try:
        leader_id = REDIS_CLIENT.get("leader_lease")
        if leader_id:
            # This is correct. The consumer will use this.
            return jsonify({"leader_id": leader_id, "is_leader": leader_id == BROKER_ID})
        return jsonify({"error": "No leader elected"}), 404
    except Exception as e:
        logger.error(f"[{BROKER_ID}] Error getting leader: {e}")
        return jsonify({"error": "Failed to fetch leader info"}), 500


# --- THIS IS THE KEY FUNCTION FOR THE CONSUMER ---
@app.route("/consume", methods=["GET"])
def handle_consume():
    global IS_LEADER
    if not IS_LEADER:
        return jsonify({"error": "Not the leader"}), 400

    try:
        # Get parameters from consumer
        topic = request.args.get('topic', 'default')
        partition = int(request.args.get('partition', 0))
        offset = int(request.args.get('offset', 0))
    except ValueError:
        return jsonify({"error": "Invalid parameters"}), 400

    try:
        # Ensure topic exists - if not, create it
        if topic not in TOPICS:
            logger.info(f"Topic '{topic}' does not exist, creating it...")

            ensure_topic_exists(topic)
       
        if partition not in TOPICS[topic]:
            return jsonify({"messages": [], "error": f"Partition {partition} does not exist for topic '{topic}'"}), 404
       
        log_file = TOPICS[topic][partition]
       
        # Get the High Water Mark (HWM) for this topic-partition
        hwm_key = f"hwm:{topic}:{partition}"
        hwm_str = REDIS_CLIENT.get(hwm_key)
        high_water_mark = int(hwm_str) if hwm_str else 0
       
        messages = []
        current_offset = 0 # Log files are 0-indexed

        if not os.path.exists(log_file):
            logger.info(f"No log file found for {topic}:p{partition}, returning empty list.")
            return jsonify({"messages": []})
       
        # Read the log file line by line
        with open(log_file, "r") as f:
            for line in f:
                # Our HWM is 1-based, so msg_offset is current_offset + 1
                msg_offset = current_offset + 1

                if msg_offset > high_water_mark:
                    # We have read all committed messages
                    break
               
                if msg_offset > offset:
                    # This is a new message the consumer hasn't seen
                    try:
                        msg_data = json.loads(line.strip())
                        messages.append({
                            "offset": msg_offset,
                            "topic": topic,
                            "partition": partition,
                            "data": msg_data
                        })
                    except json.JSONDecodeError:
                        logger.warning(f"Skipping corrupt log line at offset {msg_offset}")

                current_offset += 1
       
        # Return the messages
        METRICS["messages_consumed"] += len(messages)
        if messages:
            add_activity_log("consume", f"Served {len(messages)} message(s) from {topic}:p{partition} at offset {offset}")
        return jsonify({"messages": messages, "high_water_mark": high_water_mark})

    except Exception as e:
        logger.error(f"Error in /consume: {e}")
        return jsonify({"error": "Internal server error"}), 500


@app.route("/health", methods=["GET"])
def health():
    try:
        redis_ok = REDIS_CLIENT.ping()
        return jsonify({
            "status": "healthy",
            "broker_id": BROKER_ID,
            "is_leader": IS_LEADER,
            "redis_connected": redis_ok,
            "leader": REDIS_CLIENT.get("leader_lease") or "none"
        })
    except Exception as e:
        return jsonify({"status": "unhealthy", "error": str(e)}), 500


@app.route("/leader", methods=["GET"])
def leader_dashboard():
    """Serve the broker dashboard UI."""
    return render_template("dashboard.html")


@app.route("/topics", methods=["GET"])
def list_topics():
    """List all topics and their partitions."""
    try:
        topics_info = []
        for topic_name, partitions in TOPICS.items():
            topic_data = {
                "name": topic_name,
                "partitions": len(partitions),
                "messages": METRICS["topics"].get(topic_name, {}).get("messages", 0)
            }
            topics_info.append(topic_data)
        return jsonify({"topics": topics_info})
    except Exception as e:
        logger.error(f"Error listing topics: {e}")
        return jsonify({"error": "Failed to list topics"}), 500


@app.route("/metrics", methods=["GET"])
def get_metrics():
    """Return broker metrics for the dashboard."""
    try:
        # Check follower health
        follower_health = "Unknown"
        if FOLLOWER_URL:
            try:
                resp = requests.get(f"{FOLLOWER_URL}/health", timeout=2)
                if resp.status_code == 200:
                    follower_health = " Healthy"
                else:
                    follower_health = " Degraded"
            except:
                follower_health = " Unreachable"
       
        return jsonify({
            "messages_produced": METRICS["messages_produced"],
            "messages_consumed": METRICS["messages_consumed"],
            "replications": METRICS["replications"],
            "elections_won": METRICS["elections_won"],
            "leadership_changes": METRICS["leadership_changes"],
            "follower_url": FOLLOWER_URL,
            "follower_health": follower_health,
            "last_replication": METRICS["last_replication"],
            "lease_time_seconds": LEASE_TIME_SECONDS,
            "recent_activity": METRICS["recent_activity"][:20],  # Last 20 activities
            "topics": METRICS["topics"]
        })
    except Exception as e:
        logger.error(f"Error fetching metrics: {e}")
        return jsonify({"error": "Failed to fetch metrics"}), 500


# ---------------- LEADER ELECTION ----------------

def start_lease_renewal():
    """Keep renewing lease if we're leader."""
    global LEASE_RENEWAL_THREAD, IS_LEADER

    if LEASE_RENEWAL_THREAD and LEASE_RENEWAL_THREAD.is_alive():
        return

    def _renew_lease():
        global IS_LEADER
        while IS_LEADER:
            try:
                time.sleep(RENEW_INTERVAL_SECONDS)
                success = REDIS_CLIENT.set("leader_lease", BROKER_ID, ex=LEASE_TIME_SECONDS, xx=True)
                if not success:
                    logger.warning(f"[{BROKER_ID}] Lost leadership (lease renewal failed).")
                    IS_LEADER = False
                    break
            except Exception as e:
                logger.error(f"[{BROKER_ID}] Lease renewal error: {e}")
                IS_LEADER = False
                break

    LEASE_RENEWAL_THREAD = threading.Thread(target=_renew_lease, daemon=True)
    LEASE_RENEWAL_THREAD.start()


def attempt_leader_election():
    """
    Try to become leader.
    !!! THIS IS THE UPDATED FUNCTION !!!
    It forcefully takes leadership by not using 'nx=True'.
    """
    global IS_LEADER
    try:
        # Updated log message
        logger.info(f"[{BROKER_ID}] Attempting preferred leader election (forceful takeover)...")
       
        # --- THIS IS THE CHANGE ---
        # Removed 'nx=True' to forcefully overwrite any existing lease.
        success = REDIS_CLIENT.set("leader_lease", BROKER_ID, ex=LEASE_TIME_SECONDS)
       
        # Since 'set' without 'nx' always returns True on success (or raises error),
        # we can assume we are the leader if no error was raised.
        IS_LEADER = True
        logger.info(f"[{BROKER_ID}] Became the leader (or reaffirmed leadership) 🏆")
        METRICS["elections_won"] += 1
        add_activity_log("election", f"{BROKER_ID} won the election and became leader 🏆")
        start_lease_renewal()

    except Exception as e:
        logger.error(f"[{BROKER_ID}] Leader election error: {e}")
        IS_LEADER = False


def watch_leader_status():
    """Keep checking if leadership changes."""
    global IS_LEADER
    while True:
        try:
            time.sleep(LEASE_TIME_SECONDS // 2)
            current = REDIS_CLIENT.get("leader_lease")
            if not current:
                logger.info(f"[{BROKER_ID}] No leader found, retrying election...")
                # This broker is the preferred leader, so it should
                # always try to take leadership if there is none.
                attempt_leader_election()
            elif current == BROKER_ID and not IS_LEADER:
                logger.info(f"[{BROKER_ID}] Regained leadership unexpectedly.")
                IS_LEADER = True
                start_lease_renewal()
            elif current != BROKER_ID and IS_LEADER:
                # This should not happen, as this broker should
                # always be renewing its own lease.
                logger.warning(f"[{BROKER_ID}] Leadership stolen by {current}")
                IS_LEADER = False
                METRICS["leadership_changes"] += 1
                add_activity_log("election", f"Leadership transferred to {current}")
                # Forcefully reclaim leadership as we are the preferred leader
                attempt_leader_election()
            elif current != BROKER_ID and not IS_LEADER:
                # This is the expected state when the leader is down
                # and the follower has taken over. We must try to reclaim.
                logger.info(f"[{BROKER_ID}] Follower {current} is leader. Attempting to reclaim...")
                attempt_leader_election()

        except Exception as e:
            logger.error(f"[{BROKER_ID}] Exception in leader watch thread: {e}")
            time.sleep(3)


# ---------------- MAIN ----------------
def main():
    # This broker (leader.py) should not wait.
    # It should immediately try to become the leader.
    attempt_leader_election()
   
    # The watch_leader_status thread will handle re-election
    # if this broker crashes and comes back up.
    threading.Thread(target=watch_leader_status, daemon=True).start()

    logger.info(f"[{BROKER_ID}] Broker starting on port {PORT}")
    logger.info(f"[{BROKER_ID}] Current leader: {REDIS_CLIENT.get('leader_lease') or 'None'}")

    app.run(host="0.0.0.0", port=PORT, debug=False)


if __name__ == "__main__":
    main()
