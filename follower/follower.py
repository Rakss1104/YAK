import redis
import time
import threading
import logging
import socket
import requests
import json # <-- ADDED
import os # <-- ADDED

from flask import Flask, request, jsonify, render_template

# Initialize Flask app first
app = Flask(__name__, template_folder='../broker/templates')

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuration ---
BROKER_ID = f"follower-{socket.gethostname()}"
REDIS_HOST = '192.168.191.242'
REDIS_PORT = 6379
PORT = 5002
FOLLOWER_URL = None # This is correct, as it has no follower
LOG_FILE = f"{BROKER_ID}_log.txt" # <-- ADDED for consistency

LEASE_TIME_SECONDS = 10
RENEW_INTERVAL_SECONDS = 5
# --- End Configuration ---


# Global state
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
    "recent_activity": [],
    "topics": {}
}

def add_activity_log(log_type, message):
    """Add an activity log entry."""
    timestamp = time.strftime("%H:%M:%S")
    METRICS["recent_activity"].insert(0, {
        "type": log_type,
        "message": message,
        "timestamp": timestamp
    })
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
            log_file = f"{BROKER_ID}_{topic_name}_p{partition_id}.log"
            TOPICS[topic_name][partition_id] = log_file
        METRICS["topics"][topic_name] = {"partitions": num_partitions, "messages": 0}
        logger.info(f"[{BROKER_ID}] Created topic '{topic_name}' with {num_partitions} partitions")
        add_activity_log("topic", f"Created topic '{topic_name}' with {num_partitions} partitions")
    return TOPICS[topic_name]

# Initialize Redis client
try:
    REDIS_CLIENT = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
    REDIS_CLIENT.ping()
    logger.info(f"[{BROKER_ID}] Successfully connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
except redis.ConnectionError as e:
    logger.error(f"[{BROKER_ID}] Failed to connect to Redis: {e}")
    raise

# --- Follower's Replication Endpoint ---

@app.route("/internal/replicate", methods=["POST"])
def handle_replicate():
    """
    This is the follower-only endpoint.
    It receives data from the leader and writes it to its local log.
    """
    if IS_LEADER:
        logger.warning(f"[{BROKER_ID}] Received replication request while being leader. Ignoring.")
        return jsonify({"error": "I am the leader, not a follower"}), 400

    data = request.get_json()
    if not data:
        return jsonify({"error": "No data provided"}), 400
    
    # Extract topic and partition from message
    topic = data.get("topic", "default")
    partition = data.get("partition", 0)
        
    try:
        # Ensure topic exists
        topic_partitions = ensure_topic_exists(topic)
        
        # Get the log file for this partition
        if partition not in topic_partitions:
            return jsonify({"error": f"Partition {partition} does not exist"}), 400
            
        log_file = topic_partitions[partition]
        
        # Write to partition log
        with open(log_file, "a") as f:
            f.write(json.dumps(data) + "\n")
        
        # Update metrics
        METRICS["replications"] += 1
        METRICS["topics"][topic]["messages"] += 1
        METRICS["last_replication"] = time.strftime("%H:%M:%S")
        add_activity_log("replicate", f"Replicated message for topic '{topic}':p{partition}")
            
        logger.info(f"[{BROKER_ID}] Successfully replicated data to {topic}:p{partition}")
        return jsonify({"status": "ack"}), 200
        
    except Exception as e:
        logger.error(f"[{BROKER_ID}] Failed to write replicated data: {str(e)}")
        return jsonify({"error": "Failed to save replicated data"}), 500

# --- Dual-Purpose Produce Endpoint ---

@app.route("/produce", methods=["POST"])
def handle_produce():
    """
    Handles produce requests.
    - If NOT leader (follower mode): Rejects the request.
    - If IS leader (promoted mode): Acts as the new leader.
    """
    if not IS_LEADER:
        leader_id = REDIS_CLIENT.get("leader_lease")
        logger.warning(f"[{BROKER_ID}] Received produce request while being a follower. Rejecting.")
        if not leader_id:
            return jsonify({"error": "No leader elected yet. Please try again."}), 503
        return jsonify({
            "error": "Not the leader. I am a follower.",
            "leader_id": leader_id
        }), 400 # Use 400-level error

    # --- LEADER BEHAVIOR (if promoted) ---
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data provided"}), 400
        
    logger.info(f"[{BROKER_ID} (Promoted Leader)] Received produce request: {data}")

    try:
        # --- FIX: Write as proper JSON line ---
        with open(LOG_FILE, "a") as f:
            f.write(json.dumps(data) + "\n")
        
        # Step 2: Replicate to follower (if follower exists)
        if FOLLOWER_URL:
            # ... (replication logic would go here) ...
            pass
        
        # Step 3: Commit by advancing HWM.
        new_hwm = REDIS_CLIENT.incr("high_water_mark")
        logger.info(f"[{BROKER_ID} (Promoted Leader)] Successfully wrote. New HWM: {new_hwm}")
        
        return jsonify({
            "status": "success",
            "offset": new_hwm,
            "leader_id": BROKER_ID
        }), 200
        
    except Exception as e:
        logger.error(f"[{BROKER_ID} (Promoted Leader)] Failed to process produce request: {str(e)}")
        return jsonify({"error": "Failed to process request"}), 500

# --- ADDED: THE MISSING CONSUME ENDPOINT ---

@app.route("/consume", methods=["GET"])
def handle_consume():
    """
    Handles consume requests ONLY IF this node is the leader.
    This fixes the 404 error on failover.
    """
    global IS_LEADER
    if not IS_LEADER:
        return jsonify({"error": "Not the leader"}), 400

    try:
        # 1. Get offset from consumer
        offset = int(request.args.get('offset', 0))
    except ValueError:
        return jsonify({"error": "Invalid offset"}), 400

    try:
        # 2. Get the High Water Mark (HWM)
        hwm_str = REDIS_CLIENT.get("high_water_mark")
        high_water_mark = int(hwm_str) if hwm_str else 0
        
        messages = []
        current_offset = 0 # Log files are 0-indexed

        if not os.path.exists(LOG_FILE):
            logger.info("No log file found, returning empty list.")
            return jsonify({"messages": []})
        
        # 3. Read the log file line by line
        with open(LOG_FILE, "r") as f:
            for line in f:
                # Our HWM is 1-based, so msg_offset is current_offset + 1
                msg_offset = current_offset + 1

                if msg_offset > high_water_mark:
                    # We have read all *committed* messages
                    break 
                
                if msg_offset > offset:
                    # This is a new message the consumer hasn't seen
                    try:
                        msg_data = json.loads(line.strip())
                        messages.append({
                            "offset": msg_offset,
                            "data": msg_data
                        })
                    except json.JSONDecodeError:
                        logger.warning(f"Skipping corrupt log line at offset {msg_offset}")

                current_offset += 1
        
        # 4. Return the messages
        return jsonify({"messages": messages})

    except Exception as e:
        logger.error(f"Error in /consume: {e}")
        return jsonify({"error": "Internal server error"}), 500


# --- Leader Election and Health Check Code (Same as Leader's) ---

@app.route("/metadata/leader", methods=["GET"])
def get_leader():
    """Get the current leader broker ID."""
    try:
        leader_id = REDIS_CLIENT.get("leader_lease")
        if leader_id:
            return jsonify({
                "leader_id": leader_id,
                "is_leader": leader_id == BROKER_ID
            })
        return jsonify({"error": "No leader elected"}), 404
    except Exception as e:
        logger.error(f"[{BROKER_ID}] Failed to get leader: {str(e)}")
        return jsonify({"error": "Failed to get leader information"}), 500

def start_lease_renewal():
    """Start a background thread to renew the leader lease."""
    global LEASE_RENEWAL_THREAD
    if LEASE_RENEWAL_THREAD and LEASE_RENEWAL_THREAD.is_alive():
        return
        
    def _renew_lease():
        global IS_LEADER
        while IS_LEADER:
            try:
                time.sleep(RENEW_INTERVAL_SECONDS)
                logger.debug(f"[{BROKER_ID}] Renewing leader lease...")
                success = REDIS_CLIENT.set(
                    "leader_lease", 
                    BROKER_ID, 
                    ex=LEASE_TIME_SECONDS,
                    xx=True # Only set if key *already exists*
                )
                if not success:
                    logger.warning(f"[{BROKER_ID}] Lost leadership (lease expired or was stolen)")
                    IS_LEADER = False
                    start_leader_monitoring() 
                    break
            except Exception as e:
                logger.error(f"[{BROKER_ID}] Failed to renew lease: {str(e)}")
                IS_LEADER = False
                start_leader_monitoring() # Start monitoring again
                break
    
    LEASE_RENEWAL_THREAD = threading.Thread(target=_renew_lease, daemon=True)
    LEASE_RENEWAL_THREAD.start()
    logger.info(f"[{BROKER_ID}] Started lease renewal thread.")


def attempt_leader_election():
    """Attempt to become the leader by acquiring the leader lease in Redis."""
    global IS_LEADER
    
    try:
        logger.info(f"[{BROKER_ID}] Attempting leader election...")
        
        success = REDIS_CLIENT.set(
            name="leader_lease",
            value=BROKER_ID,
            ex=LEASE_TIME_SECONDS,
            nx=True  # Only set if not exists
        )
        
        if success:
            IS_LEADER = True
            logger.info(f"[{BROKER_ID}] *** Successfully became the leader ***")
            start_lease_renewal() 
        else:
            IS_LEADER = False
            leader_id = REDIS_CLIENT.get("leader_lease")
            logger.info(f"[{BROKER_ID}] Did not win election. Current leader is {leader_id}")
            
    except Exception as e:
        logger.error(f"[{BROKER_ID}] Error during leader election: {str(e)}")
        IS_LEADER = False

@app.route("/health", methods=["GET"])
def health():
    """Simple health check endpoint."""
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
        return jsonify({
            "messages_produced": METRICS["messages_produced"],
            "messages_consumed": METRICS["messages_consumed"],
            "replications": METRICS["replications"],
            "elections_won": METRICS["elections_won"],
            "leadership_changes": METRICS["leadership_changes"],
            "follower_url": FOLLOWER_URL,
            "follower_health": "N/A",
            "last_replication": METRICS["last_replication"],
            "lease_time_seconds": LEASE_TIME_SECONDS,
            "recent_activity": METRICS["recent_activity"][:20],
            "topics": METRICS["topics"]
        })
    except Exception as e:
        logger.error(f"Error fetching metrics: {e}")
        return jsonify({"error": "Failed to fetch metrics"}), 500

def start_leader_monitoring():
    """
    This is the main loop for the follower.
    It periodically checks if the leader's lease has expired.
    """
    def _monitor_loop():
        global IS_LEADER
        while not IS_LEADER:
            try:
                time.sleep(LEASE_TIME_SECONDS / 2) 
                
                if IS_LEADER: # Check if we became leader in another thread
                    break
                    
                current_leader = REDIS_CLIENT.get("leader_lease")
                if not current_leader:
                    logger.warning(f"[{BROKER_ID}] No leader lease found. Attempting election.")
                    attempt_leader_election()
                else:
                    logger.debug(f"[{BROKER_ID}] Monitoring leader: {current_leader}")
            
            except Exception as e:
                logger.error(f"[{BROKER_ID}] Error in monitor loop: {e}")
                time.sleep(2) # Back off on error
    
    # Start the monitoring thread
    monitor_thread = threading.Thread(target=_monitor_loop, daemon=True)
    monitor_thread.start()
    logger.info(f"[{BROKER_ID}] Started leader monitoring thread.")


def main():
    global IS_LEADER
    IS_LEADER = False # Explicitly start as follower
    
    start_leader_monitoring()
    
    logger.info(f"[{BROKER_ID}] Starting follower broker on port {PORT}")
    logger.info(f"[{BROKER_ID}] Monitoring Redis at {REDIS_HOST} for leader lease.")
    
    try:
        app.run(host='0.0.0.0', port=PORT, debug=False)
    except Exception as e:
        logger.error(f"[{BROKER_ID}] Failed to start server: {e}")
        raise

if __name__ == '__main__':
    main()