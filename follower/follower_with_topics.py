import redis 
import time
import threading
import logging
import socket
import requests
import json
import os
from flask import Flask, request, jsonify

# Initialize Flask app first
app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuration ---
BROKER_ID = f"follower-{socket.gethostname()}"
# This MUST be your ZeroTier IP, since you are hosting Redis
REDIS_HOST = '192.168.191.242' 
REDIS_PORT = 6379
PORT = 5002
FOLLOWER_URL = None 

# --- NEW: Topic Configuration ---
# We will store all log files in this directory
LOG_DIR = "data_logs" 

LEASE_TIME_SECONDS = 10
RENEW_INTERVAL_SECONDS = 5
# --- End Configuration ---


# Global state
IS_LEADER = False
LEASE_RENEWAL_THREAD = None

# Initialize Redis client
try:
    REDIS_CLIENT = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
    REDIS_CLIENT.ping()
    logger.info(f"[{BROKER_ID}] Successfully connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
except redis.ConnectionError as e:
    logger.error(f"[{BROKER_ID}] Failed to connect to Redis: {e}")
    raise

# --- Helper Function for Topic-Based Logging ---
def write_to_topic_log(message_data):
    """
    Extracts the topic from a message and writes it to a topic-specific log file.
    'message_data' is expected to be the dict: {'topic': ..., 'key': ..., 'payload': ...}
    """
    
    # 1. Determine topic
    if not isinstance(message_data, dict) or 'topic' not in message_data:
        logger.warning(f"Message format error, using 'default' topic. Data: {message_data}")
        topic = "default"
    else:
        topic = message_data.get('topic')

    # 2. Ensure log directory exists
    os.makedirs(LOG_DIR, exist_ok=True)
    
    # 3. Define file path and write to log
    log_file_path = os.path.join(LOG_DIR, f"{topic}.log")
    
    with open(log_file_path, "a") as f:
        # Write the full message (including topic/key) as a single JSON line
        f.write(json.dumps(message_data) + "\n")
        
    return topic, log_file_path


# --- Follower's Replication Endpoint (MODIFIED) ---

@app.route("/internal/replicate", methods=["POST"])
def handle_replicate():
    """
    Receives data from the leader and writes it to its local topic log.
    """
    if IS_LEADER:
        logger.warning(f"[{BROKER_ID}] Received replication request while being leader. Ignoring.")
        return jsonify({"error": "I am the leader, not a follower"}), 400

    data = request.get_json()
    if not data:
        return jsonify({"error": "No data provided"}), 400
        
    try:
        # The producer sends {'msg_id': ..., 'data': ...}
        # The 'data' part is what we care about: {'topic': ..., 'key': ..., 'payload': ...}
        message_to_log = data.get('data')
        
        # --- MODIFIED: Use the topic-based write function ---
        topic, log_file = write_to_topic_log(message_to_log)
            
        logger.info(f"[{BROKER_ID}] Successfully replicated data to topic '{topic}' (File: {log_file})")
        return jsonify({"status": "ack"}), 200
        
    except Exception as e:
        logger.error(f"[{BROKER_ID}] Failed to write replicated data: {str(e)}")
        return jsonify({"error": "Failed to save replicated data"}), 500

# --- Dual-Purpose Produce Endpoint (MODIFIED) ---

@app.route("/produce", methods=["POST"])
def handle_produce():
    """
    Handles produce requests.
    - If NOT leader (follower mode): Rejects the request.
    - If IS leader (promoted mode): Acts as the new leader, writes to topic log,
      and increments the topic-specific HWM.
    """
    if not IS_LEADER:
        # ... (Follower rejection logic is unchanged) ...
        leader_id = REDIS_CLIENT.get("leader_lease")
        logger.warning(f"[{BROKER_ID}] Received produce request while being a follower. Rejecting.")
        if not leader_id:
            return jsonify({"error": "No leader elected yet. Please try again."}), 503
        return jsonify({
            "error": "Not the leader. I am a follower.",
            "leader_id": leader_id
        }), 400 

    # --- LEADER BEHAVIOR (if promoted) ---
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data provided"}), 400
        
    logger.info(f"[{BROKER_ID} (Promoted Leader)] Received produce request: {data}")

    try:
        # The 'data' part is what we care about
        message_to_log = data.get('data')

        # --- MODIFIED: Step 1: Write to local topic log ---
        topic, log_file = write_to_topic_log(message_to_log)
        
        # Step 2: Replicate to follower (if follower exists)
        if FOLLOWER_URL:
            # ... (replication logic would go here) ...
            pass
        
        # --- MODIFIED: Step 3: Commit by advancing topic-specific HWM ---
        # We use a Redis Hash 'topic_hwm' to store offsets per topic
        new_topic_hwm = REDIS_CLIENT.hincrby("topic_hwm", topic, 1)
        
        logger.info(f"[{BROKER_ID} (Promoted Leader)] Successfully wrote to topic '{topic}'. New HWM: {new_topic_hwm}")
        
        # Return the topic-specific offset
        return jsonify({
            "status": "success",
            "topic": topic,
            "offset": new_topic_hwm,
            "leader_id": BROKER_ID
        }), 200
        
    except Exception as e:
        logger.error(f"[{BROKER_ID} (Promoted Leader)] Failed to process produce request: {str(e)}")
        return jsonify({"error": "Failed to process request"}), 500

# --- Consume Endpoint (MODIFIED) ---

@app.route("/consume", methods=["GET"])
def handle_consume():
    """
    Handles consume requests ONLY IF this node is the leader.
    It now requires a 'topic' parameter and reads from the correct log,
    checking against the topic-specific HWM.
    """
    global IS_LEADER
    if not IS_LEADER:
        return jsonify({"error": "Not the leader"}), 400

    try:
        # --- MODIFIED: 1. Get topic from consumer ---
        topic = request.args.get('topic')
        if not topic:
            return jsonify({"error": "A 'topic' query parameter is required"}), 400
            
        offset = int(request.args.get('offset', 0))
    except ValueError:
        return jsonify({"error": "Invalid offset"}), 400

    try:
        # --- MODIFIED: 2. Get the High Water Mark (HWM) for this topic ---
        hwm_str = REDIS_CLIENT.hget("topic_hwm", topic)
        high_water_mark = int(hwm_str) if hwm_str else 0
        
        messages = []
        current_offset = 0 # Log files are 0-indexed

        # --- MODIFIED: 3. Construct log path ---
        log_file_path = os.path.join(LOG_DIR, f"{topic}.log")

        if not os.path.exists(log_file_path):
            logger.info(f"No log file found for topic '{topic}', returning empty list.")
            return jsonify({"messages": [], "latest_offset": high_water_mark})
        
        # 4. Read the correct log file line by line
        with open(log_file_path, "r") as f:
            for line in f:
                # Our HWM is 1-based, so msg_offset is current_offset + 1
                msg_offset = current_offset + 1

                if msg_offset > high_water_mark:
                    # We have read all *committed* messages for this topic
                    break 
                
                if msg_offset > offset:
                    # This is a new message the consumer hasn't seen
                    try:
                        msg_data = json.loads(line.strip())
                        messages.append({
                            "offset": msg_offset,
                            # The 'data' is the full message: {'topic':..., 'key':..., 'payload':...}
                            "data": msg_data 
                        })
                    except json.JSONDecodeError:
                        logger.warning(f"Skipping corrupt log line in {topic}.log at offset {msg_offset}")

                current_offset += 1
        
        # 5. Return the messages
        return jsonify({"messages": messages, "latest_offset": high_water_mark})

    except Exception as e:
        logger.error(f"Error in /consume: {e}")
        return jsonify({"error": "Internal server error"}), 500


# --- Leader Election and Health Check Code (Unchanged) ---
# ... (All functions from @app.route("/metadata/leader") to main() are identical) ...

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
            "leader_from_redis": REDIS_CLIENT.get("leader_lease") or "none"
        })
    except Exception as e:
        return jsonify({"status": "unhealthy", "error": str(e)}), 500

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
    
    # Ensure log directory exists on startup
    os.makedirs(LOG_DIR, exist_ok=True) 

    start_leader_monitoring()
    
    logger.info(f"[{BROKER_ID}] Starting follower broker on port {PORT}")
    logger.info(f"[{BROKER_ID}] Log directory: {os.path.abspath(LOG_DIR)}")
    logger.info(f"[{BROKER_ID}] Monitoring Redis at {REDIS_HOST} for leader lease.")
    
    try:
        app.run(host='0.0.0.0', port=PORT, debug=False)
    except Exception as e:
        logger.error(f"[{BROKER_ID}] Failed to start server: {e}")
        raise

if __name__ == '__main__':
    main()