import redis
import requests
import time
import threading
import logging
import socket
import json
import os
import uuid
from flask import Flask, request, jsonify, render_template

app = Flask(__name__)

# ---------------- CONFIG (MODIFIED FOR FOLLOWER) ----------------
FOLLOWER_URL = None
BROKER_ID = f"follower-{socket.gethostname()}"
REDIS_HOST = "192.168.191.242" 
REDIS_PORT = 6379
LEASE_TIME_SECONDS = 10
RENEW_INTERVAL_SECONDS = 5
PORT = 5002

# <-- NEW BLOCK: How long to remember a msg_id to prevent duplicates (in seconds)
# 1 hour = 3600 seconds. Adjust as needed.
IDEMPOTENCE_EXPIRY_SECONDS = 3600

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ---------------- GLOBAL STATE ----------------
IS_LEADER = False
LEASE_RENEWAL_THREAD = None
TOPICS = {}
DEFAULT_PARTITIONS = 3
METRICS = {
    "messages_produced": 0, "messages_consumed": 0,
    "replications_received": 0, 
    "elections_won": 0, "leadership_changes": 0, "last_replication": None,
    "recent_activity": [], "topics": {}
}

def add_activity_log(log_type, message):
    timestamp = time.strftime("%H:%M:%S")
    METRICS["recent_activity"].insert(0, {
        "type": log_type, "message": message, "timestamp": timestamp
    })
    if len(METRICS["recent_activity"]) > 50:
        METRICS["recent_activity"].pop()

def get_partition_for_key(key, num_partitions):
    if key is None:
        return 0
    return hash(key) % num_partitions

def ensure_topic_exists(topic_name, num_partitions=DEFAULT_PARTITIONS):
    if topic_name not in TOPICS:
        TOPICS[topic_name] = {}
        for partition_id in range(num_partitions):
            log_file = f"{BROKER_ID}_{topic_name}_p{partition_id}.log"
            TOPICS[topic_name][partition_id] = log_file
        
        if topic_name not in METRICS["topics"]:
            METRICS["topics"][topic_name] = {"partitions": num_partitions, "messages": 0}
            
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

@app.route("/internal/replicate", methods=["POST"])
def handle_replicate():
    """
    FOLLOWER-ONLY: Receives a message from the leader and writes
    it to the correct partition log.
    
    NOTE: For simplicity, we trust the leader and don't re-check
    idempotence here. The leader is responsible for that.
    """
    if IS_LEADER:
        logger.warning(f"[{BROKER_ID}] Received replication request while leader. Ignoring.")
        return jsonify({"error": "I am the leader, not a follower"}), 400

    data = request.get_json()
    if not data:
        return jsonify({"error": "No data provided"}), 400
        
    try:
        topic = data.get('topic')
        partition_id = data.get('partition')

        if topic is None or partition_id is None:
            logger.warning(f"[{BROKER_ID}] Invalid replication data: {data}")
            return jsonify({"error": "Missing topic or partition"}), 400
            
        topic_partitions = ensure_topic_exists(topic)
        log_file = topic_partitions.get(partition_id)
        
        if not log_file:
            logger.error(f"[{BROKER_ID}] Leader tried to replicate to non-existent partition {partition_id}")
            return jsonify({"error": "Invalid partition"}), 400
            
        # Write to follower's local partition log
        with open(log_file, "a") as f:
            f.write(json.dumps(data) + "\n")
            
        logger.info(f"[{BROKER_ID}] Replicated to {topic}:p{partition_id}")
        METRICS["replications_received"] += 1
        METRICS["last_replication"] = time.strftime("%H:%M:%S")
        add_activity_log("replicate", f"Data received for {topic}:p{partition_id}")
        
        return jsonify({"status": "ack"}), 200
        
    except Exception as e:
        logger.error(f"[{BROKER_ID}] Failed to write replicated data: {str(e)}")
        return jsonify({"error": "Failed to save replicated data"}), 500


@app.route("/produce", methods=["POST"])
def handle_produce():
    """
    DUAL-MODE:
    1. As FOLLOWER: Rejects the request.
    2. As PROMOTED LEADER: Acts as the new leader and CHECKS FOR DUPLICATES.
    """
    global IS_LEADER
    if not IS_LEADER:
        leader_id = REDIS_CLIENT.get("leader_lease")
        if not leader_id:
            return jsonify({"error": "No leader elected yet. Please try again."}), 503
        return jsonify({"error": "Not the leader", "leader_id": leader_id}), 400 

    # --- PROMOTED LEADER LOGIC (WITH IDEMPOTENCE) ---
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data provided"}), 400

    # <-- CHANGED: We now require msg_id
    msg_id = data.get("msg_id")
    if not msg_id:
        return jsonify({"error": "msg_id is required"}), 400

    data_payload = data.get('data', {})
    topic = data_payload.get('topic') or data.get('topic') or 'default'
    key = data_payload.get('key') or data.get('key')
    payload = data_payload.get('payload', data_payload)
    
    logger.info(f"[{BROKER_ID} (Promoted)] Received produce for topic '{topic}' (MsgID: {msg_id})")

    # <-- NEW BLOCK: Idempotence Check using Redis
    # We create a unique key for this message ID.
    # 'nx=True' means "set only if it does not exist"
    # 'ex=...' means "expire after N seconds"
    idempotence_key = f"yak_msg_lock:{msg_id}"
    is_new_message = REDIS_CLIENT.set(
        idempotence_key, 
        "processed", 
        ex=IDEMPOTENCE_EXPIRY_SECONDS, 
        nx=True
    )

    if not is_new_message:
        # This message ID has been seen before.
        logger.warning(f"[{BROKER_ID} (Promoted)] Duplicate message detected (MsgID: {msg_id}). Ignoring.")
        # Return 200 OK so the producer thinks it succeeded
        return jsonify({
            "status": "success_duplicate", 
            "msg_id": msg_id,
            "leader_id": BROKER_ID
        }), 200
    # --- End of Idempotence Check ---

    METRICS["messages_produced"] += 1
    add_activity_log("produce", f"Message received for topic '{topic}'")

    try:
        topic_partitions = ensure_topic_exists(topic)
        partition_id = get_partition_for_key(key, len(topic_partitions))
        log_file = topic_partitions[partition_id]
        
        message = {
            "msg_id": msg_id, # <-- CHANGED: Use the provided msg_id
            "topic": topic, "partition": partition_id, "key": key,
            "payload": payload, "timestamp": time.time()
        }
        
        with open(log_file, "a") as f:
            f.write(json.dumps(message) + "\n")
        
        METRICS["topics"][topic]["messages"] += 1

        # FOLLOWER_URL is None, so replication is skipped
        if FOLLOWER_URL:
            pass

        # Commit (update HWM)
        hwm_key = f"hwm:{topic}:{partition_id}"
        new_hwm = REDIS_CLIENT.incr(hwm_key)
        logger.info(f"[{BROKER_ID} (Promoted)] Commit. {topic}:p{partition_id}, HWM: {new_hwm}")

        return jsonify({
            "status": "success_new", # <-- CHANGED: Be explicit this was a new message
            "offset": new_hwm, "topic": topic,
            "partition": partition_id, "leader_id": BROKER_ID
        }), 200

    except Exception as e:
        logger.error(f"[{BROKER_ID} (Promoted)] Failed to process produce: {e}")
        # <-- NEW BLOCK: If we fail, we should clear the idempotence key so it can be retried
        REDIS_CLIENT.delete(idempotence_key)
        return jsonify({"error": "Internal error"}), 500

# --- ALL OTHER ENDPOINTS ARE IDENTICAL TO LEADER ---
# (No changes to the functions below)

@app.route("/metadata/leader", methods=["GET"])
def get_leader():
    try:
        leader_id = REDIS_CLIENT.get("leader_lease")
        if leader_id:
            return jsonify({"leader_id": leader_id, "is_leader": leader_id == BROKER_ID})
        return jsonify({"error": "No leader elected"}), 404
    except Exception as e:
        logger.error(f"[{BROKER_ID}] Error getting leader: {e}")
        return jsonify({"error": "Failed to fetch leader info"}), 500

@app.route("/consume", methods=["GET"])
def handle_consume():
    global IS_LEADER
    if not IS_LEADER:
        return jsonify({"error": "Not the leader"}), 400
    try:
        topic = request.args.get('topic', 'default')
        partition = int(request.args.get('partition', 0))
        offset = int(request.args.get('offset', 0))
    except ValueError:
        return jsonify({"error": "Invalid parameters"}), 400
    try:
        if topic not in TOPICS:
            logger.warning(f"Consumer asked for non-existent topic '{topic}'")
            return jsonify({"messages": [], "error": f"Topic '{topic}' does not exist"}), 404
        if partition not in TOPICS[topic]:
            return jsonify({"messages": [], "error": f"Partition {partition} does not exist"}), 404
        
        log_file = TOPICS[topic][partition]
        
        hwm_key = f"hwm:{topic}:{partition}"
        hwm_str = REDIS_CLIENT.get(hwm_key)
        high_water_mark = int(hwm_str) if hwm_str else 0
        
        messages = []
        current_offset = 0 
        if not os.path.exists(log_file):
            return jsonify({"messages": []})
        
        with open(log_file, "r") as f:
            for line in f:
                msg_offset = current_offset + 1
                if msg_offset > high_water_mark:
                    break 
                
                # <-- THIS IS THE FIX FROM THE FIRST PROBLEM
                if msg_offset >= offset: 
                    try:
                        msg_data = json.loads(line.strip())
                        messages.append({
                            "offset": msg_offset, "topic": topic,
                            "partition": partition, "data": msg_data
                        })
                    except json.JSONDecodeError:
                        logger.warning(f"Skipping corrupt log line at offset {msg_offset}")
                current_offset += 1
        
        METRICS["messages_consumed"] += len(messages)
        if messages:
            add_activity_log("consume", f"Served {len(messages)} msg(s) from {topic}:p{partition}")
        return jsonify({"messages": messages, "high_water_mark": high_water_mark})
    except Exception as e:
        logger.error(f"Error in /consume: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route("/health", methods=["GET"])
def health():
    try:
        redis_ok = REDIS_CLIENT.ping()
        return jsonify({
            "status": "healthy", "broker_id": BROKER_ID, "is_leader": IS_LEADER,
            "redis_connected": redis_ok, "leader": REDIS_CLIENT.get("leader_lease") or "none"
        })
    except Exception as e:
        return jsonify({"status": "unhealthy", "error": str(e)}), 500

@app.route("/leader", methods=["GET"])
def leader_dashboard():
    return render_template("dashboard.html")

@app.route("/topics", methods=["GET"])
def list_topics():
    try:
        topics_info = []
        for topic_name, topic_data in METRICS["topics"].items():
            topic_data_out = {
                "name": topic_name,
                "partitions": topic_data.get("partitions", 0),
                "messages": topic_data.get("messages", 0)
            }
            topics_info.append(topic_data_out)
        return jsonify({"topics": topics_info})
    except Exception as e:
        logger.error(f"Error listing topics: {e}")
        return jsonify({"error": "Failed to list topics"}), 500

@app.route("/metrics", methods=["GET"])
def get_metrics():
    try:
        follower_health = "N/A (I am a follower)"
        
        return jsonify({
            "messages_produced": METRICS["messages_produced"],
            "messages_consumed": METRICS["messages_consumed"],
            "replications_received": METRICS["replications_received"],
            "elections_won": METRICS["elections_won"],
            "leadership_changes": METRICS["leadership_changes"], "follower_url": "N/A",
            "follower_health": follower_health, "last_replication": METRICS["last_replication"],
            "lease_time_seconds": LEASE_TIME_SECONDS,
            "recent_activity": METRICS["recent_activity"][:20], "topics": METRICS["topics"]
        })
    except Exception as e:
        logger.error(f"Error fetching metrics: {e}")
        return jsonify({"error": "Failed to fetch metrics"}), 500

# ---------------- LEADER ELECTION ----------------

def start_lease_renewal():
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
    global IS_LEADER
    try:
        logger.info(f"[{BROKER_ID}] Attempting leader election...")
        success = REDIS_CLIENT.set("leader_lease", BROKER_ID, ex=LEASE_TIME_SECONDS, nx=True)
        if success:
            IS_LEADER = True
            logger.info(f"[{BROKER_ID}] Became the leader 🏆")
            METRICS["elections_won"] += 1
            add_activity_log("election", f"{BROKER_ID} won the election and became leader 🏆")
            start_lease_renewal()
        else:
            IS_LEADER = False
            leader_id = REDIS_CLIENT.get("leader_lease")
            logger.info(f"[{BROKER_ID}] Current leader: {leader_id}")
    except Exception as e:
        logger.error(f"[{BROKER_ID}] Leader election error: {e}")
        IS_LEADER = False

def watch_leader_status():
    global IS_LEADER
    while True:
        try:
            time.sleep(LEASE_TIME_SECONDS // 2)
            current = REDIS_CLIENT.get("leader_lease")
            if not current:
                logger.info(f"[{BROKER_ID}] No leader found, retrying election...")
                attempt_leader_election()
            elif current == BROKER_ID and not IS_LEADER:
                logger.info(f"[{BROKER_ID}] Regained leadership unexpectedly.")
                IS_LEADER = True
                start_lease_renewal()
            elif current != BROKER_ID and IS_LEADER:
                logger.warning(f"[{BROKER_ID}] Leadership stolen by {current}")
                IS_LEADER = False
                METRICS["leadership_changes"] += 1
                add_activity_log("election", f"Leadership transferred to {current}")
        except Exception as e:
            logger.error(f"[{BROKER_ID}] Exception in leader watch thread: {e}")
            time.sleep(3)

# ---------------- MAIN (MODIFIED FOR FOLLOWER) ----------------
def main():
    threading.Thread(target=watch_leader_status, daemon=True).start()
    
    logger.info(f"[{BROKER_ID}] Follower broker starting on port {PORT}")
    logger.info(f"[{BROKER_ID}] Monitoring leader: {REDIS_CLIENT.get('leader_lease') or 'None'}")

    app.run(host="0.0.0.0", port=PORT, debug=False)

if __name__ == "__main__":
    main()