import redis
import requests
import time
import threading
import logging
import socket
import json # <-- ADDED for proper JSON handling
import os # <-- ADDED to check if log file exists

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

# Metrics tracking
METRICS = {
    "messages_produced": 0,
    "messages_consumed": 0,
    "replications": 0,
    "elections_won": 0,
    "leadership_changes": 0,
    "last_replication": None,
    "recent_activity": []  # Store recent activity logs
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
    global IS_LEADER
    if not IS_LEADER:
        leader_id = REDIS_CLIENT.get("leader_lease")
        if not leader_id:
            return jsonify({"error": "No leader elected yet. Please try again."}), 503
        # Return 400-level error, 307 is for redirection which isn't right
        return jsonify({"error": "Not the leader", "leader_id": leader_id}), 400 

    data = request.get_json()
    if not data:
        return jsonify({"error": "No data provided"}), 400

    logger.info(f"[{BROKER_ID}] Received produce request: {data}")
    METRICS["messages_produced"] += 1
    producer_id = data.get("producer_id", "unknown")
    add_activity_log("produce", f"Message received from producer: {producer_id}")

    try:
        # Step 1: Write locally (as a JSON line)
        with open(LOG_FILE, "a") as f:
            # f.write(f"{data}\n") # This is hard to read
            f.write(json.dumps(data) + "\n") # This is much better

        # Step 2: Replicate to follower
        if FOLLOWER_URL:
            try:
                logger.info(f"[{BROKER_ID}] Replicating to follower at {FOLLOWER_URL}...")
                rep = requests.post(f"{FOLLOWER_URL}/internal/replicate", json=data, timeout=3)
                if rep.status_code != 200:
                    logger.error(f"[{BROKER_ID}] Follower replication failed ({rep.status_code})")
                    return jsonify({"error": "Replication failed"}), 500
                logger.info(f"[{BROKER_ID}] Follower acknowledged replication.")
                METRICS["replications"] += 1
                METRICS["last_replication"] = time.strftime("%H:%M:%S")
                add_activity_log("replicate", f"Data replicated to follower at {FOLLOWER_URL}")
            except requests.exceptions.RequestException as e:
                logger.error(f"[{BROKER_ID}] Failed to reach follower: {e}")
                return jsonify({"error": "Follower unreachable"}), 500

        # Step 3: Commit (update HWM)
        # HWM represents the offset of the last *committed* message
        new_hwm = REDIS_CLIENT.incr("high_water_mark")
        logger.info(f"[{BROKER_ID}] Commit success. New HWM: {new_hwm}")

        return jsonify({"status": "success", "offset": new_hwm, "leader_id": BROKER_ID}), 200

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


# --- THIS IS THE MISSING FUNCTION THAT CAUSED THE 404 ERROR ---
@app.route("/consume", methods=["GET"])
def handle_consume():
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
        METRICS["messages_consumed"] += len(messages)
        if messages:
            add_activity_log("consume", f"Served {len(messages)} message(s) to consumer from offset {offset}")
        return jsonify({"messages": messages})

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


@app.route("/metrics", methods=["GET"])
def get_metrics():
    """Return broker metrics for the dashboard."""
    try:
        hwm_str = REDIS_CLIENT.get("high_water_mark")
        high_water_mark = int(hwm_str) if hwm_str else 0
        
        # Check follower health
        follower_health = "Unknown"
        if FOLLOWER_URL:
            try:
                resp = requests.get(f"{FOLLOWER_URL}/health", timeout=2)
                if resp.status_code == 200:
                    follower_health = "✅ Healthy"
                else:
                    follower_health = "⚠️ Degraded"
            except:
                follower_health = "❌ Unreachable"
        
        return jsonify({
            "high_water_mark": high_water_mark,
            "messages_produced": METRICS["messages_produced"],
            "messages_consumed": METRICS["messages_consumed"],
            "replications": METRICS["replications"],
            "elections_won": METRICS["elections_won"],
            "leadership_changes": METRICS["leadership_changes"],
            "follower_url": FOLLOWER_URL,
            "follower_health": follower_health,
            "last_replication": METRICS["last_replication"],
            "lease_time_seconds": LEASE_TIME_SECONDS,
            "recent_activity": METRICS["recent_activity"][:20]  # Last 20 activities
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
    """Try to become leader."""
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
    """Keep checking if leadership changes."""
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


# ---------------- MAIN ----------------
def main():
    attempt_leader_election()
    threading.Thread(target=watch_leader_status, daemon=True).start()

    logger.info(f"[{BROKER_ID}] Broker starting on port {PORT}")
    logger.info(f"[{BROKER_ID}] Current leader: {REDIS_CLIENT.get('leader_lease') or 'None'}")

    app.run(host="0.0.0.0", port=PORT, debug=False)


if __name__ == "__main__":
    main()
