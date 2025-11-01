import redis 
import time
import threading
import logging
import socket
from flask import Flask, request, jsonify

# Initialize Flask app first to avoid NameError
app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
BROKER_ID = f"broker-{socket.gethostname()}"
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
LEASE_TIME_SECONDS = 10
RENEW_INTERVAL_SECONDS = 5
PORT = 5001

# Global state
IS_LEADER = False
LEASE_RENEWAL_THREAD = None

# Initialize Redis client
try:
    REDIS_CLIENT = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
    REDIS_CLIENT.ping()  # Test connection
    logger.info(f"[{BROKER_ID}] Successfully connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
except redis.ConnectionError as e:
    logger.error(f"[{BROKER_ID}] Failed to connect to Redis: {e}")
    raise

@app.route("/produce", methods=["POST"])
def handle_produce():
    """Handle produce requests. Only the leader can accept produce requests."""
    if not IS_LEADER:
        leader_id = REDIS_CLIENT.get("leader_lease")
        if not leader_id:
            return jsonify({"error": "No leader elected yet. Please try again."}), 503
        return jsonify({
            "error": "Not the leader",
            "leader_id": leader_id
        }), 307  # Temporary redirect

    data = request.get_json()
    if not data:
        return jsonify({"error": "No data provided"}), 400
        
    logger.info(f"[{BROKER_ID}] Received produce request: {data}")

    try:
        # In a real implementation, this would append to the partition log
        with open(f"{BROKER_ID}_log.txt", "a") as f:
            f.write(f"{data}\n")
            
        # Update high water mark
        new_hwm = REDIS_CLIENT.incr("high_water_mark")
        logger.info(f"[{BROKER_ID}] Successfully wrote message. New HWM: {new_hwm}")
        
        # In a real implementation, replicate to followers here
        
        return jsonify({
            "status": "success",
            "offset": new_hwm,
            "leader_id": BROKER_ID
        }), 200
        
    except Exception as e:
        logger.error(f"[{BROKER_ID}] Failed to process produce request: {str(e)}")
        return jsonify({"error": "Failed to process request"}), 500

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
                # Use SET with EX and XX to only update if we're still the leader
                success = REDIS_CLIENT.set(
                    "leader_lease", 
                    BROKER_ID, 
                    ex=LEASE_TIME_SECONDS,
                    xx=True
                )
                if not success:
                    logger.warning(f"[{BROKER_ID}] Lost leadership")
                    IS_LEADER = False
                    break
            except Exception as e:
                logger.error(f"[{BROKER_ID}] Failed to renew lease: {str(e)}")
                IS_LEADER = False
                break
    
    LEASE_RENEWAL_THREAD = threading.Thread(target=_renew_lease, daemon=True)
    LEASE_RENEWAL_THREAD.start()

def attempt_leader_election():
    """Attempt to become the leader by acquiring the leader lease in Redis."""
    global IS_LEADER
    
    try:
        logger.info(f"[{BROKER_ID}] Attempting leader election...")
        
        # Try to acquire the leader lease
        success = REDIS_CLIENT.set(
            name="leader_lease",
            value=BROKER_ID,
            ex=LEASE_TIME_SECONDS,
            nx=True  # Only set if not exists
        )
        
        if success:
            IS_LEADER = True
            logger.info(f"[{BROKER_ID}] Successfully became the leader")
            start_lease_renewal()
        else:
            IS_LEADER = False
            leader_id = REDIS_CLIENT.get("leader_lease")
            logger.info(f"[{BROKER_ID}] Current leader is {leader_id}")
            
    except Exception as e:
        logger.error(f"[{BROKER_ID}] Error during leader election: {str(e)}")
        IS_LEADER = False

def health_check():
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

@app.route("/health", methods=["GET"])
def health():
    return health_check()

def main():
    # Attempt to become leader on startup
    attempt_leader_election()
    
    # If not leader, start a thread to periodically check for leader status
    if not IS_LEADER:
        def leader_check_thread():
            while True:
                time.sleep(LEASE_TIME_SECONDS // 2)
                current_leader = REDIS_CLIENT.get("leader_lease")
                if not current_leader or current_leader == BROKER_ID:
                    attempt_leader_election()
        
        threading.Thread(target=leader_check_thread, daemon=True).start()
    
    logger.info(f"[{BROKER_ID}] Starting broker on port {PORT}")
    logger.info(f"[{BROKER_ID}] Current leader: {REDIS_CLIENT.get('leader_lease') or 'None'}")
    
    try:
        app.run(host='0.0.0.0', port=PORT, debug=False)
    except Exception as e:
        logger.error(f"[{BROKER_ID}] Failed to start server: {e}")
        raise

if __name__ == '__main__':
    main()