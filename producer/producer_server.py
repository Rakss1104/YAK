import os
from flask import Flask, render_template, request, jsonify
import requests
import uuid
import json
import pandas as pd
import logging

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
BROKER_URLS = [
    "http://localhost:5001", 
    "http://192.168.191.242:5002"
]

# Global state
current_leader_url = None
session = requests.Session()
session.headers.update({"Content-Type": "application/json"})

def discover_leader():
    """Find the current leader broker."""
    global current_leader_url
    
    # Step 1: Get leader ID
    leader_id = None
    for broker_url in BROKER_URLS:
        try:
            response = session.get(f"{broker_url}/metadata/leader", timeout=3)
            if response.status_code == 200:
                leader_id = response.json().get("leader_id")
                logger.info(f"Broker {broker_url} reports leader is: {leader_id}")
                break
        except requests.RequestException as e:
            logger.warning(f"Could not contact broker {broker_url}: {e}")
    
    if not leader_id:
        return None
    
    # Step 2: Find URL for that leader ID
    for broker_url in BROKER_URLS:
        try:
            response = session.get(f"{broker_url}/health", timeout=3)
            if response.status_code == 200:
                broker_id = response.json().get("broker_id")
                if broker_id == leader_id:
                    current_leader_url = broker_url
                    logger.info(f"Leader found at: {broker_url}")
                    return broker_url
        except requests.RequestException as e:
            logger.warning(f"Could not contact broker {broker_url}: {e}")
    
    return None

@app.route('/')
def index():
    """Serve the producer UI."""
    return render_template('producer.html')

@app.route('/api/leader', methods=['GET'])
def get_leader():
    """Get current leader information."""
    leader = discover_leader()
    if leader:
        return jsonify({"leader_url": leader, "status": "connected"})
    return jsonify({"error": "No leader found", "status": "disconnected"}), 503

@app.route('/api/send', methods=['POST'])
def send_message():
    """Send a single message to the broker."""
    global current_leader_url
    
    data = request.get_json()
    topic = data.get('topic', 'default')
    key = data.get('key')
    payload = data.get('payload', {})
    
    if not current_leader_url:
        discover_leader()
    
    if not current_leader_url:
        return jsonify({"error": "No leader available"}), 503
    
    message = {
        "msg_id": str(uuid.uuid4()),
        "topic": topic,
        "key": key,
        "payload": payload
    }
    
    try:
        response = session.post(
            f"{current_leader_url}/produce",
            json=message,
            timeout=5
        )
        
        if response.status_code == 200:
            return jsonify(response.json())
        elif response.status_code == 400:
            # Leader changed, rediscover
            current_leader_url = None
            discover_leader()
            return jsonify({"error": "Leader changed, please retry"}), 503
        else:
            return jsonify({"error": f"Broker error: {response.text}"}), response.status_code
            
    except requests.RequestException as e:
        logger.error(f"Failed to send message: {e}")
        current_leader_url = None
        return jsonify({"error": str(e)}), 500

@app.route('/api/upload', methods=['POST'])
def upload_file():
    """Upload and process a CSV file."""
    if 'file' not in request.files:
        return jsonify({"error": "No file provided"}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No file selected"}), 400
    
    if not file.filename.endswith('.csv'):
        return jsonify({"error": "Only CSV files are supported"}), 400
    
    try:
        # Read CSV
        df = pd.read_csv(file)
        records = df.to_dict('records')
        
        return jsonify({
            "status": "success",
            "records": len(records),
            "preview": records[:5]  # Return first 5 records as preview
        })
    except Exception as e:
        logger.error(f"Failed to process file: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/send_batch', methods=['POST'])
def send_batch():
    """Send a batch of messages from uploaded file."""
    global current_leader_url
    
    data = request.get_json()
    records = data.get('records', [])
    topic = data.get('topic', 'default')
    key_field = data.get('key_field', 'server_id')
    
    if not records:
        return jsonify({"error": "No records provided"}), 400
    
    if not current_leader_url:
        discover_leader()
    
    if not current_leader_url:
        return jsonify({"error": "No leader available"}), 503
    
    sent = 0
    failed = 0
    
    for record in records:
        message = {
            "msg_id": str(uuid.uuid4()),
            "topic": topic,
            "key": record.get(key_field),
            "payload": record
        }
        
        try:
            response = session.post(
                f"{current_leader_url}/produce",
                json=message,
                timeout=5
            )
            
            if response.status_code == 200:
                sent += 1
            else:
                failed += 1
                
        except requests.RequestException as e:
            logger.error(f"Failed to send message: {e}")
            failed += 1
    
    return jsonify({
        "status": "completed",
        "sent": sent,
        "failed": failed,
        "total": len(records)
    })

if __name__ == '__main__':
    logger.info("Starting Producer Web UI on port 5003...")
    app.run(host='0.0.0.0', port=5003, debug=False)
